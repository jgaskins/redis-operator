//! Prometheus metrics: an HTTP server, the operator's own instrumentation, and
//! a background poller that scrapes `INFO` from every managed Redis pod.
//!
//! The poller and the server are deliberately decoupled by a snapshot. The
//! poller writes a fresh [`Snapshot`] every tick; `/metrics` renders whatever is
//! in the cell at the moment it is scraped. A Redis pod that hangs can delay the
//! freshness of the data by one tick, but it can never delay — or fail — a
//! Prometheus scrape.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

pub mod collector;
pub mod info;
pub mod resp;
pub mod server;

use collector::{InfoCollector, Snapshot};

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ControllerLabels {
    controller: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ReconcileLabels {
    controller: String,
    result: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BuildLabels {
    version: String,
}

/// The most recent poll result, shared between the poller that writes it and the
/// collector that renders it.
///
/// A `std::sync::RwLock`, not tokio's: `Collector::encode` is synchronous, and
/// the lock is only ever held long enough to clone or replace an `Arc`, never
/// across an `.await`.
#[derive(Debug, Default)]
pub struct SnapshotCell(RwLock<Arc<Snapshot>>);

impl SnapshotCell {
    pub fn load(&self) -> Arc<Snapshot> {
        self.0.read().expect("snapshot lock poisoned").clone()
    }

    pub fn store(&self, snapshot: Snapshot) {
        *self.0.write().expect("snapshot lock poisoned") = Arc::new(snapshot);
    }
}

/// Everything the metrics endpoint serves: one registry holding the operator's
/// own instrumentation plus the collector that renders per-pod Redis metrics.
pub struct Metrics {
    registry: Registry,
    reconciles: Family<ReconcileLabels, Counter>,
    reconcile_errors: Family<ControllerLabels, Counter>,
    reconcile_duration: Family<ControllerLabels, Histogram, fn() -> Histogram>,
    scrape_targets: Gauge,
    scrape_failures: Counter,
    snapshot: Arc<SnapshotCell>,
    /// Flipped by the poller once it has listed pods successfully at least once,
    /// which is the point at which we know the kube client works. Drives
    /// `/readyz`.
    ready: AtomicBool,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let snapshot = Arc::new(SnapshotCell::default());
        // Registered on the root registry with no prefix: the INFO metric names
        // in `info::REDIS_METRICS` are already fully qualified.
        registry.register_collector(Box::new(InfoCollector::new(snapshot.clone())));

        let reconciles = Family::<ReconcileLabels, Counter>::default();
        let reconcile_errors = Family::<ControllerLabels, Counter>::default();
        // Written out in full because inference cannot recover the constructor
        // type parameter from `new_with_constructor`.
        let buckets: fn() -> Histogram = || Histogram::new(exponential_buckets(0.01, 2.0, 14));
        let reconcile_duration =
            Family::<ControllerLabels, Histogram, fn() -> Histogram>::new_with_constructor(buckets);
        let scrape_targets = Gauge::default();
        let scrape_failures = Counter::default();
        let build_info = Family::<BuildLabels, Gauge>::default();
        build_info
            .get_or_create(&BuildLabels {
                version: env!("CARGO_PKG_VERSION").to_string(),
            })
            .set(1);

        let op = registry.sub_registry_with_prefix("redis_operator");
        // Counters are registered without the `_total` suffix: the OpenMetrics
        // encoder appends it from the metric type, and spelling it here would
        // produce `..._total_total`.
        op.register(
            "reconcile",
            "Reconciles run, by controller and outcome",
            reconciles.clone(),
        );
        op.register(
            "reconcile_errors",
            "Reconciles that returned an error, by controller",
            reconcile_errors.clone(),
        );
        op.register(
            "reconcile_duration_seconds",
            "Time spent in a single reconcile",
            reconcile_duration.clone(),
        );
        op.register(
            "scrape_targets",
            "Managed Redis pods the last poll found scrapable",
            scrape_targets.clone(),
        );
        op.register(
            "scrape_failures",
            "Pod scrapes that did not return a usable INFO reply",
            scrape_failures.clone(),
        );
        op.register("build_info", "Operator build information", build_info);

        Self {
            registry,
            reconciles,
            reconcile_errors,
            reconcile_duration,
            scrape_targets,
            scrape_failures,
            snapshot,
            ready: AtomicBool::new(false),
        }
    }

    /// The cell the poller writes its results into.
    pub fn snapshot(&self) -> &Arc<SnapshotCell> {
        &self.snapshot
    }

    /// Record the outcome of one poll tick.
    pub fn observe_poll(&self, targets: usize, failures: usize) {
        self.scrape_targets.set(targets as i64);
        self.scrape_failures.inc_by(failures as u64);
        self.ready.store(true, Ordering::Relaxed);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    /// Start timing a reconcile. The caller must call
    /// [`ReconcileTimer::finish`], which is why both controllers wrap their
    /// reconcile rather than instrumenting its body — the bodies have several
    /// early returns.
    pub fn reconcile_started(&self, controller: &str) -> ReconcileTimer<'_> {
        ReconcileTimer {
            metrics: self,
            controller: controller.to_string(),
            started: Instant::now(),
        }
    }

    /// Render the whole registry as an OpenMetrics document.
    pub fn render(&self) -> std::result::Result<String, std::fmt::Error> {
        let mut out = String::with_capacity(64 * 1024);
        prometheus_client::encoding::text::encode(&mut out, &self.registry)?;
        Ok(out)
    }
}

/// Records a reconcile's duration and outcome when [`finish`] is called.
///
/// [`finish`]: ReconcileTimer::finish
pub struct ReconcileTimer<'a> {
    metrics: &'a Metrics,
    controller: String,
    started: Instant,
}

impl ReconcileTimer<'_> {
    pub fn finish(self, ok: bool) {
        let labels = ControllerLabels {
            controller: self.controller.clone(),
        };
        self.metrics
            .reconcile_duration
            .get_or_create(&labels)
            .observe(self.started.elapsed().as_secs_f64());
        if !ok {
            self.metrics.reconcile_errors.get_or_create(&labels).inc();
        }
        self.metrics
            .reconciles
            .get_or_create(&ReconcileLabels {
                controller: self.controller,
                result: if ok { "success" } else { "error" }.to_string(),
            })
            .inc();
    }
}

/// Resolves on SIGINT or SIGTERM.
///
/// A second listener alongside the controllers' own `.shutdown_on_signal()` is
/// fine — tokio fans a signal out to every registered listener. Without it,
/// SIGTERM would stop both controllers and the process would then sit in
/// `try_join!` forever waiting on a server that never returns, until the
/// kubelet's grace period expired and SIGKILLed it.
pub async fn shutdown_signal() {
    let interrupt = async {
        tokio::signal::ctrl_c().await.ok();
    };
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            // Nothing sensible to do if the handler can't be installed; let the
            // other branch carry the shutdown.
            Err(_) => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        _ = interrupt => {}
        _ = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_observations_reach_the_registry() {
        let metrics = Metrics::new();
        metrics.reconcile_started("Redis").finish(true);
        metrics.reconcile_started("Redis").finish(false);

        let out = metrics.render().expect("render");
        assert!(
            out.contains("redis_operator_reconcile_total{controller=\"Redis\",result=\"success\"} 1"),
            "{out}"
        );
        assert!(
            out.contains("redis_operator_reconcile_total{controller=\"Redis\",result=\"error\"} 1"),
            "{out}"
        );
        assert!(
            out.contains("redis_operator_reconcile_errors_total{controller=\"Redis\"} 1"),
            "{out}"
        );
        assert!(out.contains("redis_operator_reconcile_duration_seconds_count"), "{out}");
    }

    #[test]
    fn build_info_reports_the_crate_version() {
        let out = Metrics::new().render().expect("render");
        let expected = format!(
            "redis_operator_build_info{{version=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION")
        );
        assert!(out.contains(&expected), "{out}");
    }

    #[test]
    fn rendered_document_has_exactly_one_eof_marker() {
        let out = Metrics::new().render().expect("render");
        assert_eq!(out.matches("# EOF").count(), 1, "{out}");
    }

    #[test]
    fn readiness_flips_only_after_a_poll() {
        let metrics = Metrics::new();
        assert!(!metrics.is_ready());
        metrics.observe_poll(0, 0);
        assert!(metrics.is_ready());
    }
}
