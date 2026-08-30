//! Discovery and polling of managed Redis pods, and the `Collector` that turns
//! the most recent poll into Prometheus series.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;
use prometheus_client::metrics::MetricType;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::controller::{Context, FIELD_MANAGER};
use crate::error::Result;
use crate::metrics::info::{
    Info, InfoMetric, Kind, REDIS_METRICS, SENTINEL_METRICS, STATUS_METRICS, SentinelMaster,
};
use crate::metrics::{SnapshotCell, resp, shutdown_signal};

const REDIS_PORT: u16 = 6379;
const SENTINEL_PORT: u16 = 26379;

/// Every workload the operator creates carries this label, on all three pod
/// templates, so one cluster-wide selector finds every scrape target.
const MANAGED_SELECTOR: &str = "app.kubernetes.io/managed-by=redis-operator";

/// Identity of one scraped instance.
///
/// `instance_name`, not `instance`: Prometheus attaches its own `instance`
/// target label, and a metric label of the same name is either renamed to
/// `exported_instance` (with `honor_labels: false`) or silently clobbers target
/// identity (with `honor_labels: true`). Neither is what anyone wants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstanceLabels {
    pub namespace: String,
    pub pod: String,
    pub instance_name: String,
    /// `redis`, `rediscluster`, or `sentinel`.
    pub kind: &'static str,
}

impl InstanceLabels {
    fn encode(&self) -> Vec<(&'static str, &str)> {
        vec![
            ("namespace", self.namespace.as_str()),
            ("pod", self.pod.as_str()),
            ("instance_name", self.instance_name.as_str()),
            ("kind", self.kind),
        ]
    }
}

/// A pod worth dialling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub addr: SocketAddr,
    pub labels: InstanceLabels,
}

/// One instance's last observation.
///
/// A failed scrape is kept, with `up: false` and no `info`: a pod that exists
/// but will not answer is the single most important thing this exporter can
/// say, and dropping it would make an unreachable Redis indistinguishable from
/// one that was never there.
#[derive(Clone, Debug)]
pub struct Instance {
    pub labels: InstanceLabels,
    pub up: bool,
    pub scrape_seconds: f64,
    pub info: Option<Info>,
}

impl Instance {
    /// `master` or `replica`, from the INFO reply itself rather than from the
    /// operator's `redis-operator/role` pod label. INFO is ground truth at
    /// scrape time; the pod label is the operator's belief, refreshed once per
    /// reconcile.
    fn role(&self) -> Option<&str> {
        match self.info.as_ref()?.get("role")? {
            "slave" => Some("replica"),
            other => Some(other),
        }
    }
}

/// The result of one poll tick. Replaced wholesale rather than merged: a pod
/// that has gone away stops being exported because it is simply absent from the
/// next snapshot, with no stale-series bookkeeping to get wrong.
#[derive(Debug, Default)]
pub struct Snapshot {
    pub instances: Vec<Instance>,
}

pub struct PollConfig {
    pub interval: Duration,
    pub scrape_timeout: Duration,
    pub concurrency: usize,
}

/// Poll every managed Redis pod until shutdown.
pub async fn run(ctx: Arc<Context>, cfg: PollConfig) -> anyhow::Result<()> {
    info!(
        interval = ?cfg.interval,
        timeout = ?cfg.scrape_timeout,
        concurrency = cfg.concurrency,
        "starting Redis INFO poller"
    );

    let mut ticker = tokio::time::interval(cfg.interval);
    // If a tick's worth of scraping overruns the interval, run the next one
    // immediately rather than firing the whole backlog at once.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut shutdown = Box::pin(shutdown_signal());

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = &mut shutdown => {
                info!("Redis INFO poller shutting down");
                return Ok(());
            }
        }
        // A failed LIST must not kill the poller — and, through `try_join!`, the
        // controllers with it. Keep serving the previous snapshot and retry.
        if let Err(err) = poll_once(&ctx, &cfg).await {
            warn!(?err, "pod discovery failed; serving the previous snapshot");
        }
    }
}

async fn poll_once(ctx: &Context, cfg: &PollConfig) -> Result<()> {
    let targets = discover(&ctx.client).await?;
    let mut instances: Vec<Instance> = futures::stream::iter(targets)
        .map(|target| scrape(target, cfg.scrape_timeout))
        .buffer_unordered(cfg.concurrency)
        .collect()
        .await;
    // Scrapes complete out of order; sorting keeps the exposition stable, which
    // makes diffing two scrapes by hand actually possible.
    instances.sort_by(|a, b| {
        (&a.labels.namespace, &a.labels.pod).cmp(&(&b.labels.namespace, &b.labels.pod))
    });

    let failures = instances.iter().filter(|i| !i.up).count();
    ctx.metrics.observe_poll(instances.len(), failures);
    ctx.metrics.snapshot().store(Snapshot { instances });
    Ok(())
}

async fn discover(client: &Client) -> Result<Vec<Target>> {
    let api: Api<Pod> = Api::all(client.clone());
    let pods = api
        .list(&ListParams::default().labels(MANAGED_SELECTOR))
        .await?;
    Ok(pods.items.iter().filter_map(target_from_pod).collect())
}

/// Resolve a pod into a scrape target, or `None` if it isn't one.
///
/// Skips anything we cannot dial or would only produce noise for: a pod with no
/// IP yet, one that isn't Running, and one that is terminating — the last of
/// those will start refusing connections shortly, and flapping `redis_up`
/// through every rolling restart is worse than saying nothing. A skipped pod
/// emits no series at all, which is deliberately distinct from `redis_up 0`: a
/// legitimately Pending pod is not a scrape failure.
pub fn target_from_pod(pod: &Pod) -> Option<Target> {
    if pod.metadata.deletion_timestamp.is_some() {
        return None;
    }
    let status = pod.status.as_ref()?;
    if status.phase.as_deref() != Some("Running") {
        return None;
    }
    let ip: IpAddr = status.pod_ip.as_ref()?.parse().ok()?;

    let labels = pod.labels();
    if labels.get("app.kubernetes.io/managed-by").map(String::as_str) != Some(FIELD_MANAGER) {
        return None;
    }
    let (kind, port) = match labels.get("app.kubernetes.io/name")?.as_str() {
        "redis" => ("redis", REDIS_PORT),
        "redis-cluster" => ("rediscluster", REDIS_PORT),
        "redis-sentinel" => ("sentinel", SENTINEL_PORT),
        _ => return None,
    };
    // A pod without an instance label isn't one of ours despite the managed-by
    // label, and there would be no CR to attribute its metrics to.
    let instance_name = labels.get("app.kubernetes.io/instance")?.clone();

    Some(Target {
        addr: SocketAddr::new(ip, port),
        labels: InstanceLabels {
            namespace: pod.namespace()?,
            pod: pod.name_any(),
            instance_name,
            kind,
        },
    })
}

async fn scrape(target: Target, timeout: Duration) -> Instance {
    let started = Instant::now();
    let result = resp::info(target.addr, timeout).await;
    let scrape_seconds = started.elapsed().as_secs_f64();
    match result {
        Ok(text) => Instance {
            labels: target.labels,
            up: true,
            scrape_seconds,
            info: Some(Info::parse(&text)),
        },
        Err(err) => {
            warn!(
                pod = %target.labels.pod,
                ns = %target.labels.namespace,
                addr = %target.addr,
                ?err,
                "INFO scrape failed",
            );
            Instance {
                labels: target.labels,
                up: false,
                scrape_seconds,
                info: None,
            }
        }
    }
}

/// Renders the latest [`Snapshot`] on each Prometheus scrape.
#[derive(Debug)]
pub struct InfoCollector {
    snapshot: Arc<SnapshotCell>,
}

impl InfoCollector {
    pub fn new(snapshot: Arc<SnapshotCell>) -> Self {
        Self { snapshot }
    }
}

impl Collector for InfoCollector {
    fn encode(&self, mut encoder: DescriptorEncoder<'_>) -> std::result::Result<(), std::fmt::Error> {
        let snapshot = self.snapshot.load();
        let instances = &snapshot.instances;
        if instances.is_empty() {
            return Ok(());
        }

        {
            let mut m = encoder.encode_descriptor(
                "redis_up",
                "1 when the most recent INFO scrape of this pod succeeded",
                None,
                MetricType::Gauge,
            )?;
            for inst in instances {
                m.encode_family(&inst.labels.encode())?
                    .encode_gauge(&(inst.up as i64))?;
            }
        }

        {
            let mut m = encoder.encode_descriptor(
                "redis_scrape_duration_seconds",
                "Seconds the operator spent on the most recent INFO scrape of this pod",
                None,
                MetricType::Gauge,
            )?;
            for inst in instances {
                m.encode_family(&inst.labels.encode())?
                    .encode_gauge(&inst.scrape_seconds)?;
            }
        }

        // Identity as an info metric rather than as labels on every series:
        // `role` flips on failover, and moving a counter to a new series
        // mid-flight makes `rate()` read the change as a reset. Join with
        // `* on (namespace, pod) group_left(role) redis_instance_info`.
        if instances.iter().any(|i| i.info.is_some()) {
            let mut m = encoder.encode_descriptor(
                "redis_instance",
                "Version, mode, and replication role of this instance",
                None,
                MetricType::Info,
            )?;
            for inst in instances {
                let Some(info) = inst.info.as_ref() else {
                    continue;
                };
                let detail = vec![
                    ("role", inst.role().unwrap_or_default()),
                    ("redis_version", info.get("redis_version").unwrap_or_default()),
                    ("redis_mode", info.get("redis_mode").unwrap_or_default()),
                ];
                m.encode_family(&inst.labels.encode())?.encode_info(&detail)?;
            }
        }

        for metric in REDIS_METRICS.iter().chain(SENTINEL_METRICS) {
            encode_field(&mut encoder, instances, metric)?;
        }

        for metric in STATUS_METRICS {
            if !instances
                .iter()
                .any(|i| i.info.as_ref().is_some_and(|info| info.get(metric.key).is_some()))
            {
                continue;
            }
            let mut m =
                encoder.encode_descriptor(metric.name, metric.help, None, MetricType::Gauge)?;
            for inst in instances {
                let Some(ok) = inst
                    .info
                    .as_ref()
                    .and_then(|info| info.is(metric.key, metric.ok_value))
                else {
                    continue;
                };
                m.encode_family(&inst.labels.encode())?
                    .encode_gauge(&(ok as i64))?;
            }
        }

        encode_keyspace(&mut encoder, instances)?;
        encode_sentinel_masters(&mut encoder, instances)?;
        Ok(())
    }
}

/// Emit one catalogue entry across every instance that reports its field.
///
/// Instances that don't report it are skipped rather than zero-filled — a
/// sentinel has no `# Memory` section, and inventing a zero there would be a
/// lie that alerts on memory would act on.
fn encode_field(
    encoder: &mut DescriptorEncoder<'_>,
    instances: &[Instance],
    metric: &InfoMetric,
) -> std::result::Result<(), std::fmt::Error> {
    let values: Vec<(&Instance, f64)> = instances
        .iter()
        .filter_map(|inst| Some((inst, inst.info.as_ref()?.num(metric.key)?)))
        .collect();
    if values.is_empty() {
        return Ok(());
    }
    let kind = match metric.kind {
        Kind::Gauge => MetricType::Gauge,
        Kind::Counter => MetricType::Counter,
    };
    let mut m = encoder.encode_descriptor(metric.name, metric.help, None, kind)?;
    for (inst, value) in values {
        // Bound to a local: `encode_family` borrows the label set for as long
        // as the encoder it returns lives.
        let labels = inst.labels.encode();
        let mut family = m.encode_family(&labels)?;
        match metric.kind {
            Kind::Gauge => family.encode_gauge(&value)?,
            Kind::Counter => family.encode_counter::<Vec<(&str, &str)>, f64, f64>(&value, None)?,
        }
    }
    Ok(())
}

/// Bounded by Redis's `databases` setting, 16 by default.
const MAX_KEYSPACE_DBS: usize = 16;

fn encode_keyspace(
    encoder: &mut DescriptorEncoder<'_>,
    instances: &[Instance],
) -> std::result::Result<(), std::fmt::Error> {
    let mut rows = Vec::new();
    for inst in instances {
        let Some(info) = inst.info.as_ref() else {
            continue;
        };
        let dbs = info.keyspace();
        if dbs.len() > MAX_KEYSPACE_DBS {
            warn!(
                pod = %inst.labels.pod,
                count = dbs.len(),
                "instance reports more databases than expected; truncating keyspace metrics",
            );
        }
        for db in dbs.into_iter().take(MAX_KEYSPACE_DBS) {
            rows.push((inst, db));
        }
    }
    if rows.is_empty() {
        return Ok(());
    }

    {
        let mut m = encoder.encode_descriptor(
            "redis_db_keys",
            "Keys held in this database",
            None,
            MetricType::Gauge,
        )?;
        for (inst, db) in &rows {
            let mut labels = inst.labels.encode();
            labels.push(("db", db.db.as_str()));
            m.encode_family(&labels)?.encode_gauge(&(db.keys as i64))?;
        }
    }
    let mut m = encoder.encode_descriptor(
        "redis_db_keys_expiring",
        "Keys in this database that carry a TTL",
        None,
        MetricType::Gauge,
    )?;
    for (inst, db) in &rows {
        let mut labels = inst.labels.encode();
        labels.push(("db", db.db.as_str()));
        m.encode_family(&labels)?
            .encode_gauge(&(db.expires as i64))?;
    }
    Ok(())
}

fn encode_sentinel_masters(
    encoder: &mut DescriptorEncoder<'_>,
    instances: &[Instance],
) -> std::result::Result<(), std::fmt::Error> {
    let rows: Vec<_> = instances
        .iter()
        .filter_map(|inst| Some((inst, inst.info.as_ref()?.sentinel_masters())))
        .flat_map(|(inst, masters)| masters.into_iter().map(move |m| (inst, m)))
        .collect();
    if rows.is_empty() {
        return Ok(());
    }

    /// One per-master gauge: exported name, help, and how to read its value.
    type MasterGauge = (&'static str, &'static str, fn(&SentinelMaster) -> i64);

    let specs: [MasterGauge; 3] = [
        (
            "redis_sentinel_master_ok",
            "1 when this sentinel considers the monitored master healthy",
            |m| m.ok as i64,
        ),
        (
            "redis_sentinel_master_replicas",
            "Replicas this sentinel sees attached to the monitored master",
            |m| m.slaves as i64,
        ),
        (
            "redis_sentinel_master_sentinels",
            "Other sentinels this sentinel sees monitoring the same master",
            |m| m.sentinels as i64,
        ),
    ];
    for (name, help, value) in specs {
        let mut m = encoder.encode_descriptor(name, help, None, MetricType::Gauge)?;
        for (inst, master) in &rows {
            let mut labels = inst.labels.encode();
            labels.push(("master", master.name.as_str()));
            m.encode_family(&labels)?.encode_gauge(&value(master))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use k8s_openapi::api::core::v1::PodStatus;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use kube::api::ObjectMeta;
    use prometheus_client::registry::Registry;

    const REPLICA_INFO: &str = include_str!("testdata/info_replica.txt");
    const SENTINEL_INFO: &str = include_str!("testdata/info_sentinel.txt");

    fn pod(name: &str, app: &str, ip: Option<&str>, phase: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("default".to_string()),
                labels: Some(BTreeMap::from([
                    ("app.kubernetes.io/name".to_string(), app.to_string()),
                    ("app.kubernetes.io/instance".to_string(), "cache".to_string()),
                    (
                        "app.kubernetes.io/managed-by".to_string(),
                        FIELD_MANAGER.to_string(),
                    ),
                ])),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some(phase.to_string()),
                pod_ip: ip.map(String::from),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn instance(pod_name: &str, kind: &'static str, info: Option<&str>) -> Instance {
        Instance {
            labels: InstanceLabels {
                namespace: "default".to_string(),
                pod: pod_name.to_string(),
                instance_name: "cache".to_string(),
                kind,
            },
            up: info.is_some(),
            scrape_seconds: 0.25,
            info: info.map(Info::parse),
        }
    }

    /// Encode a snapshot the way `/metrics` would, so the assertions below are
    /// against the bytes Prometheus actually receives.
    fn render(instances: Vec<Instance>) -> String {
        let cell = Arc::new(SnapshotCell::default());
        cell.store(Snapshot { instances });
        let mut registry = Registry::default();
        registry.register_collector(Box::new(InfoCollector::new(cell)));
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &registry).expect("encode");
        out
    }

    #[test]
    fn target_resolves_kind_and_port_from_the_app_name_label() {
        let t = target_from_pod(&pod("cache-0", "redis", Some("10.1.2.3"), "Running")).unwrap();
        assert_eq!(t.addr, "10.1.2.3:6379".parse::<SocketAddr>().unwrap());
        assert_eq!(t.labels.kind, "redis");
        assert_eq!(t.labels.instance_name, "cache");
        assert_eq!(t.labels.namespace, "default");

        let t = target_from_pod(&pod("c-0", "redis-cluster", Some("10.1.2.4"), "Running")).unwrap();
        assert_eq!(t.addr.port(), 6379);
        assert_eq!(t.labels.kind, "rediscluster");

        let t = target_from_pod(&pod("s-0", "redis-sentinel", Some("10.1.2.5"), "Running")).unwrap();
        assert_eq!(t.addr.port(), 26379);
        assert_eq!(t.labels.kind, "sentinel");
    }

    #[test]
    fn target_skips_pods_without_an_ip() {
        assert!(target_from_pod(&pod("cache-0", "redis", None, "Running")).is_none());
    }

    #[test]
    fn target_skips_pods_that_are_not_running() {
        assert!(target_from_pod(&pod("cache-0", "redis", Some("10.1.2.3"), "Pending")).is_none());
    }

    #[test]
    fn target_skips_terminating_pods() {
        let mut p = pod("cache-0", "redis", Some("10.1.2.3"), "Running");
        p.metadata.deletion_timestamp = Some(Time(k8s_openapi::jiff::Timestamp::UNIX_EPOCH));
        assert!(target_from_pod(&p).is_none());
    }

    #[test]
    fn target_skips_unmanaged_and_unlabelled_pods() {
        let mut p = pod("other-0", "postgres", Some("10.1.2.3"), "Running");
        assert!(target_from_pod(&p).is_none());

        p = pod("cache-0", "redis", Some("10.1.2.3"), "Running");
        p.metadata.labels.as_mut().unwrap().remove("app.kubernetes.io/instance");
        assert!(target_from_pod(&p).is_none());

        p = pod("cache-0", "redis", Some("10.1.2.3"), "Running");
        p.metadata.labels.as_mut().unwrap().remove("app.kubernetes.io/managed-by");
        assert!(target_from_pod(&p).is_none());
    }

    #[test]
    fn encodes_allowlisted_fields_from_a_real_reply() {
        let out = render(vec![instance("cache-0", "redis", Some(REPLICA_INFO))]);
        let labels = r#"{namespace="default",pod="cache-0",instance_name="cache",kind="redis"}"#;
        assert!(out.contains(&format!("redis_up{labels} 1")), "{out}");
        assert!(
            out.contains(&format!("redis_memory_used_bytes{labels} 274387912.0")),
            "{out}"
        );
        assert!(
            out.contains(&format!("redis_connected_clients{labels} 8.0")),
            "{out}"
        );
    }

    #[test]
    fn counters_are_typed_as_counters_and_get_the_total_suffix() {
        let out = render(vec![instance("cache-0", "redis", Some(REPLICA_INFO))]);
        assert!(
            out.contains("# TYPE redis_commands_processed counter"),
            "{out}"
        );
        assert!(out.contains("redis_commands_processed_total{"), "{out}");
        // And gauges must not acquire one.
        assert!(!out.contains("redis_connected_clients_total"), "{out}");
    }

    #[test]
    fn skips_fields_outside_the_allowlist() {
        let out = render(vec![instance("cache-0", "redis", Some(REPLICA_INFO))]);
        for banned in [
            "errorstat",
            "db0_distrib",
            "io_thread",
            "used_memory_human",
            "process_id",
            "lru_clock",
        ] {
            assert!(!out.contains(banned), "{banned} leaked into the exposition");
        }
    }

    #[test]
    fn maps_status_words_to_one_and_zero() {
        let out = render(vec![instance("cache-0", "redis", Some(REPLICA_INFO))]);
        let labels = r#"{namespace="default",pod="cache-0",instance_name="cache",kind="redis"}"#;
        assert!(out.contains(&format!("redis_master_link_up{labels} 1")), "{out}");
        assert!(
            out.contains(&format!("redis_rdb_last_bgsave_success{labels} 1")),
            "{out}"
        );

        let broken = REPLICA_INFO.replace("rdb_last_bgsave_status:ok", "rdb_last_bgsave_status:err");
        let out = render(vec![instance("cache-0", "redis", Some(&broken))]);
        assert!(
            out.contains(&format!("redis_rdb_last_bgsave_success{labels} 0")),
            "{out}"
        );
    }

    #[test]
    fn normalizes_the_slave_role_to_replica() {
        let out = render(vec![instance("cache-0", "redis", Some(REPLICA_INFO))]);
        assert!(out.contains(r#"role="replica""#), "{out}");
        assert!(!out.contains(r#"role="slave""#), "{out}");
        assert!(out.contains(r#"redis_version="8.10.0""#), "{out}");
    }

    #[test]
    fn exports_keyspace_per_database() {
        let out = render(vec![instance("cache-0", "redis", Some(REPLICA_INFO))]);
        assert!(out.contains(r#"db="0"} 110868"#), "{out}");
        assert!(out.contains("redis_db_keys_expiring{"), "{out}");
    }

    #[test]
    fn exports_sentinel_sections() {
        let out = render(vec![instance("cache-sentinel-0", "sentinel", Some(SENTINEL_INFO))]);
        assert!(out.contains("redis_sentinel_masters{"), "{out}");
        assert!(out.contains("redis_sentinel_tilt{"), "{out}");
        assert!(out.contains(r#"master="mymaster""#), "{out}");
        assert!(out.contains("redis_sentinel_master_sentinels{"), "{out}");
    }

    #[test]
    fn omits_fields_the_instance_did_not_report() {
        // A sentinel reports no Memory section, so it must produce no memory
        // series rather than a zero.
        let out = render(vec![instance("cache-sentinel-0", "sentinel", Some(SENTINEL_INFO))]);
        assert!(!out.contains("redis_memory_used_bytes"), "{out}");
        assert!(!out.contains("redis_master_link_up"), "{out}");
        // But the sections it does share with a server are still exported.
        assert!(out.contains("redis_connected_clients{"), "{out}");
        assert!(out.contains("redis_commands_processed_total{"), "{out}");
    }

    #[test]
    fn an_unreachable_instance_reports_only_up_zero() {
        let out = render(vec![instance("cache-0", "redis", None)]);
        let labels = r#"{namespace="default",pod="cache-0",instance_name="cache",kind="redis"}"#;
        assert!(out.contains(&format!("redis_up{labels} 0")), "{out}");
        assert!(out.contains("redis_scrape_duration_seconds{"), "{out}");
        assert!(!out.contains("redis_memory_used_bytes"), "{out}");
        assert!(!out.contains("redis_instance_info"), "{out}");
    }

    #[test]
    fn a_pod_absent_from_the_new_snapshot_is_no_longer_exported() {
        let both = render(vec![
            instance("cache-0", "redis", Some(REPLICA_INFO)),
            instance("cache-1", "redis", Some(REPLICA_INFO)),
        ]);
        assert!(both.contains("cache-1"), "{both}");

        let one = render(vec![instance("cache-0", "redis", Some(REPLICA_INFO))]);
        assert!(one.contains("cache-0"), "{one}");
        assert!(!one.contains("cache-1"), "{one}");
    }

    #[test]
    fn an_empty_snapshot_encodes_cleanly() {
        let out = render(vec![]);
        assert!(!out.contains("redis_up"), "{out}");
    }
}
