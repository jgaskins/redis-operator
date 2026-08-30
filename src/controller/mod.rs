use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::{ObjectReference, ResourceRequirements, TopologySpreadConstraint};
use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec as K8sPdbSpec};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{DeleteParams, ObjectMeta, Patch, PatchParams};
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::{Api, Client, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::{info, warn};

use crate::crd::redis::{PersistenceSpec, PodDisruptionBudgetSpec, ResourcesSpec};
use crate::error::Result;
use crate::metrics::Metrics;

pub mod redis;
pub mod redis_cluster;

pub const FIELD_MANAGER: &str = "redis-operator";

#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub recorder: Recorder,
    /// Shared with the metrics server, which renders the same registry the
    /// reconcile wrappers write to.
    pub metrics: Arc<Metrics>,
}

impl Context {
    pub fn new(client: Client, metrics: Arc<Metrics>) -> Self {
        let reporter = Reporter {
            controller: FIELD_MANAGER.to_string(),
            // Set from the downward API in deploy/operator.yaml. Naming the pod
            // makes `kubectl describe` unambiguous if the operator is ever run
            // with more than one replica.
            instance: std::env::var("CONTROLLER_POD_NAME").ok(),
        };
        Self {
            // Built once and shared: `Recorder` is `Clone` and its dedup cache
            // is behind an `Arc`, which is what collapses a warning re-emitted
            // on every reconcile into a single Event with a rising
            // `series.count`. Constructing one per reconcile would defeat that
            // and flood the events API.
            recorder: Recorder::new(client.clone(), reporter),
            client,
            metrics,
        }
    }
}

/// Publish an Event, downgrading failures to a log line. Event delivery is
/// best-effort telemetry — a missing RBAC grant on `events.k8s.io` must not
/// take the reconcile down with it.
pub async fn emit(ctx: &Context, obj_ref: &ObjectReference, ev: Event) {
    if let Err(err) = ctx.recorder.publish(&ev, obj_ref).await {
        warn!(?err, reason = %ev.reason, "failed to publish event");
    }
}

/// Defaults applied to every Redis server pod when the user doesn't override
/// them. Sized to give Redis enough memory to be useful (1Gi) and enough CPU
/// to handle bursty traffic (1.2 request, 2 limit) — well above BestEffort
/// QoS so the kubelet won't evict the pod under node pressure.
///
/// Memory request equals limit so the pod runs at Guaranteed QoS; this matches
/// how `maxmemory` is configured (70% of the limit) and avoids surprises where
/// the kubelet OOM-kills Redis before it hits its own `maxmemory` ceiling.
fn default_redis_resources() -> ResourceRequirements {
    let q = |s: &str| Quantity(s.to_string());
    ResourceRequirements {
        requests: Some(BTreeMap::from([
            ("cpu".into(), q("1200m")),
            ("memory".into(), q("1Gi")),
        ])),
        limits: Some(BTreeMap::from([
            ("cpu".into(), q("2")),
            ("memory".into(), q("1Gi")),
        ])),
        ..Default::default()
    }
}

/// Build the `ResourceRequirements` to set on the Redis container, merging
/// any user-provided values into the defaults per key. Overriding `cpu`
/// alone preserves the default memory settings, and vice versa.
pub fn effective_redis_resources(user: Option<&ResourcesSpec>) -> ResourceRequirements {
    let mut rr = default_redis_resources();
    let Some(user) = user else { return rr };
    if let Some(user_requests) = &user.requests {
        let requests = rr.requests.get_or_insert_with(BTreeMap::new);
        for (k, v) in user_requests {
            requests.insert(k.clone(), Quantity(v.clone()));
        }
    }
    if let Some(user_limits) = &user.limits {
        let limits = rr.limits.get_or_insert_with(BTreeMap::new);
        for (k, v) in user_limits {
            limits.insert(k.clone(), Quantity(v.clone()));
        }
    }
    rr
}

/// Redis's own default snapshot triggers, and what the `redis:8` image runs
/// with when nothing overrides it.
const DEFAULT_SAVE_POINTS: &str = "3600 1 300 100 60 10000";

/// The `redis-server` flags implementing a `PersistenceSpec`, ready to be
/// appended to the command line. Includes the leading space; returns flags for
/// the default (RDB + AOF, fsync everysec) when `user` is `None`.
///
/// `--save` and `--appendonly` are always emitted explicitly, even when they
/// match Redis's built-in defaults, so the CR stays the single source of truth
/// — otherwise a base image that changed its defaults, or a `redis.conf` that
/// Sentinel rewrote after a failover, would silently decide durability. For
/// the same reason these are command-line flags rather than conf entries:
/// arguments are applied after the config file, so they win over anything
/// `CONFIG REWRITE` persisted.
///
/// `--dir /data` is unconditional. `/data` is always mounted — the PVC when
/// `storage` is set, an emptyDir otherwise — and both kinds keep other state
/// there too (`nodes.conf` for a cluster, the Sentinel-managed `redis.conf`
/// for a Sentinel-enabled Redis).
pub fn persistence_args(user: Option<&PersistenceSpec>) -> String {
    let default = PersistenceSpec::default();
    let p = user.unwrap_or(&default);

    let rdb_on = p.enabled && p.rdb.as_ref().is_none_or(|r| r.enabled);
    let aof_on = p.enabled && p.aof.as_ref().is_none_or(|a| a.enabled);

    // Interpolated into a double-quoted shell word, which is safe because the
    // CRD schema constrains every entry to `^\d+ \d+$`.
    let save = if rdb_on {
        p.rdb
            .as_ref()
            .and_then(|r| r.save.as_deref())
            .map_or(DEFAULT_SAVE_POINTS.to_string(), |pts| pts.join(" "))
    } else {
        String::new()
    };

    let mut args = format!(" --dir /data --save \"{save}\"");
    if aof_on {
        let fsync = p
            .aof
            .as_ref()
            .and_then(|a| a.fsync)
            .unwrap_or_default()
            .as_redis_value();
        args.push_str(&format!(" --appendonly yes --appendfsync {fsync}"));
    } else {
        args.push_str(" --appendonly no");
    }
    args
}

/// Compute the `--maxmemory` value to pass to redis-server: 70% of the
/// effective memory limit (or request, when a limit isn't set). Leaves
/// 30% headroom for the COW buffer during BGSAVE/AOF rewrites, replication
/// backlog, client output buffers, and allocator fragmentation. Returns None
/// only if no parseable memory value is configured.
pub fn maxmemory_bytes(rr: &ResourceRequirements) -> Option<u64> {
    let mem = rr
        .limits
        .as_ref()
        .and_then(|m| m.get("memory"))
        .or_else(|| rr.requests.as_ref().and_then(|m| m.get("memory")))?;
    let bytes = parse_quantity_bytes(&mem.0)?;
    Some(bytes * 7 / 10)
}

/// Parse a Kubernetes resource quantity (memory) into bytes. Handles the
/// binary suffixes (Ki/Mi/Gi/Ti/Pi/Ei) and the decimal suffixes (K/M/G/T/P/E),
/// plus plain byte counts. Returns None for unparseable input — callers fall
/// back to omitting `--maxmemory` rather than guessing.
pub fn parse_quantity_bytes(q: &str) -> Option<u64> {
    const KI: u64 = 1024;
    let q = q.trim();
    let suffixes: &[(&str, u64)] = &[
        ("Ki", KI),
        ("Mi", KI.pow(2)),
        ("Gi", KI.pow(3)),
        ("Ti", KI.pow(4)),
        ("Pi", KI.pow(5)),
        ("Ei", KI.pow(6)),
        ("K", 1_000),
        ("M", 1_000_000),
        ("G", 1_000_000_000),
        ("T", 1_000_000_000_000),
        ("P", 1_000_000_000_000_000),
        ("E", 1_000_000_000_000_000_000),
    ];
    for (suf, mult) in suffixes {
        if let Some(n) = q.strip_suffix(suf) {
            let val: f64 = n.trim().parse().ok()?;
            return Some((val * *mult as f64) as u64);
        }
    }
    q.parse::<u64>().ok()
}

/// Server-side apply for k8s-openapi typed resources. Injects apiVersion/kind
/// (which k8s-openapi types omit on serialize) and force-applies as
/// `redis-operator`.
pub async fn apply<K>(api: &Api<K>, name: &str, obj: &K) -> Result<()>
where
    K: Resource<DynamicType = ()>
        + Clone
        + std::fmt::Debug
        + DeserializeOwned
        + Serialize
        + k8s_openapi::Resource,
{
    let mut value = serde_json::to_value(obj)?;
    if let Value::Object(m) = &mut value {
        m.insert(
            "apiVersion".to_string(),
            Value::String(K::API_VERSION.to_string()),
        );
        m.insert("kind".to_string(), Value::String(K::KIND.to_string()));
    }
    let pp = PatchParams::apply(FIELD_MANAGER).force();
    api.patch(name, &pp, &Patch::Apply(&value)).await?;
    Ok(())
}

/// Outcome of validating a user-supplied PDB against a workload's real
/// disruption tolerance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PdbVerdict {
    /// Requested disruption is within the safe bound.
    Ok,
    /// Workload is too small for a PDB to protect anything (fewer than 2 pods).
    /// Applied as asked; the user is told it does nothing.
    NotApplicable,
    /// Neither `minAvailable` nor `maxUnavailable` set — nothing to apply.
    Empty,
    /// Both set. The API server rejects this outright, so refuse before it 422s.
    BothSet,
    /// Would permit more concurrent eviction than the workload survives.
    Unsafe { requested: i32, allowed: i32 },
    /// A value the operator cannot resolve — the API server would reject it too.
    Unresolvable { field: &'static str, value: String },
}

impl PdbVerdict {
    /// True when the spec should be sent to the API server.
    pub fn should_apply(&self) -> bool {
        matches!(self, PdbVerdict::Ok | PdbVerdict::NotApplicable)
    }

    /// Event reason. `None` for verdicts that warrant no Event.
    pub fn reason(&self) -> Option<&'static str> {
        match self {
            PdbVerdict::Ok | PdbVerdict::Empty => None,
            PdbVerdict::NotApplicable => Some("IneffectivePodDisruptionBudget"),
            PdbVerdict::BothSet | PdbVerdict::Unresolvable { .. } => {
                Some("InvalidPodDisruptionBudget")
            }
            PdbVerdict::Unsafe { .. } => Some("UnsafePodDisruptionBudget"),
        }
    }

    /// Human-readable note for the Event and the log line.
    pub fn note(&self) -> Option<String> {
        match self {
            PdbVerdict::Ok | PdbVerdict::Empty => None,
            PdbVerdict::NotApplicable => Some(
                "podDisruptionBudget applied, but this workload has fewer than 2 pods \
                 so the budget cannot protect anything"
                    .to_string(),
            ),
            PdbVerdict::BothSet => Some(
                "podDisruptionBudget sets both minAvailable and maxUnavailable, which \
                 the API server rejects; leaving the PDB unchanged"
                    .to_string(),
            ),
            PdbVerdict::Unresolvable { field, value } => Some(format!(
                "podDisruptionBudget {field} value {value:?} is not a valid integer or \
                 percentage; leaving the PDB unchanged"
            )),
            PdbVerdict::Unsafe { requested, allowed } => Some(format!(
                "podDisruptionBudget permits {requested} concurrent disruption(s) but \
                 this workload survives at most {allowed}; leaving the PDB unchanged"
            )),
        }
    }
}

/// Resolve a PDB `IntOrString` against a known workload size, mirroring
/// `intstr.GetScaledValueFromIntOrPercent(v, total, roundUp = true)` — the exact
/// call the disruption controller makes for **both** `minAvailable` and
/// `maxUnavailable`.
///
/// The rounding direction matters: rounding up makes a `minAvailable` percentage
/// stricter but a `maxUnavailable` percentage *looser*, so rounding down here
/// would under-report the disruption a budget permits and wave through unsafe
/// configs. Returns `None` for anything the API server would reject (bare-number
/// strings, a missing `%`, non-numeric, out of range).
pub fn resolve_int_or_percent(v: &IntOrString, total: i32) -> Option<i32> {
    match v {
        IntOrString::Int(n) => Some(*n),
        IntOrString::String(s) => {
            let pct: i64 = s.trim().strip_suffix('%')?.trim().parse().ok()?;
            if !(0..=100).contains(&pct) {
                return None;
            }
            // Ceiling division, matching roundUp = true.
            Some(((pct * total as i64 + 99) / 100) as i32)
        }
    }
}

/// Concurrent evictions a PDB spec permits over a workload of `total` pods,
/// normalising both spec forms to a single number.
pub fn pdb_effective_max_unavailable(
    spec: &PodDisruptionBudgetSpec,
    total: i32,
) -> std::result::Result<i32, PdbVerdict> {
    match (&spec.min_available, &spec.max_unavailable) {
        (Some(_), Some(_)) => Err(PdbVerdict::BothSet),
        (None, None) => Err(PdbVerdict::Empty),
        (Some(min), None) => resolve_int_or_percent(min, total)
            .map(|m| total - m)
            .ok_or_else(|| PdbVerdict::Unresolvable {
                field: "minAvailable",
                value: int_or_string_display(min),
            }),
        (None, Some(max)) => {
            resolve_int_or_percent(max, total).ok_or_else(|| PdbVerdict::Unresolvable {
                field: "maxUnavailable",
                value: int_or_string_display(max),
            })
        }
    }
}

fn int_or_string_display(v: &IntOrString) -> String {
    match v {
        IntOrString::Int(n) => n.to_string(),
        IntOrString::String(s) => s.clone(),
    }
}

/// Compare the disruption a budget permits against what the workload survives.
pub fn validate_pdb(spec: &PodDisruptionBudgetSpec, total: i32, safe: i32) -> PdbVerdict {
    let requested = match pdb_effective_max_unavailable(spec, total) {
        Ok(n) => n,
        Err(verdict) => return verdict,
    };
    if total < 2 {
        return PdbVerdict::NotApplicable;
    }
    if requested > safe {
        return PdbVerdict::Unsafe {
            requested,
            allowed: safe,
        };
    }
    PdbVerdict::Ok
}

/// Build a `policy/v1` PodDisruptionBudget selecting `labels`.
pub fn build_pdb(
    name: &str,
    ns: &str,
    labels: &BTreeMap<String, String>,
    owner: OwnerReference,
    spec: &PodDisruptionBudgetSpec,
) -> PodDisruptionBudget {
    PodDisruptionBudget {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec: Some(K8sPdbSpec {
            min_available: spec.min_available.clone(),
            max_unavailable: spec.max_unavailable.clone(),
            unhealthy_pod_eviction_policy: spec.unhealthy_pod_eviction_policy.clone(),
            selector: Some(LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            }),
        }),
        status: None,
    }
}

/// Everything one PDB reconcile needs. A struct because the positional form
/// would be eight arguments.
pub struct PdbRequest<'a> {
    /// Name of the PDB object; matches the StatefulSet it protects.
    pub name: &'a str,
    pub ns: &'a str,
    /// Pod labels the PDB selects — must equal the StatefulSet's pod-template
    /// labels.
    pub labels: &'a BTreeMap<String, String>,
    pub owner: OwnerReference,
    /// The CR the Event is attached to, from `cr.object_ref(&())`.
    pub obj_ref: &'a ObjectReference,
    /// User spec, or the operator's default. `None` deletes any existing PDB.
    pub desired: Option<PodDisruptionBudgetSpec>,
    /// Pods the selector matches — the StatefulSet's `.spec.replicas`. This is
    /// what the disruption controller uses as `expectedCount`, which is why
    /// percentages can be resolved without reading live state.
    pub total: i32,
    /// Pods that may be concurrently unavailable without losing availability.
    pub safe_max_unavailable: i32,
}

/// Validate a desired PDB, then apply it, skip it, or delete it.
///
/// Never returns `Err` for a bad user budget — only for genuine API failures —
/// so one malformed budget cannot wedge the rest of the reconcile.
///
/// On a refusal the existing PDB is deliberately left in place rather than
/// deleted: the only PDBs the operator ever applies are validated ones, so
/// whatever is live is either absent or previously safe, and tearing it down
/// would strip protection at exactly the moment the user's config broke. A PDB
/// is deleted only when `desired` is `None` — the field was removed, or the
/// workload it protected is gone.
pub async fn reconcile_pdb(
    api: &Api<PodDisruptionBudget>,
    ctx: &Context,
    req: PdbRequest<'_>,
) -> Result<PdbVerdict> {
    // `None` (field removed, workload gone) and a spec that sets neither field
    // both mean "no budget wanted". Deleting outright — rather than leaving it
    // to owner-reference GC — makes toggling the field off take effect at once.
    let verdict = match &req.desired {
        Some(spec) => validate_pdb(spec, req.total, req.safe_max_unavailable),
        None => PdbVerdict::Empty,
    };
    if verdict == PdbVerdict::Empty {
        return match api.delete(req.name, &DeleteParams::default()).await {
            Ok(_) => Ok(PdbVerdict::Empty),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(PdbVerdict::Empty),
            Err(e) => Err(e.into()),
        };
    }
    let spec = req.desired.as_ref().expect("Empty covers the None case");

    if let (Some(reason), Some(note)) = (verdict.reason(), verdict.note()) {
        let type_ = if matches!(verdict, PdbVerdict::NotApplicable) {
            EventType::Normal
        } else {
            EventType::Warning
        };
        if type_ == EventType::Warning {
            warn!(pdb = %req.name, %note, "refusing pod disruption budget");
        } else {
            info!(pdb = %req.name, %note);
        }
        emit(
            ctx,
            req.obj_ref,
            Event {
                type_,
                reason: reason.to_string(),
                note: Some(note),
                action: "ReconcilePodDisruptionBudget".to_string(),
                secondary: None,
            },
        )
        .await;
    }

    if verdict.should_apply() {
        let pdb = build_pdb(req.name, req.ns, req.labels, req.owner, spec);
        apply(api, req.name, &pdb).await?;
    }

    Ok(verdict)
}

/// Default soft spread for a workload: one constraint, `maxSkew` 1 across
/// hostnames, selecting the workload's own pods.
///
/// `ScheduleAnyway` on purpose — `DoNotSchedule` would leave pods Pending
/// forever on kind/minikube/CI and on any cluster with fewer nodes than
/// replicas. Users who want a hard guarantee override the field.
pub fn default_topology_spread(labels: &BTreeMap<String, String>) -> Vec<TopologySpreadConstraint> {
    vec![TopologySpreadConstraint {
        max_skew: 1,
        topology_key: "kubernetes.io/hostname".to_string(),
        when_unsatisfiable: "ScheduleAnyway".to_string(),
        label_selector: Some(LabelSelector {
            match_labels: Some(labels.clone()),
            ..Default::default()
        }),
        ..Default::default()
    }]
}

/// `None` -> operator default; `Some([])` -> explicit opt-out; `Some(v)` -> the
/// user's list verbatim.
///
/// The empty-vec case maps to `None` rather than an empty list: both are
/// equivalent to the scheduler, but only omitting the field releases the
/// operator's server-side-apply ownership of it cleanly.
pub fn effective_topology_spread(
    user: Option<&Vec<TopologySpreadConstraint>>,
    labels: &BTreeMap<String, String>,
) -> Option<Vec<TopologySpreadConstraint>> {
    match user {
        None => Some(default_topology_spread(labels)),
        Some(v) if v.is_empty() => None,
        Some(v) => Some(v.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::crd::redis::{AofSpec, FsyncPolicy, RdbSpec};

    #[test]
    fn persistence_defaults_to_rdb_and_aof_everysec() {
        // An absent block and an empty one must agree.
        let expected = " --dir /data --save \"3600 1 300 100 60 10000\" \
                        --appendonly yes --appendfsync everysec";
        assert_eq!(persistence_args(None), expected);
        assert_eq!(
            persistence_args(Some(&PersistenceSpec::default())),
            expected
        );
    }

    #[test]
    fn persistence_master_switch_disables_both() {
        let p = PersistenceSpec {
            enabled: false,
            // Explicitly on, and still overridden by the master switch.
            rdb: Some(RdbSpec::default()),
            aof: Some(AofSpec::default()),
        };
        assert_eq!(
            persistence_args(Some(&p)),
            " --dir /data --save \"\" --appendonly no"
        );
    }

    #[test]
    fn persistence_disabling_rdb_keeps_aof() {
        let p = PersistenceSpec {
            rdb: Some(RdbSpec {
                enabled: false,
                save: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            persistence_args(Some(&p)),
            " --dir /data --save \"\" --appendonly yes --appendfsync everysec"
        );
    }

    #[test]
    fn persistence_disabling_aof_keeps_rdb() {
        let p = PersistenceSpec {
            aof: Some(AofSpec {
                enabled: false,
                fsync: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            persistence_args(Some(&p)),
            " --dir /data --save \"3600 1 300 100 60 10000\" --appendonly no"
        );
    }

    #[test]
    fn persistence_joins_save_points_into_one_arg() {
        let p = PersistenceSpec {
            rdb: Some(RdbSpec {
                enabled: true,
                save: Some(vec!["900 1".into(), "300 10".into()]),
            }),
            ..Default::default()
        };
        assert!(persistence_args(Some(&p)).contains("--save \"900 1 300 10\""));
    }

    #[test]
    fn persistence_empty_save_list_disables_snapshots() {
        let p = PersistenceSpec {
            rdb: Some(RdbSpec {
                enabled: true,
                save: Some(vec![]),
            }),
            ..Default::default()
        };
        assert!(persistence_args(Some(&p)).contains("--save \"\""));
    }

    #[test]
    fn persistence_never_maps_to_redis_no() {
        let p = PersistenceSpec {
            aof: Some(AofSpec {
                enabled: true,
                fsync: Some(FsyncPolicy::Never),
            }),
            ..Default::default()
        };
        assert!(persistence_args(Some(&p)).ends_with("--appendonly yes --appendfsync no"));
    }

    #[test]
    fn persistence_always_is_passed_through() {
        let p = PersistenceSpec {
            aof: Some(AofSpec {
                enabled: true,
                fsync: Some(FsyncPolicy::Always),
            }),
            ..Default::default()
        };
        assert!(persistence_args(Some(&p)).ends_with("--appendfsync always"));
    }

    #[test]
    fn parse_quantity_handles_binary_suffixes() {
        assert_eq!(parse_quantity_bytes("1Ki"), Some(1024));
        assert_eq!(parse_quantity_bytes("1Mi"), Some(1024 * 1024));
        assert_eq!(parse_quantity_bytes("1Gi"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_quantity_bytes("512Mi"), Some(512 * 1024 * 1024));
    }

    #[test]
    fn parse_quantity_handles_decimal_suffixes() {
        assert_eq!(parse_quantity_bytes("1K"), Some(1_000));
        assert_eq!(parse_quantity_bytes("2G"), Some(2_000_000_000));
    }

    #[test]
    fn parse_quantity_handles_plain_bytes() {
        assert_eq!(parse_quantity_bytes("1024"), Some(1024));
    }

    #[test]
    fn parse_quantity_rejects_garbage() {
        assert_eq!(parse_quantity_bytes("abc"), None);
        assert_eq!(parse_quantity_bytes(""), None);
    }

    #[test]
    fn maxmemory_takes_70_percent_of_limit() {
        let rr = default_redis_resources();
        let m = maxmemory_bytes(&rr).expect("should derive maxmemory");
        // 1Gi = 1073741824, * 7 / 10 = 751619276
        assert_eq!(m, 1024u64.pow(3) * 7 / 10);
    }

    #[test]
    fn maxmemory_falls_back_to_request_when_no_limit() {
        let rr = ResourceRequirements {
            requests: Some(BTreeMap::from([(
                "memory".to_string(),
                Quantity("2Gi".to_string()),
            )])),
            limits: None,
            ..Default::default()
        };
        let m = maxmemory_bytes(&rr).expect("should derive maxmemory");
        assert_eq!(m, 2 * 1024u64.pow(3) * 7 / 10);
    }

    #[test]
    fn effective_resources_merges_user_cpu_with_default_memory() {
        let user = ResourcesSpec {
            requests: Some(BTreeMap::from([("cpu".into(), "500m".into())])),
            limits: Some(BTreeMap::from([("cpu".into(), "1".into())])),
        };
        let rr = effective_redis_resources(Some(&user));
        let req = rr.requests.unwrap();
        let lim = rr.limits.unwrap();
        assert_eq!(req.get("cpu").unwrap().0, "500m");
        assert_eq!(req.get("memory").unwrap().0, "1Gi");
        assert_eq!(lim.get("cpu").unwrap().0, "1");
        assert_eq!(lim.get("memory").unwrap().0, "1Gi");
    }

    fn pct(s: &str) -> IntOrString {
        IntOrString::String(s.to_string())
    }

    fn min_available(v: IntOrString) -> PodDisruptionBudgetSpec {
        PodDisruptionBudgetSpec {
            min_available: Some(v),
            ..Default::default()
        }
    }

    fn max_unavailable(v: IntOrString) -> PodDisruptionBudgetSpec {
        PodDisruptionBudgetSpec {
            max_unavailable: Some(v),
            ..Default::default()
        }
    }

    fn test_labels() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("app.kubernetes.io/name".to_string(), "redis".to_string()),
            (
                "app.kubernetes.io/instance".to_string(),
                "cache".to_string(),
            ),
        ])
    }

    #[test]
    fn resolve_percent_rounds_up_like_apiserver() {
        assert_eq!(resolve_int_or_percent(&pct("50%"), 3), Some(2));
        assert_eq!(resolve_int_or_percent(&pct("50%"), 5), Some(3));
        assert_eq!(resolve_int_or_percent(&pct("34%"), 3), Some(2));
    }

    #[test]
    fn resolve_percent_handles_bounds() {
        assert_eq!(resolve_int_or_percent(&pct("0%"), 6), Some(0));
        assert_eq!(resolve_int_or_percent(&pct("100%"), 6), Some(6));
    }

    #[test]
    fn resolve_int_passes_through_unscaled() {
        assert_eq!(resolve_int_or_percent(&IntOrString::Int(2), 6), Some(2));
    }

    #[test]
    fn resolve_rejects_bare_number_string() {
        assert_eq!(resolve_int_or_percent(&pct("3"), 6), None);
    }

    #[test]
    fn resolve_rejects_garbage() {
        for s in ["abc", "", "%", "-10%", "150%"] {
            assert_eq!(resolve_int_or_percent(&pct(s), 6), None, "input {s:?}");
        }
    }

    #[test]
    fn effective_max_unavailable_from_min_available() {
        let spec = min_available(IntOrString::Int(5));
        assert_eq!(pdb_effective_max_unavailable(&spec, 6), Ok(1));
    }

    #[test]
    fn effective_max_unavailable_from_min_available_percent() {
        // 50% of 5 rounds up to 3, leaving 2 disruptable.
        let spec = min_available(pct("50%"));
        assert_eq!(pdb_effective_max_unavailable(&spec, 5), Ok(2));
    }

    #[test]
    fn effective_max_unavailable_from_max_unavailable_percent() {
        // Rounds UP to 3, not down to 2 — rounding down here would under-report
        // the disruption permitted and wave through unsafe budgets.
        let spec = max_unavailable(pct("50%"));
        assert_eq!(pdb_effective_max_unavailable(&spec, 5), Ok(3));
    }

    #[test]
    fn effective_max_unavailable_rejects_both_set() {
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(2)),
            max_unavailable: Some(IntOrString::Int(1)),
            ..Default::default()
        };
        assert_eq!(
            pdb_effective_max_unavailable(&spec, 6),
            Err(PdbVerdict::BothSet)
        );
    }

    #[test]
    fn effective_max_unavailable_rejects_neither_set() {
        let spec = PodDisruptionBudgetSpec::default();
        assert_eq!(
            pdb_effective_max_unavailable(&spec, 6),
            Err(PdbVerdict::Empty)
        );
    }

    #[test]
    fn validate_pdb_accepts_at_the_bound() {
        let spec = max_unavailable(IntOrString::Int(1));
        assert_eq!(validate_pdb(&spec, 6, 1), PdbVerdict::Ok);
    }

    #[test]
    fn validate_pdb_rejects_above_the_bound() {
        let spec = max_unavailable(IntOrString::Int(2));
        assert_eq!(
            validate_pdb(&spec, 6, 1),
            PdbVerdict::Unsafe {
                requested: 2,
                allowed: 1
            }
        );
    }

    #[test]
    fn validate_pdb_accepts_min_available_at_the_bound() {
        let spec = min_available(IntOrString::Int(5));
        assert_eq!(validate_pdb(&spec, 6, 1), PdbVerdict::Ok);
    }

    #[test]
    fn validate_pdb_rejects_min_available_below_the_bound() {
        let spec = min_available(IntOrString::Int(3));
        assert_eq!(
            validate_pdb(&spec, 6, 1),
            PdbVerdict::Unsafe {
                requested: 3,
                allowed: 1
            }
        );
    }

    #[test]
    fn validate_pdb_rejects_zero_bound_workload() {
        let spec = max_unavailable(IntOrString::Int(1));
        assert_eq!(
            validate_pdb(&spec, 3, 0),
            PdbVerdict::Unsafe {
                requested: 1,
                allowed: 0
            }
        );
    }

    #[test]
    fn validate_pdb_rejects_unresolvable_percentage() {
        let spec = max_unavailable(pct("abc"));
        assert_eq!(
            validate_pdb(&spec, 6, 1),
            PdbVerdict::Unresolvable {
                field: "maxUnavailable",
                value: "abc".to_string()
            }
        );
    }

    #[test]
    fn validate_pdb_not_applicable_for_single_pod_workload() {
        let spec = max_unavailable(IntOrString::Int(1));
        assert_eq!(validate_pdb(&spec, 1, 0), PdbVerdict::NotApplicable);
    }

    #[test]
    fn only_ok_and_not_applicable_are_applied() {
        assert!(PdbVerdict::Ok.should_apply());
        assert!(PdbVerdict::NotApplicable.should_apply());
        assert!(!PdbVerdict::Empty.should_apply());
        assert!(!PdbVerdict::BothSet.should_apply());
        assert!(
            !PdbVerdict::Unsafe {
                requested: 2,
                allowed: 1
            }
            .should_apply()
        );
    }

    #[test]
    fn build_pdb_selects_the_workload_labels() {
        let l = test_labels();
        let spec = max_unavailable(IntOrString::Int(1));
        let pdb = build_pdb("cache", "ns", &l, OwnerReference::default(), &spec);
        let selector = pdb.spec.unwrap().selector.unwrap();
        assert_eq!(selector.match_labels.unwrap(), l);
    }

    #[test]
    fn build_pdb_sets_owner_reference() {
        let spec = max_unavailable(IntOrString::Int(1));
        let owner = OwnerReference {
            name: "cache".to_string(),
            ..Default::default()
        };
        let pdb = build_pdb("cache", "ns", &test_labels(), owner, &spec);
        let refs = pdb.metadata.owner_references.unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "cache");
        assert_eq!(pdb.metadata.namespace.unwrap(), "ns");
    }

    #[test]
    fn build_pdb_passes_through_unhealthy_pod_eviction_policy() {
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(2)),
            unhealthy_pod_eviction_policy: Some("AlwaysAllow".to_string()),
            ..Default::default()
        };
        let pdb = build_pdb(
            "cache",
            "ns",
            &test_labels(),
            OwnerReference::default(),
            &spec,
        );
        assert_eq!(
            pdb.spec.unwrap().unhealthy_pod_eviction_policy.unwrap(),
            "AlwaysAllow"
        );
    }

    #[test]
    fn default_topology_spread_is_soft_hostname_skew_one() {
        let c = default_topology_spread(&test_labels());
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].max_skew, 1);
        assert_eq!(c[0].topology_key, "kubernetes.io/hostname");
        // Must stay soft: DoNotSchedule would leave pods Pending on any cluster
        // with fewer nodes than replicas.
        assert_eq!(c[0].when_unsatisfiable, "ScheduleAnyway");
    }

    #[test]
    fn default_topology_spread_selector_matches_workload_labels() {
        let l = test_labels();
        let c = default_topology_spread(&l);
        assert_eq!(
            c[0].label_selector.clone().unwrap().match_labels.unwrap(),
            l
        );
    }

    #[test]
    fn effective_topology_spread_none_yields_default() {
        let l = test_labels();
        assert_eq!(
            effective_topology_spread(None, &l),
            Some(default_topology_spread(&l))
        );
    }

    #[test]
    fn effective_topology_spread_empty_vec_opts_out() {
        assert_eq!(
            effective_topology_spread(Some(&vec![]), &test_labels()),
            None
        );
    }

    #[test]
    fn effective_topology_spread_user_override_wins_verbatim() {
        let user = vec![TopologySpreadConstraint {
            max_skew: 2,
            topology_key: "topology.kubernetes.io/zone".to_string(),
            when_unsatisfiable: "DoNotSchedule".to_string(),
            ..Default::default()
        }];
        assert_eq!(
            effective_topology_spread(Some(&user), &test_labels()),
            Some(user.clone())
        );
    }
}
