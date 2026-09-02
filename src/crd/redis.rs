use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::TopologySpreadConstraint;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[kube(
    group = "redis.jgaskins.dev",
    version = "v1alpha1",
    kind = "Redis",
    plural = "redises",
    singular = "redis",
    shortname = "rds",
    namespaced,
    status = "RedisStatus",
    derive = "PartialEq",
    derive = "Default",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Primary","type":"string","jsonPath":".status.masterPod"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Desired","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Connected","type":"integer","jsonPath":".status.connectedReplicas","description":"Replicas connected to primary"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RedisSpec {
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    #[serde(default = "default_image")]
    pub image: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageSpec>,

    /// Container resource requests/limits for the Redis pods. Defaults: 1Gi
    /// memory (requests and limits), 1.2 CPU requests, 2 CPU limits. User
    /// values are merged per-key with the defaults — overriding `cpu` alone
    /// preserves the default memory settings, and vice versa.
    ///
    /// `maxmemory` is derived from the effective memory limit (70%) and
    /// passed to redis-server, so users don't need to keep it in sync
    /// manually.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesSpec>,

    /// What Redis does with the keyspace once it reaches `maxmemory`
    /// (`maxmemory-policy`). Defaults to `noeviction`: writes that would
    /// exceed the ceiling fail with an OOM error and nothing is dropped.
    ///
    /// The policy is applied to every pod, replicas included, because a
    /// StatefulSet has one pod template and any replica may be promoted. That
    /// is also the correct arrangement: since Redis 5 a replica does not evict
    /// on its own, it waits for the primary's `DEL`, so the setting only takes
    /// effect on whichever pod is currently master.
    ///
    /// Eviction and persistence are a deliberate combination, not a default —
    /// an evicted key is gone from the RDB/AOF too. Leave this at `noeviction`
    /// unless the workload is a cache. See `EvictionPolicy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eviction_policy: Option<EvictionPolicy>,

    /// How the dataset is written to disk. Defaults to both RDB and AOF on,
    /// with AOF fsyncing once a second. See `PersistenceSpec`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<PersistenceSpec>,

    /// When set, the operator deploys a Redis Sentinel StatefulSet alongside
    /// the Redis pods to provide automatic failover. Clients that aren't
    /// Sentinel-aware can still use the `<name>-primary` Service, whose
    /// selector the operator updates to track the current master.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sentinel: Option<SentinelSpec>,

    /// Overrides the automatic PodDisruptionBudget for the Redis data pods.
    ///
    /// Created automatically whenever `replicas` is 2 or more, with
    /// `maxUnavailable: 1` and `unhealthyPodEvictionPolicy: AlwaysAllow` — one
    /// pod disrupted at a time, which keeps the workload usable through a node
    /// drain and matches Sentinel's `parallel-syncs 1` so failovers stay
    /// serialised. Below 2 replicas none is created, because a budget over a
    /// single pod either does nothing or blocks every drain.
    ///
    /// Set explicitly to override; set to `{}` (neither field) to opt out and
    /// have no PDB at all. Overrides are validated against `replicas` — at most
    /// `replicas - 1` may be unavailable at once, so at least one copy of the
    /// data survives — and an unsafe budget is refused rather than applied. See
    /// `PodDisruptionBudgetSpec`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_disruption_budget: Option<PodDisruptionBudgetSpec>,

    /// Overrides the operator's default pod spread for the Redis data pods.
    ///
    /// By default each workload gets a single soft constraint — `maxSkew: 1`
    /// over `kubernetes.io/hostname` with `whenUnsatisfiable: ScheduleAnyway` —
    /// selecting its own pods, so the scheduler prefers a different node per pod
    /// but still schedules on clusters with fewer nodes than replicas. Set to
    /// `[]` to emit no constraints at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology_spread_constraints: Option<Vec<TopologySpreadConstraint>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SentinelSpec {
    /// Number of Sentinel pods. Should be ≥ 3 — Sentinel needs majority
    /// quorum to declare a master down, so 2 sentinels can't tolerate any
    /// failure.
    #[serde(default = "default_sentinel_replicas")]
    pub replicas: i32,

    /// Quorum required to flag the master as objectively down and trigger
    /// failover. Defaults to majority of `replicas`. Lower values risk
    /// split-brain during network partitions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quorum: Option<i32>,

    /// Name this Sentinel set uses for the monitored master. Defaults to
    /// `mymaster`. Worth setting when several Redis instances share one
    /// Sentinel deployment, or to match a name clients are already
    /// configured with.
    ///
    /// Restricted to the characters Sentinel itself accepts in a master
    /// name — alphanumerics, `.`, `_` and `-`.
    ///
    /// Changing it on a running instance is disruptive: `sentinel.conf` is
    /// only written on first boot, so existing sentinels keep monitoring the
    /// old name while the operator asks for the new one, and master tracking
    /// stalls on the last known selector. Recreate the sentinel pods (and
    /// their PVCs, if `storage` is set) to adopt the new name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("pattern" = r"^[A-Za-z0-9._-]+$", "maxLength" = 128))]
    pub master_name: Option<String>,

    /// Override image for Sentinel pods. Defaults to the Redis spec image —
    /// `redis-sentinel` ships in the same container as `redis-server`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Persistent storage for `sentinel.conf`. When set, sentinels survive
    /// pod restarts with their persisted master view, avoiding the brief
    /// re-monitor-pod-0 window after an unrelated sentinel restart. When
    /// unset, sentinels use emptyDir and rediscover topology via INFO on
    /// each restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageSpec>,

    /// Container resource requests/limits. When omitted, the operator
    /// applies a safe default (cpu 50m, mem 64Mi requests; mem 128Mi limit)
    /// so the pod isn't BestEffort QoS — BestEffort sentinels get throttled
    /// into TILT mode under node pressure. To opt out of the default and
    /// run BestEffort intentionally, set `resources: {}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesSpec>,

    /// Overrides the automatic Sentinel PodDisruptionBudget.
    ///
    /// Unlike the other budgets, this one is created automatically: losing
    /// Sentinel quorum silently disables failover, so it is opt-out rather than
    /// opt-in. Whenever Sentinel is enabled with 3 or more replicas the operator
    /// creates a PDB whose `minAvailable` is the number of sentinels needed both
    /// to agree the master is down (`quorum`) and to elect a failover leader (a
    /// majority of the sentinel set) — whichever is larger — with
    /// `unhealthyPodEvictionPolicy: AlwaysAllow`.
    ///
    /// Below 3 replicas no PDB is created, because `minAvailable` would equal
    /// the replica count and block every node drain forever.
    ///
    /// Set explicitly to override; set to `{}` (neither field) to opt out and
    /// have no PDB at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_disruption_budget: Option<PodDisruptionBudgetSpec>,

    /// Overrides the operator's default pod spread for the Sentinel pods. Same
    /// soft `maxSkew: 1` hostname default as the Redis pods, selecting only the
    /// Sentinel pods so the two workloads spread independently. Set to `[]` to
    /// emit no constraints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology_spread_constraints: Option<Vec<TopologySpreadConstraint>>,
}

/// Mirrors the shape of `corev1.ResourceRequirements`, deliberately simplified.
///
/// The upstream type is usable in a CRD schema (k8s-openapi implements
/// `JsonSchema` under its `schemars` feature — `TopologySpreadConstraint` above
/// is used directly), but it also carries `claims` and types its values as
/// `Quantity`, which renders as `x-kubernetes-int-or-string`. Plain
/// `BTreeMap<String, String>` keeps the schema and the merge logic in
/// `effective_redis_resources` simple.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<BTreeMap<String, String>>,
}

/// Mirrors `policy/v1` `PodDisruptionBudgetSpec`. The operator builds and owns
/// the PDB, selecting on the workload's own pods, and validates it against the
/// number of concurrent evictions that workload can actually survive.
///
/// An unsafe budget is refused, not clamped: the operator leaves any existing
/// PDB untouched, logs, emits a Warning Event on the CR (visible in `kubectl
/// describe`), and sets `status.phase` to `Degraded`.
///
/// Exactly one of `minAvailable` / `maxUnavailable` may be set — setting both is
/// rejected by the API server. Setting neither leaves the PDB absent.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PodDisruptionBudgetSpec {
    /// Integer or percentage (e.g. `1` or `"50%"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_available: Option<IntOrString>,

    /// Integer or percentage. Mutually exclusive with `minAvailable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_unavailable: Option<IntOrString>,

    /// `IfHealthyBudget` (the Kubernetes default) or `AlwaysAllow`.
    ///
    /// Under `IfHealthyBudget`, a pod that is *already* unready cannot be
    /// evicted once the budget is spent — which stalls `kubectl drain` during
    /// exactly the incident you are draining for. `AlwaysAllow` lets an
    /// already-unready pod go, since it is not serving traffic anyway, while
    /// healthy pods are still held to the budget. Requires Kubernetes 1.27+
    /// (GA in 1.31); older API servers silently drop the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unhealthy_pod_eviction_policy: Option<String>,
}

/// Sentinel's own default master name, and the operator's when
/// `sentinel.masterName` is unset.
pub const DEFAULT_MASTER_NAME: &str = "mymaster";

impl SentinelSpec {
    /// Effective quorum: explicit value if set, otherwise majority of
    /// `replicas` (e.g. 2 of 3).
    pub fn effective_quorum(&self) -> i32 {
        self.quorum.unwrap_or(self.replicas / 2 + 1)
    }

    /// Effective master name: explicit value if set, otherwise `mymaster`.
    pub fn effective_master_name(&self) -> &str {
        self.master_name.as_deref().unwrap_or(DEFAULT_MASTER_NAME)
    }

    /// Sentinels that must stay up for failover to still work.
    ///
    /// `quorum` alone is not enough. Agreeing the master is down takes `quorum`
    /// sentinels, but *electing the failover leader* independently requires a
    /// majority of the known sentinel set — Sentinel enforces that regardless of
    /// how low `quorum` is set, precisely to stop a minority from promoting a
    /// master during a partition. Taking the larger of the two is what actually
    /// keeps failover possible. For the default 3-sentinel deployment both terms
    /// are 2, so this equals `effective_quorum()`.
    pub fn min_available(&self) -> i32 {
        self.effective_quorum().max(self.replicas / 2 + 1)
    }

    /// Sentinels that may be concurrently unavailable. Zero below 3 replicas,
    /// where there is no failure tolerance to protect in the first place.
    pub fn safe_max_unavailable(&self) -> i32 {
        (self.replicas - self.min_available()).max(0)
    }
}

impl RedisSpec {
    /// Data pods that may be concurrently unavailable while at least one copy
    /// of the data is still served.
    ///
    /// Losing the *master* is only survivable when Sentinel is enabled. Without
    /// it, `build_redis_args` hard-pins ordinal 0 as the master and nothing
    /// promotes a replica, so the survivors are read-only until pod-0 returns.
    /// That is a write-availability concern rather than a data-availability one,
    /// so it is reported in the Event rather than tightening this bound.
    pub fn safe_max_unavailable(&self) -> i32 {
        (self.replicas - 1).max(0)
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    #[serde(default = "default_storage_size")]
    pub size: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionSpec>,
}

fn default_storage_size() -> String {
    "1Gi".to_string()
}

/// How the dataset is written to disk so it survives a restart.
///
/// Omitting this block means the same as `{}`: both RDB and AOF enabled, AOF
/// fsyncing every second. The pair is the default because they recover
/// different things. AOF bounds data loss to roughly one second of writes,
/// while RDB gives a compact snapshot that loads faster and — the part that
/// matters on a rolling update — carries the replication ID and offset in its
/// aux fields, letting a restarted replica ask its master for a *partial*
/// resync instead of forcing a fork-and-ship of the entire dataset.
///
/// Applies uniformly to every pod in the workload, and deliberately so. Roles
/// are not fixed here: under Sentinel or Redis Cluster any pod may be promoted
/// to master, and a StatefulSet has a single pod template, so there is no
/// coherent way to configure replicas separately. It is also the safer
/// arrangement — a persistence-less node that gets promoted and then restarts
/// comes back empty and replicates that empty dataset onto its own replicas.
///
/// Files are written to `/data`, which is the PVC when `storage` is set and an
/// emptyDir otherwise. Without `storage`, persistence therefore survives a
/// container restart (an OOM kill, a failed liveness probe) but not a
/// reschedule onto another node.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PersistenceSpec {
    /// Master switch. When false, both RDB and AOF are disabled regardless of
    /// the `rdb` and `aof` blocks below and Redis runs as a pure in-memory
    /// cache, losing its dataset on every restart.
    #[serde(default = "default_true")]
    pub enabled: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rdb: Option<RdbSpec>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aof: Option<AofSpec>,
}

impl Default for PersistenceSpec {
    /// Matches what serde produces for `persistence: {}`, so
    /// `PersistenceSpec::default()` can stand in for an absent block.
    fn default() -> Self {
        Self {
            enabled: true,
            rdb: None,
            aof: None,
        }
    }
}

/// Point-in-time snapshotting (`save` / `BGSAVE`).
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RdbSpec {
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Snapshot triggers, each `"<seconds> <changes>"` — snapshot when at
    /// least `changes` keys changed within `seconds`. Defaults to Redis's own
    /// `3600 1`, `300 100`, `60 10000`.
    ///
    /// An empty list schedules no automatic snapshots, which is the same thing
    /// `enabled: false` does: both emit `--save ""`. Neither stops Redis from
    /// producing an RDB for a replication full sync or an explicit `BGSAVE` —
    /// there is no Redis setting that does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("items" = json!({"type": "string", "pattern": r"^\d+ \d+$"})))]
    pub save: Option<Vec<String>>,
}

impl Default for RdbSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            save: None,
        }
    }
}

/// Append-only file logging.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AofSpec {
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// How often the append-only file is fsynced. Defaults to `everysec`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fsync: Option<FsyncPolicy>,
}

impl Default for AofSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            fsync: None,
        }
    }
}

/// Redis's `appendfsync` policy. Modelled as an enum so the API server rejects
/// a bad value at `kubectl apply` time — redis-server refuses to start on an
/// unrecognised one, which would otherwise surface as a CrashLoopBackOff.
///
/// The third variant is spelled `never` rather than Redis's own `no`. `no` is
/// unusable as a schema enum value here: serde_yaml emits it bare (YAML 1.2,
/// where it is a string), but the API server parses CRDs with go-yaml's YAML
/// 1.1, where bare `no` is the boolean `false` — which would land a boolean
/// inside a `type: string` enum and make the value unmatchable. Spelling it
/// `never` sidesteps that, and spares users the same quoting trap in their own
/// manifests. It maps to `appendfsync no` on the redis-server command line.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum FsyncPolicy {
    /// fsync on every write. Safest and slowest.
    Always,
    /// fsync once a second, bounding loss to about a second of writes.
    #[default]
    Everysec,
    /// Never fsync explicitly; leave it to the kernel, which typically flushes
    /// every 30 seconds. Fastest, and the weakest durability guarantee.
    Never,
}

impl FsyncPolicy {
    /// The literal `appendfsync` value redis-server expects.
    pub fn as_redis_value(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::Everysec => "everysec",
            Self::Never => "no",
        }
    }
}

/// Redis's `maxmemory-policy`: what to do with the keyspace once `maxmemory`
/// is reached. Modelled as an enum for the same reason `FsyncPolicy` is — the
/// API server rejects a bad value at `kubectl apply` time, where redis-server
/// would refuse to start and surface it as a CrashLoopBackOff instead.
///
/// The `allkeys-*` variants may evict any key; the `volatile-*` variants only
/// consider keys carrying a TTL, and behave like `noeviction` when none of the
/// keys in memory have one — a cache built without TTLs gets write errors under
/// a `volatile-*` policy, not evictions.
///
/// Two things to weigh before moving off the default:
///
/// - Eviction deletes committed data. An evicted key is dropped from the RDB
///   and AOF along with everything else, so anything but `noeviction` turns the
///   deployment into a cache whether or not persistence is on.
/// - The ceiling is per pod. `maxmemory` is one node's limit, so under Redis
///   Cluster the fullest shard starts evicting while the rest may be near
///   empty, and the policy applies to every pod because a StatefulSet has a
///   single pod template. Replicas don't evict independently — since Redis 5
///   they wait for the primary's `DEL` — so it only acts on the current master.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum EvictionPolicy {
    /// Evict nothing; reject writes that would exceed `maxmemory` with an OOM
    /// error. Redis's own default, and the right one for a datastore — see the
    /// note on `RedisSpec::eviction_policy` about replicas.
    #[default]
    #[serde(rename = "noeviction")]
    NoEviction,
    /// Approximated LRU over every key.
    AllkeysLru,
    /// Approximated LFU over every key. Usually the best cache policy: it
    /// tracks access frequency, so a one-off scan of cold keys doesn't flush
    /// the working set the way LRU allows.
    AllkeysLfu,
    /// Uniformly random over every key.
    AllkeysRandom,
    /// Approximated LRU, restricted to keys with a TTL.
    VolatileLru,
    /// Approximated LFU, restricted to keys with a TTL.
    VolatileLfu,
    /// Uniformly random, restricted to keys with a TTL.
    VolatileRandom,
    /// Shortest remaining TTL first.
    VolatileTtl,
}

impl EvictionPolicy {
    /// The literal `maxmemory-policy` value redis-server expects. Identical to
    /// the serialised form, but spelled out rather than round-tripped through
    /// serde so the command line can't drift if the schema naming changes.
    pub fn as_redis_value(self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllkeysLru => "allkeys-lru",
            Self::AllkeysLfu => "allkeys-lfu",
            Self::AllkeysRandom => "allkeys-random",
            Self::VolatileLru => "volatile-lru",
            Self::VolatileLfu => "volatile-lfu",
            Self::VolatileRandom => "volatile-random",
            Self::VolatileTtl => "volatile-ttl",
        }
    }
}

fn default_true() -> bool {
    true
}

/// PVC retention policy. Mirrors StatefulSet's
/// `persistentVolumeClaimRetentionPolicy`. Both fields default to `Retain`
/// when unset (k8s default), preserving data on scale-down or deletion.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionSpec {
    /// "Retain" or "Delete". Applies when the StatefulSet is scaled down.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when_scaled: Option<String>,
    /// "Retain" or "Delete". Applies when the StatefulSet is deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when_deleted: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RedisStatus {
    #[serde(default)]
    pub ready_replicas: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// FQDN of the current master. Tracks Sentinel-driven failovers when
    /// Sentinel is enabled; otherwise always pod-0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_host: Option<String>,
    /// Pod name (short form) of the current master. Easier to read than the
    /// full FQDN and matches what `kubectl get pods` shows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub master_pod: Option<String>,
    /// `connected_slaves` reported by `INFO replication` on the primary.
    /// Zero when `replicas == 1` (no replicas configured) or before the
    /// primary becomes ready.
    #[serde(default)]
    pub connected_replicas: i32,
    /// Number of Sentinel pods that report ready. Zero when Sentinel is
    /// disabled.
    #[serde(default)]
    pub sentinel_replicas_ready: i32,
}

fn default_replicas() -> i32 {
    1
}

fn default_sentinel_replicas() -> i32 {
    3
}

fn default_image() -> String {
    "redis:8".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_EVICTION_POLICIES: [EvictionPolicy; 8] = [
        EvictionPolicy::NoEviction,
        EvictionPolicy::AllkeysLru,
        EvictionPolicy::AllkeysLfu,
        EvictionPolicy::AllkeysRandom,
        EvictionPolicy::VolatileLru,
        EvictionPolicy::VolatileLfu,
        EvictionPolicy::VolatileRandom,
        EvictionPolicy::VolatileTtl,
    ];

    #[test]
    fn eviction_policy_defaults_to_noeviction() {
        assert_eq!(EvictionPolicy::default(), EvictionPolicy::NoEviction);
        assert_eq!(EvictionPolicy::default().as_redis_value(), "noeviction");
    }

    #[test]
    fn eviction_policy_covers_every_redis_policy() {
        // The set redis-server accepts for `maxmemory-policy`. A variant added
        // here without a matching entry — or vice versa — fails this.
        let mut emitted: Vec<&str> = ALL_EVICTION_POLICIES
            .iter()
            .map(|p| p.as_redis_value())
            .collect();
        emitted.sort_unstable();
        assert_eq!(
            emitted,
            [
                "allkeys-lfu",
                "allkeys-lru",
                "allkeys-random",
                "noeviction",
                "volatile-lfu",
                "volatile-lru",
                "volatile-random",
                "volatile-ttl",
            ]
        );
    }

    #[test]
    fn eviction_policy_command_line_value_matches_the_accepted_api_value() {
        // `as_redis_value` is hand-written; this is what keeps it from drifting
        // away from what the CRD schema actually lets users apply.
        for p in ALL_EVICTION_POLICIES {
            let serialized = serde_json::to_string(&p).expect("policy should serialize");
            assert_eq!(serialized, format!("\"{}\"", p.as_redis_value()));
        }
    }

    #[test]
    fn eviction_policy_deserializes_from_the_redis_spelling() {
        for p in ALL_EVICTION_POLICIES {
            let json = format!("\"{}\"", p.as_redis_value());
            let parsed: EvictionPolicy =
                serde_json::from_str(&json).expect("policy should deserialize");
            assert_eq!(parsed, p);
        }
    }

    #[test]
    fn eviction_policy_rejects_an_unknown_value() {
        assert!(serde_json::from_str::<EvictionPolicy>("\"allkeys-ttl\"").is_err());
        assert!(serde_json::from_str::<EvictionPolicy>("\"no-eviction\"").is_err());
    }

    fn sentinel(replicas: i32, quorum: Option<i32>) -> SentinelSpec {
        SentinelSpec {
            replicas,
            quorum,
            ..Default::default()
        }
    }

    #[test]
    fn sentinel_master_name_defaults_to_mymaster() {
        assert_eq!(sentinel(3, None).effective_master_name(), "mymaster");
    }

    #[test]
    fn sentinel_master_name_honors_an_explicit_value() {
        let spec = SentinelSpec {
            master_name: Some("cache-primary".into()),
            ..sentinel(3, None)
        };
        assert_eq!(spec.effective_master_name(), "cache-primary");
    }

    #[test]
    fn sentinel_min_available_defaults_to_quorum() {
        assert_eq!(sentinel(3, None).min_available(), 2);
        assert_eq!(sentinel(5, None).min_available(), 3);
    }

    #[test]
    fn sentinel_min_available_raises_low_explicit_quorum_to_majority() {
        // quorum 1 would permit eviction down to a set that provably cannot
        // elect a failover leader.
        assert_eq!(sentinel(5, Some(1)).min_available(), 3);
    }

    #[test]
    fn sentinel_min_available_honors_high_explicit_quorum() {
        assert_eq!(sentinel(5, Some(4)).min_available(), 4);
    }

    #[test]
    fn sentinel_safe_bound_allows_one_down_at_three_replicas() {
        assert_eq!(sentinel(3, None).safe_max_unavailable(), 1);
        assert_eq!(sentinel(5, None).safe_max_unavailable(), 2);
    }

    #[test]
    fn sentinel_safe_bound_is_zero_below_three_replicas() {
        assert_eq!(sentinel(2, None).safe_max_unavailable(), 0);
        assert_eq!(sentinel(1, None).safe_max_unavailable(), 0);
    }

    #[test]
    fn redis_safe_bound_leaves_one_survivor() {
        let s = RedisSpec {
            replicas: 3,
            ..Default::default()
        };
        assert_eq!(s.safe_max_unavailable(), 2);
    }

    #[test]
    fn redis_safe_bound_is_zero_for_single_replica() {
        let s = RedisSpec {
            replicas: 1,
            ..Default::default()
        };
        assert_eq!(s.safe_max_unavailable(), 0);
    }
}
