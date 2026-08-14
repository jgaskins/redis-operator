use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::TopologySpreadConstraint;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

impl SentinelSpec {
    /// Effective quorum: explicit value if set, otherwise majority of
    /// `replicas` (e.g. 2 of 3).
    pub fn effective_quorum(&self) -> i32 {
        self.quorum.unwrap_or(self.replicas / 2 + 1)
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

    fn sentinel(replicas: i32, quorum: Option<i32>) -> SentinelSpec {
        SentinelSpec {
            replicas,
            quorum,
            ..Default::default()
        }
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
