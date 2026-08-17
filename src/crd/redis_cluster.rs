use k8s_openapi::api::core::v1::TopologySpreadConstraint;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::redis::{PersistenceSpec, PodDisruptionBudgetSpec, ResourcesSpec, StorageSpec};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[kube(
    group = "redis.jgaskins.dev",
    version = "v1alpha1",
    kind = "RedisCluster",
    plural = "redisclusters",
    singular = "rediscluster",
    shortname = "rdsc",
    namespaced,
    status = "RedisClusterStatus",
    derive = "PartialEq",
    derive = "Default",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Masters","type":"integer","jsonPath":".status.masters"}"#,
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".status.replicas"}"#,
    printcolumn = r#"{"name":"Slots","type":"integer","jsonPath":".status.slotsAssigned","description":"Slots assigned (16384 when fully populated)"}"#,
    printcolumn = r#"{"name":"Imbalance","type":"integer","jsonPath":".status.replicaImbalance","description":"Replica imbalance (0 when fully balanced)"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RedisClusterSpec {
    #[serde(default = "default_masters")]
    pub masters: i32,

    #[serde(default = "default_replicas_per_master")]
    pub replicas_per_master: i32,

    #[serde(default = "default_image")]
    pub image: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageSpec>,

    /// How the dataset is written to disk. Defaults to both RDB and AOF on,
    /// with AOF fsyncing once a second. See `PersistenceSpec`.
    ///
    /// `/data` is mounted regardless of this setting, because the cluster's
    /// `nodes.conf` — the file holding each pod's cluster identity and slot
    /// map — lives there whether or not the dataset is persisted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<PersistenceSpec>,

    /// Container resource requests/limits for each cluster pod. Defaults:
    /// 1Gi memory (requests and limits), 1.2 CPU requests, 2 CPU limits.
    /// User values are merged per-key with the defaults — overriding `cpu`
    /// alone preserves the default memory settings, and vice versa.
    ///
    /// `maxmemory` is derived from the effective memory limit (70%) and
    /// passed to redis-server, so users don't need to keep it in sync
    /// manually.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesSpec>,

    /// PodDisruptionBudget for the cluster's pods. Opt-in, and validated against
    /// the topology: the safe maximum is the smaller of the master-majority
    /// slack `(masters - 1) / 2` and `replicasPerMaster`. For the default
    /// 3-by-1 cluster that is 1. An unsafe budget is refused rather than
    /// applied — see `PodDisruptionBudgetSpec`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_disruption_budget: Option<PodDisruptionBudgetSpec>,

    /// Overrides the operator's default pod spread.
    ///
    /// By default the cluster's pods get a single soft constraint — `maxSkew: 1`
    /// over `kubernetes.io/hostname` with `whenUnsatisfiable: ScheduleAnyway` —
    /// so the scheduler prefers to put every pod on a different node but still
    /// schedules on clusters with fewer nodes than pods. This matters more here
    /// than a PodDisruptionBudget does: a budget only rate-limits *voluntary*
    /// eviction, so without spreading, losing one node can still take the whole
    /// cluster down. Set to `[]` to emit no constraints at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology_spread_constraints: Option<Vec<TopologySpreadConstraint>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RedisClusterStatus {
    /// `cluster_known_nodes` from Redis — total nodes the cluster reports.
    #[serde(default)]
    pub ready_nodes: i32,

    /// High-level rollup. `Running` only when cluster_state=ok, all 16384 slots
    /// are assigned, and replicas are perfectly distributed; otherwise `Degraded`
    /// or `Pending`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,

    /// Raw `cluster_state` from Redis (`ok`, `fail`, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_state: Option<String>,

    /// Number of masters currently in the cluster.
    #[serde(default)]
    pub masters: i32,

    /// Number of replicas currently in the cluster.
    #[serde(default)]
    pub replicas: i32,

    /// Sum of `|observed_replicas - replicasPerMaster|` across all masters.
    /// Zero when every master has exactly the desired number of replicas.
    #[serde(default)]
    pub replica_imbalance: i32,

    /// Sum of slots owned across all masters. 16384 when fully assigned.
    #[serde(default)]
    pub slots_assigned: i32,

    /// Per-node breakdown, sorted by hostname for stable diffs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<NodeStatus>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    /// Redis Cluster node ID (40-char hex).
    pub id: String,
    /// Announced hostname (typically `<sts>-<ord>.<sts>.<ns>.svc.cluster.local`).
    pub hostname: String,
    /// `master` or `replica`.
    pub role: String,
    /// For replicas, the ID of the master they replicate. Empty for masters.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub master_id: String,
    /// For masters, number of slots owned. Always 0 for replicas.
    #[serde(default)]
    pub slot_count: i32,
    /// For masters, number of replicas attached. Always 0 for replicas.
    #[serde(default)]
    pub replica_count: i32,
}

impl RedisClusterSpec {
    /// Pods the StatefulSet runs: one per master, plus each master's replicas.
    pub fn total_pods(&self) -> i32 {
        total_pods(self.masters, self.replicas_per_master)
    }

    /// Pods that may be concurrently unavailable while the cluster keeps
    /// serving all 16384 slots.
    ///
    /// The PDB selector matches every pod in the StatefulSet indiscriminately —
    /// shard membership is decided at runtime by `redis-cli --cluster` and
    /// appears in no pod label — so this bound has to hold even if every
    /// eviction lands in the worst possible place. Two limits apply at once:
    ///
    /// - **Shard survival.** A shard is `1 + replicas_per_master` pods. If all
    ///   of one shard is down its slots are unserved and `cluster_state` goes
    ///   to `fail`, so at most `replicas_per_master` may go.
    /// - **Master majority.** Redis Cluster needs a majority of masters to mark
    ///   a node failed and authorise a promotion; a node that can't reach one
    ///   stops serving. That allows `(masters - 1) / 2` down.
    ///
    /// Whichever is tighter wins. The majority bound is deliberately pessimistic
    /// when replicas exist — an evicted master is failed over and the count
    /// recovers — but the promotion itself needs the surviving majority to vote,
    /// so the simultaneous bound is the correct one for a PDB.
    ///
    /// Returns 0 when the topology can't survive any voluntary disruption at
    /// all (no replicas, or fewer than 3 masters).
    pub fn safe_max_unavailable(&self) -> i32 {
        let master_majority_slack = (self.masters - 1) / 2;
        master_majority_slack.min(self.replicas_per_master).max(0)
    }
}

/// Pod count for a cluster of `masters` shards each carrying
/// `replicas_per_master` replicas. A free function as well as a method because
/// several call sites hold the two counts directly rather than the whole spec,
/// and this formula should have exactly one definition.
pub fn total_pods(masters: i32, replicas_per_master: i32) -> i32 {
    masters * (1 + replicas_per_master)
}

fn default_masters() -> i32 {
    3
}

fn default_replicas_per_master() -> i32 {
    1
}

fn default_image() -> String {
    "redis:8".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(masters: i32, replicas_per_master: i32) -> RedisClusterSpec {
        RedisClusterSpec {
            masters,
            replicas_per_master,
            ..Default::default()
        }
    }

    #[test]
    fn total_pods_counts_masters_and_replicas() {
        assert_eq!(spec(3, 1).total_pods(), 6);
        assert_eq!(spec(3, 0).total_pods(), 3);
        assert_eq!(spec(5, 2).total_pods(), 15);
    }

    #[test]
    fn cluster_safe_bound_is_one_for_three_by_one() {
        assert_eq!(spec(3, 1).safe_max_unavailable(), 1);
    }

    #[test]
    fn cluster_safe_bound_is_zero_without_replicas() {
        // No replica means evicting any master takes its slots offline.
        assert_eq!(spec(3, 0).safe_max_unavailable(), 0);
    }

    #[test]
    fn cluster_safe_bound_is_zero_below_three_masters() {
        // 2 masters cannot form a majority after losing one.
        assert_eq!(spec(2, 1).safe_max_unavailable(), 0);
        assert_eq!(spec(1, 1).safe_max_unavailable(), 0);
    }

    #[test]
    fn cluster_safe_bound_capped_by_master_majority() {
        // 6 masters tolerate 2 down; the 3 replicas per shard are not the limit.
        assert_eq!(spec(6, 3).safe_max_unavailable(), 2);
    }

    #[test]
    fn cluster_safe_bound_capped_by_shard_replicas() {
        // 9 masters would tolerate 4 down, but 1 replica per shard caps it at 1.
        assert_eq!(spec(9, 1).safe_max_unavailable(), 1);
    }

    #[test]
    fn cluster_safe_bound_scales_with_both() {
        assert_eq!(spec(5, 2).safe_max_unavailable(), 2);
    }
}
