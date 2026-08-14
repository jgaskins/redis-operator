use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::redis::{ResourcesSpec, StorageSpec};

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

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_disruption_budget: Option<PodDisruptionBudgetSpec>,
}

/// Mirrors `policy/v1` `PodDisruptionBudgetSpec` — the operator builds and
/// owns the PDB, selecting on the cluster's pods. Exactly one of
/// `minAvailable` / `maxUnavailable` should be set; if neither is, the PDB
/// is left absent.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PodDisruptionBudgetSpec {
    /// Integer or percentage (e.g. `1` or `"50%"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_available: Option<IntOrString>,

    /// Integer or percentage. Mutually exclusive with `minAvailable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_unavailable: Option<IntOrString>,

    /// `IfHealthyBudget` (default) or `AlwaysAllow`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unhealthy_pod_eviction_policy: Option<String>,
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

fn default_masters() -> i32 {
    3
}

fn default_replicas_per_master() -> i32 {
    1
}

fn default_image() -> String {
    "redis:8".to_string()
}
