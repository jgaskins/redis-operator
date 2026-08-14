use std::collections::BTreeMap;

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
}

/// Mirrors the shape of `corev1.ResourceRequirements`. We can't use the
/// upstream type directly because k8s_openapi types don't implement
/// `JsonSchema`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<BTreeMap<String, String>>,
}

impl SentinelSpec {
    /// Effective quorum: explicit value if set, otherwise majority of
    /// `replicas` (e.g. 2 of 3).
    pub fn effective_quorum(&self) -> i32 {
        self.quorum.unwrap_or(self.replicas / 2 + 1)
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
