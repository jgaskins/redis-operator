use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ResourceRequirements;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::crd::redis::ResourcesSpec;
use crate::error::Result;

pub mod redis;
pub mod redis_cluster;

pub const FIELD_MANAGER: &str = "redis-operator";

#[derive(Clone)]
pub struct Context {
    pub client: Client,
}

impl Context {
    pub fn new(client: Client) -> Self {
        Self { client }
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
