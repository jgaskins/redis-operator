use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::{
    StatefulSet, StatefulSetPersistentVolumeClaimRetentionPolicy, StatefulSetSpec,
};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EmptyDirVolumeSource, EnvVar, ExecAction, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, Pod, PodSpec, PodTemplateSpec, Probe, ResourceRequirements,
    Service, ServicePort, ServiceSpec, Volume, VolumeMount, VolumeResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{AttachParams, DeleteParams, ListParams, ObjectMeta, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{Controller, watcher};
use kube::{Api, Client, Resource, ResourceExt};
use serde_json::json;
use tokio::io::AsyncReadExt;
use tracing::{info, warn};

use crate::controller::{Context, FIELD_MANAGER, apply, effective_redis_resources, maxmemory_bytes};
use crate::crd::Redis;
use crate::crd::redis::{ResourcesSpec, SentinelSpec};
use crate::error::{Error, Result};

const REDIS_PORT: i32 = 6379;
const SENTINEL_PORT: i32 = 26379;
const DATA_VOLUME: &str = "data";
/// Label automatically set by the StatefulSet controller on each pod.
/// Used to select the current master pod in the primary Service.
const POD_NAME_LABEL: &str = "statefulset.kubernetes.io/pod-name";
/// Sentinel master-name. Hard-coded for now — could become a spec field if
/// users start running multiple Redis instances behind a shared sentinel set.
const MASTER_NAME: &str = "mymaster";
/// Operator-managed label distinguishing master from replica pods. Drives the
/// replicas Service selector; updated each reconcile to track failovers.
const ROLE_LABEL: &str = "redis-operator/role";

pub async fn run(ctx: Arc<Context>) -> anyhow::Result<()> {
    let redis_api: Api<Redis> = Api::all(ctx.client.clone());
    redis_api
        .list(&Default::default())
        .await
        .map_err(|e| anyhow::anyhow!("Redis CRD not installed or inaccessible: {e}"))?;

    let sts_api: Api<StatefulSet> = Api::all(ctx.client.clone());
    let svc_api: Api<Service> = Api::all(ctx.client.clone());

    info!("starting Redis controller");

    Controller::new(redis_api, watcher::Config::default())
        .owns(sts_api, watcher::Config::default())
        .owns(svc_api, watcher::Config::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => info!(name = %obj.name, ns = ?obj.namespace, "reconciled"),
                Err(err) => warn!(?err, "reconcile failed"),
            }
        })
        .await;

    Ok(())
}

async fn reconcile(redis: Arc<Redis>, ctx: Arc<Context>) -> Result<Action> {
    let name = redis.name_any();
    let ns = redis
        .namespace()
        .ok_or(Error::MissingMetadata("namespace"))?;
    info!(%name, %ns, "reconciling Redis");

    let owner = redis
        .controller_owner_ref(&())
        .ok_or(Error::MissingMetadata("uid"))?;

    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &ns);
    let redis_api: Api<Redis> = Api::namespaced(ctx.client.clone(), &ns);

    apply(&svc_api, &name, &build_headless_service(&redis, &ns, owner.clone())).await?;
    apply(&sts_api, &name, &build_redis_statefulset(&redis, &ns, owner.clone())).await?;
    apply(
        &svc_api,
        &replicas_service_name(&name),
        &build_replicas_service(&redis, &ns, owner.clone()),
    )
    .await?;

    let sentinel_name = sentinel_sts_name(&name);
    if let Some(sentinel_spec) = redis.spec.sentinel.as_ref() {
        apply(
            &svc_api,
            &sentinel_name,
            &build_sentinel_headless_service(&redis, &ns, owner.clone()),
        )
        .await?;
        apply(
            &sts_api,
            &sentinel_name,
            &build_sentinel_statefulset(&redis, &ns, sentinel_spec, owner.clone()),
        )
        .await?;
    } else {
        delete_if_exists(svc_api.delete(&sentinel_name, &DeleteParams::default()).await)?;
        delete_if_exists(sts_api.delete(&sentinel_name, &DeleteParams::default()).await)?;
    }

    let master_pod =
        determine_master_pod(&ctx.client, &svc_api, &redis, &ns).await?;

    apply(
        &svc_api,
        &primary_service_name(&name),
        &build_primary_service(&redis, &ns, owner, &master_pod),
    )
    .await?;

    label_pods_by_role(&ctx.client, &ns, &name, &master_pod).await?;

    let ready = sts_api
        .get_opt(&name)
        .await?
        .and_then(|s| s.status)
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);
    let phase = if ready == redis.spec.replicas && redis.spec.replicas > 0 {
        "Running"
    } else {
        "Pending"
    };

    let primary_host = (redis.spec.replicas > 0).then(|| pod_fqdn(&master_pod, &name, &ns));
    let connected = if redis.spec.replicas > 1 && ready >= 1 {
        match query_connected_replicas(&ctx.client, &ns, &master_pod).await {
            Ok(n) => n,
            Err(e) => {
                warn!(?e, %master_pod, "failed to query INFO replication");
                0
            }
        }
    } else {
        0
    };
    let sentinel_ready = if redis.spec.sentinel.is_some() {
        sts_api
            .get_opt(&sentinel_name)
            .await?
            .and_then(|s| s.status)
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0)
    } else {
        0
    };

    let status_patch = json!({
        "status": {
            "readyReplicas": ready,
            "phase": phase,
            "primaryHost": primary_host,
            "masterPod": master_pod,
            "connectedReplicas": connected,
            "sentinelReplicasReady": sentinel_ready,
        }
    });
    redis_api
        .patch_status(&name, &PatchParams::default(), &Patch::Merge(&status_patch))
        .await?;

    Ok(Action::requeue(Duration::from_secs(30)))
}

fn error_policy(_obj: Arc<Redis>, err: &Error, _ctx: Arc<Context>) -> Action {
    warn!(?err, "Redis reconcile error");
    Action::requeue(Duration::from_secs(15))
}

/// Swallow 404s on delete — we're tearing down resources we previously owned,
/// and "already gone" is the desired terminal state.
fn delete_if_exists<T>(res: std::result::Result<T, kube::Error>) -> Result<()> {
    match res {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn labels(name: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert("app.kubernetes.io/name".into(), "redis".into());
    m.insert("app.kubernetes.io/instance".into(), name.to_string());
    m.insert("app.kubernetes.io/managed-by".into(), FIELD_MANAGER.into());
    m
}

fn sentinel_labels(name: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert("app.kubernetes.io/name".into(), "redis-sentinel".into());
    m.insert("app.kubernetes.io/instance".into(), name.to_string());
    m.insert("app.kubernetes.io/component".into(), "sentinel".into());
    m.insert("app.kubernetes.io/managed-by".into(), FIELD_MANAGER.into());
    m
}

fn primary_service_name(name: &str) -> String {
    format!("{name}-primary")
}

fn replicas_service_name(name: &str) -> String {
    format!("{name}-replicas")
}

fn sentinel_sts_name(name: &str) -> String {
    format!("{name}-sentinel")
}

fn pod_fqdn(pod: &str, svc: &str, ns: &str) -> String {
    format!("{pod}.{svc}.{ns}.svc.cluster.local")
}

fn build_resource_requirements(spec: &ResourcesSpec) -> ResourceRequirements {
    let to_quantities = |m: &BTreeMap<String, String>| {
        m.iter()
            .map(|(k, v)| (k.clone(), Quantity(v.clone())))
            .collect::<BTreeMap<_, _>>()
    };
    ResourceRequirements {
        requests: spec.requests.as_ref().map(to_quantities),
        limits: spec.limits.as_ref().map(to_quantities),
        ..Default::default()
    }
}

/// Defaults applied when `sentinel.resources` is omitted. Sets just enough
/// to lift the pod out of BestEffort QoS so the kubelet doesn't throttle it
/// into TILT mode under node pressure. No CPU limit on purpose — CPU
/// throttling from a limit itself trips TILT. Users can override by setting
/// `resources` explicitly, or opt out entirely with `resources: {}`.
fn default_sentinel_resources() -> ResourceRequirements {
    let q = |s: &str| Quantity(s.to_string());
    ResourceRequirements {
        requests: Some(BTreeMap::from([
            ("cpu".into(), q("50m")),
            ("memory".into(), q("64Mi")),
        ])),
        limits: Some(BTreeMap::from([("memory".into(), q("128Mi"))])),
        ..Default::default()
    }
}

/// FQDN of pod-0 — the bootstrap primary, before any failover. Used as the
/// initial `--replicaof` target and the initial `sentinel monitor` target.
fn primary_hostname(name: &str, ns: &str) -> String {
    pod_fqdn(&format!("{name}-0"), name, ns)
}

fn build_headless_service(redis: &Redis, ns: &str, owner: OwnerReference) -> Service {
    let name = redis.name_any();
    let l = labels(&name);
    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(ns.to_string()),
            labels: Some(l.clone()),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            selector: Some(l),
            ports: Some(vec![ServicePort {
                name: Some("redis".to_string()),
                port: REDIS_PORT,
                target_port: Some(IntOrString::Int(REDIS_PORT)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            publish_not_ready_addresses: Some(true),
            ..Default::default()
        }),
        status: None,
    }
}

/// ClusterIP service for the current master. Selects via the auto-set
/// `pod-name` label, which the operator sets to track Sentinel-driven
/// failovers — so the endpoint shifts to the new master without needing
/// Sentinel-aware clients.
fn build_primary_service(
    redis: &Redis,
    ns: &str,
    owner: OwnerReference,
    master_pod: &str,
) -> Service {
    let name = redis.name_any();
    let l = labels(&name);
    let mut selector = l.clone();
    selector.insert(POD_NAME_LABEL.into(), master_pod.to_string());
    Service {
        metadata: ObjectMeta {
            name: Some(primary_service_name(&name)),
            namespace: Some(ns.to_string()),
            labels: Some(l),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(selector),
            ports: Some(vec![ServicePort {
                name: Some("redis".to_string()),
                port: REDIS_PORT,
                target_port: Some(IntOrString::Int(REDIS_PORT)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

/// ClusterIP service for replica pods only. Service selectors only support
/// equality, so the operator labels each pod with `redis-operator/role` and
/// this Service selects on `role=replica`. The replica set tracks failovers
/// because `label_pods_by_role` re-runs each reconcile against the current
/// master.
fn build_replicas_service(redis: &Redis, ns: &str, owner: OwnerReference) -> Service {
    let name = redis.name_any();
    let l = labels(&name);
    let mut selector = l.clone();
    selector.insert(ROLE_LABEL.into(), "replica".into());
    Service {
        metadata: ObjectMeta {
            name: Some(replicas_service_name(&name)),
            namespace: Some(ns.to_string()),
            labels: Some(l),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(selector),
            ports: Some(vec![ServicePort {
                name: Some("redis".to_string()),
                port: REDIS_PORT,
                target_port: Some(IntOrString::Int(REDIS_PORT)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

/// Entrypoint for Redis pods.
///
/// **Sentinel-disabled**: simple ordinal-based bootstrap — pod-0 runs as
/// primary, others use `--replicaof` pointing at pod-0.
///
/// **Sentinel-enabled**: writes a minimal `redis.conf` (just the `replicaof`
/// directive) on first boot only, then execs `redis-server <conf>` with
/// every other setting passed via command-line args. The `replicaof`
/// directive is what Sentinel rewrites via `CONFIG REWRITE` after a
/// failover; persisting just that one directive in the conf file means a
/// pod's last-known role survives across restarts.
///
/// First-boot determination: ask Sentinel via the headless service; if
/// Sentinel returns a master that's not us, start as a replica of it. If
/// Sentinel is unreachable (initial bootstrap, no quorum yet), fall back to
/// ordinal-based: pod-0 is master, others replicate from pod-0.
///
/// On subsequent boots the conf is reused as-is — including any
/// `replicaof` Sentinel rewrote post-failover. Without a Redis PVC the conf
/// is ephemeral and behaves like every-boot sentinel-aware bootstrap; with
/// a PVC, the rewritten state genuinely persists.
/// The `redis:8` image's entrypoint drops privileges to the `redis` user
/// after attempting to chown the data dir — but it skips the chown when it
/// sees files it doesn't recognize (our `redis.conf`, `nodes.conf`), so the
/// dropped-privilege process can't write to root-owned files left by
/// previous runs. Skipping the privilege drop keeps redis running as the
/// container's default user (root), matching pre-entrypoint behavior and
/// preserving access to existing PVC contents.
fn entrypoint_skip_drop_privs() -> EnvVar {
    EnvVar {
        name: "SKIP_DROP_PRIVS".to_string(),
        value: Some("1".to_string()),
        value_from: None,
    }
}

fn build_redis_args(redis: &Redis, ns: &str) -> Vec<String> {
    let name = redis.name_any();
    let primary = primary_hostname(&name, ns);
    let resources = effective_redis_resources(redis.spec.resources.as_ref());
    let maxmem = maxmemory_bytes(&resources)
        .map(|b| format!(" --maxmemory {b}"))
        .unwrap_or_default();
    let extra = if redis.spec.storage.is_some() {
        format!(" --appendonly yes --dir /data{maxmem}")
    } else {
        maxmem
    };

    let script = if redis.spec.sentinel.is_some() {
        let sentinel_svc = sentinel_sts_name(&name);
        format!(
            r#"set -eu
CONF=/data/redis.conf
HOST=$(hostname).{name}.{ns}.svc.cluster.local
ORD=${{HOSTNAME##*-}}
if [ ! -f "$CONF" ]; then
  REPLICAOF=""
  if getent hosts "{sentinel_svc}" >/dev/null 2>&1; then
    MASTER_INFO=$(timeout 2 redis-cli -h "{sentinel_svc}" -p {SENTINEL_PORT} sentinel get-master-addr-by-name {MASTER_NAME} 2>/dev/null || true)
    MASTER_HOST=$(echo "$MASTER_INFO" | head -n 1 | tr -d '"')
    if [ -n "$MASTER_HOST" ] && [ "$MASTER_HOST" != "$HOST" ]; then
      REPLICAOF="replicaof $MASTER_HOST {REDIS_PORT}"
    fi
  fi
  if [ -z "$REPLICAOF" ] && [ "$ORD" != "0" ]; then
    REPLICAOF="replicaof {primary} {REDIS_PORT}"
  fi
  printf '%s\n' "$REPLICAOF" > "$CONF"
fi
# CONFIG REWRITE (invoked by Sentinel on failover) serializes the full
# effective config, including a `loadmodule` line for every loaded module.
# The redis:8 entrypoint already passes `--loadmodule` for bundled modules,
# so a persisted duplicate aborts the server on next start. Strip them.
sed -i '/^[[:space:]]*loadmodule[[:space:]]/d' "$CONF"
exec docker-entrypoint.sh redis-server "$CONF"{extra} --replica-announce-ip "$HOST" --replica-announce-port {REDIS_PORT}
"#
        )
    } else {
        format!(
            r#"ORD=${{HOSTNAME##*-}}
HOST=$(hostname).{name}.{ns}.svc.cluster.local
ANNOUNCE="--replica-announce-ip $HOST --replica-announce-port {REDIS_PORT}"
if [ "$ORD" = "0" ]; then
  exec docker-entrypoint.sh redis-server{extra} $ANNOUNCE
else
  exec docker-entrypoint.sh redis-server{extra} $ANNOUNCE --replicaof {primary} {REDIS_PORT}
fi
"#
        )
    };
    vec!["sh".into(), "-c".into(), script]
}

fn build_redis_statefulset(redis: &Redis, ns: &str, owner: OwnerReference) -> StatefulSet {
    let name = redis.name_any();
    let l = labels(&name);

    let (volume_mounts, volume_claim_templates) = match &redis.spec.storage {
        Some(s) => {
            let mounts = vec![VolumeMount {
                name: DATA_VOLUME.to_string(),
                mount_path: "/data".to_string(),
                ..Default::default()
            }];
            let mut requests = BTreeMap::new();
            requests.insert("storage".to_string(), Quantity(s.size.clone()));
            let pvcs = vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some(DATA_VOLUME.to_string()),
                    ..Default::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    resources: Some(VolumeResourceRequirements {
                        requests: Some(requests),
                        limits: None,
                    }),
                    storage_class_name: s.storage_class.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }];
            (Some(mounts), Some(pvcs))
        }
        None => (None, None),
    };

    let probe = Some(Probe {
        exec: Some(ExecAction {
            command: Some(vec!["redis-cli".into(), "ping".into()]),
        }),
        initial_delay_seconds: Some(5),
        period_seconds: Some(5),
        timeout_seconds: Some(3),
        ..Default::default()
    });

    let args = build_redis_args(redis, ns);

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(ns.to_string()),
            labels: Some(l.clone()),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(redis.spec.replicas),
            service_name: Some(name),
            selector: LabelSelector {
                match_labels: Some(l.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(l),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "redis".to_string(),
                        image: Some(redis.spec.image.clone()),
                        command: Some(vec![args[0].clone()]),
                        args: Some(args[1..].to_vec()),
                        env: Some(vec![entrypoint_skip_drop_privs()]),
                        ports: Some(vec![ContainerPort {
                            name: Some("redis".to_string()),
                            container_port: REDIS_PORT,
                            protocol: Some("TCP".to_string()),
                            ..Default::default()
                        }]),
                        liveness_probe: probe.clone(),
                        readiness_probe: probe,
                        resources: Some(effective_redis_resources(
                            redis.spec.resources.as_ref(),
                        )),
                        volume_mounts,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            volume_claim_templates,
            persistent_volume_claim_retention_policy: redis
                .spec
                .storage
                .as_ref()
                .and_then(|s| s.retention.as_ref())
                .map(|r| StatefulSetPersistentVolumeClaimRetentionPolicy {
                    when_deleted: r.when_deleted.clone(),
                    when_scaled: r.when_scaled.clone(),
                }),
            ..Default::default()
        }),
        status: None,
    }
}

fn build_sentinel_headless_service(redis: &Redis, ns: &str, owner: OwnerReference) -> Service {
    let name = redis.name_any();
    let sentinel_name = sentinel_sts_name(&name);
    let l = sentinel_labels(&name);
    Service {
        metadata: ObjectMeta {
            name: Some(sentinel_name),
            namespace: Some(ns.to_string()),
            labels: Some(l.clone()),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            selector: Some(l),
            ports: Some(vec![ServicePort {
                name: Some("sentinel".to_string()),
                port: SENTINEL_PORT,
                target_port: Some(IntOrString::Int(SENTINEL_PORT)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            publish_not_ready_addresses: Some(true),
            ..Default::default()
        }),
        status: None,
    }
}

/// Sentinel entrypoint. Writes `sentinel.conf` on first boot only, then
/// execs `redis-sentinel`. Sentinel rewrites this file as it observes the
/// cluster (e.g. recording known replicas, the current master after
/// failover); preserving it across restarts means a sentinel pod restart
/// doesn't snap back to monitoring pod-0, which would briefly contradict
/// the actual cluster state.
///
/// Without `sentinel.storage` the file lives on emptyDir and is regenerated
/// on each restart — sentinel rediscovers via INFO, but with a brief stale
/// view. With storage configured, the file lives on a PVC and sentinel
/// resumes its prior view immediately.
///
/// Spec changes that affect static directives (e.g. quorum) won't propagate
/// to a pod whose conf file already exists; either update via
/// `SENTINEL SET` at runtime or delete the PVC to force regeneration.
fn build_sentinel_args(redis: &Redis, ns: &str, sentinel: &SentinelSpec) -> Vec<String> {
    let name = redis.name_any();
    let sentinel_svc = sentinel_sts_name(&name);
    let primary = primary_hostname(&name, ns);
    let quorum = sentinel.effective_quorum();
    let script = format!(
        r#"set -eu
CONF=/data/sentinel.conf
HOST=$(hostname).{sentinel_svc}.{ns}.svc.cluster.local
if [ ! -f "$CONF" ]; then
  cat > "$CONF" <<EOF
port {SENTINEL_PORT}
sentinel announce-ip $HOST
sentinel announce-port {SENTINEL_PORT}
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel monitor {MASTER_NAME} {primary} {REDIS_PORT} {quorum}
sentinel down-after-milliseconds {MASTER_NAME} 5000
sentinel failover-timeout {MASTER_NAME} 10000
sentinel parallel-syncs {MASTER_NAME} 1
EOF
fi
# Heal duplicate known-replica/known-sentinel lines that upstream Sentinel
# occasionally persists (e.g. when a peer is observed under both hostname
# and IP forms). A duplicate (master, host, port) triple makes Sentinel
# refuse to parse its own rewritten config on next boot.
awk '
  /^sentinel known-replica / {{ k=$3" "$4" "$5; if (seen[k]++) next }}
  /^sentinel known-sentinel / {{ k=$3" "$4" "$5; if (seen[k]++) next }}
  {{ print }}
' "$CONF" > "$CONF.tmp" && mv "$CONF.tmp" "$CONF"
exec redis-sentinel "$CONF"
"#
    );
    vec!["sh".into(), "-c".into(), script]
}

fn build_sentinel_statefulset(
    redis: &Redis,
    ns: &str,
    sentinel: &SentinelSpec,
    owner: OwnerReference,
) -> StatefulSet {
    let name = redis.name_any();
    let sentinel_name = sentinel_sts_name(&name);
    let l = sentinel_labels(&name);
    let image = sentinel
        .image
        .clone()
        .unwrap_or_else(|| redis.spec.image.clone());
    let args = build_sentinel_args(redis, ns, sentinel);

    let ping = ExecAction {
        command: Some(vec![
            "redis-cli".into(),
            "-p".into(),
            SENTINEL_PORT.to_string(),
            "ping".into(),
        ]),
    };

    // Readiness reacts quickly: a sentinel in TILT shouldn't receive client
    // traffic, so drop it from the headless service after a couple of
    // failures.
    let readiness_probe = Some(Probe {
        exec: Some(ping.clone()),
        initial_delay_seconds: Some(5),
        period_seconds: Some(5),
        timeout_seconds: Some(3),
        failure_threshold: Some(2),
        ..Default::default()
    });

    // Liveness is forgiving. TILT mode lasts 30s and recovers on its own;
    // restarting the pod just resets the same condition. Only fire if the
    // process is genuinely wedged (~1 minute unresponsive).
    let liveness_probe = Some(Probe {
        exec: Some(ping),
        initial_delay_seconds: Some(15),
        period_seconds: Some(10),
        timeout_seconds: Some(5),
        failure_threshold: Some(6),
        ..Default::default()
    });

    // Storage is opt-in: with PVC, sentinel-rewritten config persists across
    // restarts; without, it's emptyDir and sentinel rediscovers on restart.
    let (volume_claim_templates, pod_volumes, retention) = match &sentinel.storage {
        Some(s) => {
            let mut requests = BTreeMap::new();
            requests.insert("storage".into(), Quantity(s.size.clone()));
            let pvcs = vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some(DATA_VOLUME.to_string()),
                    ..Default::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    resources: Some(VolumeResourceRequirements {
                        requests: Some(requests),
                        limits: None,
                    }),
                    storage_class_name: s.storage_class.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }];
            let retention =
                s.retention
                    .as_ref()
                    .map(|r| StatefulSetPersistentVolumeClaimRetentionPolicy {
                        when_deleted: r.when_deleted.clone(),
                        when_scaled: r.when_scaled.clone(),
                    });
            (Some(pvcs), None, retention)
        }
        None => {
            let volumes = vec![Volume {
                name: DATA_VOLUME.to_string(),
                empty_dir: Some(EmptyDirVolumeSource::default()),
                ..Default::default()
            }];
            (None, Some(volumes), None)
        }
    };

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(sentinel_name.clone()),
            namespace: Some(ns.to_string()),
            labels: Some(l.clone()),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(sentinel.replicas),
            service_name: Some(sentinel_name),
            selector: LabelSelector {
                match_labels: Some(l.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(l),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "sentinel".to_string(),
                        image: Some(image),
                        command: Some(vec![args[0].clone()]),
                        args: Some(args[1..].to_vec()),
                        ports: Some(vec![ContainerPort {
                            name: Some("sentinel".to_string()),
                            container_port: SENTINEL_PORT,
                            protocol: Some("TCP".to_string()),
                            ..Default::default()
                        }]),
                        liveness_probe,
                        readiness_probe,
                        resources: Some(
                            sentinel
                                .resources
                                .as_ref()
                                .map(build_resource_requirements)
                                .unwrap_or_else(default_sentinel_resources),
                        ),
                        volume_mounts: Some(vec![VolumeMount {
                            name: DATA_VOLUME.to_string(),
                            mount_path: "/data".to_string(),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }],
                    volumes: pod_volumes,
                    ..Default::default()
                }),
            },
            volume_claim_templates,
            persistent_volume_claim_retention_policy: retention,
            ..Default::default()
        }),
        status: None,
    }
}

/// Resolve the pod name of the current master.
///
/// - When Sentinel is enabled: query `sentinel-0` for the master's host. If
///   the query fails, fall back to the existing primary-Service selector
///   (preserving the last known master) — falling back to pod-0 here would
///   point clients at a replica post-failover.
/// - When Sentinel is disabled: pod-0 is always the master.
async fn determine_master_pod(
    client: &Client,
    svc_api: &Api<Service>,
    redis: &Redis,
    ns: &str,
) -> Result<String> {
    let name = redis.name_any();
    let pod0 = format!("{name}-0");

    if redis.spec.sentinel.is_none() {
        return Ok(pod0);
    }

    let sentinel_pod = format!("{}-0", sentinel_sts_name(&name));
    match query_sentinel_master(client, ns, &sentinel_pod).await {
        Ok(Some(host)) => match master_pod_from_host(&host) {
            Some(pod) => Ok(pod),
            None => {
                warn!(%host, "sentinel returned unparseable master host; preserving prior selector");
                preserve_existing_master(svc_api, &name, &pod0).await
            }
        },
        Ok(None) => {
            // Sentinel responded but has no master yet (e.g., no quorum during
            // initial bootstrap). Pod-0 is the bootstrap primary.
            Ok(pod0)
        }
        Err(e) => {
            warn!(?e, "sentinel query failed; preserving prior primary-Service selector");
            preserve_existing_master(svc_api, &name, &pod0).await
        }
    }
}

/// Patch each redis pod with `redis-operator/role=master|replica`, driving the
/// replicas Service selector. Filters by `app.kubernetes.io/name=redis` so
/// sentinel pods are excluded. Skips patches when the label is already
/// correct to avoid update churn.
async fn label_pods_by_role(
    client: &Client,
    ns: &str,
    name: &str,
    master_pod: &str,
) -> Result<()> {
    let api: Api<Pod> = Api::namespaced(client.clone(), ns);
    let selector =
        format!("app.kubernetes.io/name=redis,app.kubernetes.io/instance={name}");
    let pods = api.list(&ListParams::default().labels(&selector)).await?;
    for pod in pods.items {
        let pod_name = pod.name_any();
        let role = if pod_name == master_pod { "master" } else { "replica" };
        if pod.labels().get(ROLE_LABEL).map(String::as_str) == Some(role) {
            continue;
        }
        let patch = json!({"metadata": {"labels": {ROLE_LABEL: role}}});
        api.patch(&pod_name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;
    }
    Ok(())
}

async fn preserve_existing_master(
    svc_api: &Api<Service>,
    name: &str,
    fallback: &str,
) -> Result<String> {
    let existing = svc_api
        .get_opt(&primary_service_name(name))
        .await?
        .and_then(|s| s.spec)
        .and_then(|s| s.selector)
        .and_then(|sel| sel.get(POD_NAME_LABEL).cloned());
    Ok(existing.unwrap_or_else(|| fallback.to_string()))
}

async fn query_sentinel_master(
    client: &Client,
    ns: &str,
    pod: &str,
) -> Result<Option<String>> {
    let out = pod_exec(
        client,
        ns,
        pod,
        &[
            "redis-cli",
            "-p",
            "26379",
            "sentinel",
            "get-master-addr-by-name",
            MASTER_NAME,
        ],
    )
    .await?;
    Ok(parse_sentinel_master_addr(&out).map(|(host, _)| host))
}

/// Parses `redis-cli SENTINEL get-master-addr-by-name <name>` output:
/// ```text
/// 1) "host"
/// 2) "6379"
/// ```
/// Tolerates both raw and prefixed-array styles. Returns None for empty or
/// nil responses (which `redis-cli` prints as `(nil)` when sentinel has no
/// master yet).
fn parse_sentinel_master_addr(out: &str) -> Option<(String, i32)> {
    let mut vals = out
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("(nil)"))
        .map(|l| {
            l.split_once(") ")
                .map(|(_, v)| v)
                .unwrap_or(l)
                .trim_matches('"')
                .to_string()
        });
    let host = vals.next()?;
    if host.is_empty() {
        return None;
    }
    let port = vals.next()?.parse().ok()?;
    Some((host, port))
}

/// Map an FQDN like `cache-1.cache.default.svc.cluster.local` back to the
/// pod name (`cache-1`). Returns None for an empty or dotless host.
fn master_pod_from_host(host: &str) -> Option<String> {
    let pod = host.split('.').next()?;
    (!pod.is_empty()).then(|| pod.to_string())
}

async fn query_connected_replicas(client: &Client, ns: &str, pod: &str) -> Result<i32> {
    let out = pod_exec(client, ns, pod, &["redis-cli", "info", "replication"]).await?;
    Ok(parse_connected_slaves(&out))
}

fn parse_connected_slaves(s: &str) -> i32 {
    for line in s.lines() {
        if let Some(v) = line.trim().strip_prefix("connected_slaves:") {
            return v.trim().parse().unwrap_or(0);
        }
    }
    0
}

async fn pod_exec(client: &Client, ns: &str, pod: &str, cmd: &[&str]) -> Result<String> {
    let api: Api<Pod> = Api::namespaced(client.clone(), ns);
    let argv: Vec<String> = cmd.iter().map(|s| (*s).to_string()).collect();
    let mut process = api
        .exec(pod, &argv, &AttachParams::default().stderr(true))
        .await?;

    let mut stdout = process
        .stdout()
        .ok_or_else(|| Error::Exec("no stdout stream".into()))?;
    let mut stderr = process
        .stderr()
        .ok_or_else(|| Error::Exec("no stderr stream".into()))?;

    let mut out_buf = Vec::new();
    let mut err_buf = Vec::new();
    let (r1, r2) = tokio::join!(
        stdout.read_to_end(&mut out_buf),
        stderr.read_to_end(&mut err_buf),
    );
    r1?;
    r2?;

    process
        .join()
        .await
        .map_err(|e| Error::Exec(e.to_string()))?;

    if !err_buf.is_empty() {
        let err = String::from_utf8_lossy(&err_buf);
        warn!(?cmd, %err, "exec produced stderr");
    }
    Ok(String::from_utf8_lossy(&out_buf).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_connected_slaves_extracts_count() {
        let info = "# Replication\n\
                    role:master\n\
                    connected_slaves:2\n\
                    slave0:ip=10.0.0.5,port=6379,state=online\n";
        assert_eq!(parse_connected_slaves(info), 2);
    }

    #[test]
    fn parse_connected_slaves_missing_returns_zero() {
        assert_eq!(parse_connected_slaves("# Replication\nrole:slave\n"), 0);
    }

    #[test]
    fn parse_connected_slaves_unparseable_returns_zero() {
        assert_eq!(parse_connected_slaves("connected_slaves:abc\n"), 0);
    }

    #[test]
    fn parse_sentinel_master_addr_handles_prefixed_array() {
        let out = "1) \"cache-1.cache.default.svc.cluster.local\"\n2) \"6379\"\n";
        assert_eq!(
            parse_sentinel_master_addr(out),
            Some(("cache-1.cache.default.svc.cluster.local".into(), 6379))
        );
    }

    #[test]
    fn parse_sentinel_master_addr_handles_raw_output() {
        let out = "cache-2.cache.ns.svc.cluster.local\n6379\n";
        assert_eq!(
            parse_sentinel_master_addr(out),
            Some(("cache-2.cache.ns.svc.cluster.local".into(), 6379))
        );
    }

    #[test]
    fn parse_sentinel_master_addr_returns_none_for_nil() {
        assert_eq!(parse_sentinel_master_addr("(nil)\n"), None);
        assert_eq!(parse_sentinel_master_addr(""), None);
    }

    #[test]
    fn master_pod_from_host_strips_dns_suffix() {
        assert_eq!(
            master_pod_from_host("cache-2.cache.default.svc.cluster.local").as_deref(),
            Some("cache-2")
        );
    }

    #[test]
    fn master_pod_from_host_handles_short_name() {
        assert_eq!(master_pod_from_host("cache-0").as_deref(), Some("cache-0"));
    }

    #[test]
    fn master_pod_from_host_rejects_empty() {
        assert_eq!(master_pod_from_host(""), None);
    }

    #[test]
    fn sentinel_quorum_defaults_to_majority() {
        let s = SentinelSpec {
            replicas: 3,
            ..Default::default()
        };
        assert_eq!(s.effective_quorum(), 2);

        let s = SentinelSpec {
            replicas: 5,
            ..Default::default()
        };
        assert_eq!(s.effective_quorum(), 3);
    }

    #[test]
    fn sentinel_quorum_honors_explicit_value() {
        let s = SentinelSpec {
            replicas: 5,
            quorum: Some(4),
            ..Default::default()
        };
        assert_eq!(s.effective_quorum(), 4);
    }
}
