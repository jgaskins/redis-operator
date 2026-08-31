use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::{
    StatefulSet, StatefulSetPersistentVolumeClaimRetentionPolicy, StatefulSetSpec,
};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EmptyDirVolumeSource, EnvVar, ExecAction, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, Pod, PodSpec, PodTemplateSpec, Probe, Service, ServicePort,
    ServiceSpec, Volume, VolumeMount, VolumeResourceRequirements,
};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{AttachParams, ObjectMeta, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{Controller, watcher};
use kube::{Api, Client, Resource, ResourceExt};
use serde_json::json;
use tokio::io::AsyncReadExt;
use tracing::{info, warn};

use crate::controller::{
    Context, FIELD_MANAGER, PdbRequest, PdbVerdict, apply, effective_redis_resources,
    effective_topology_spread, eviction_args, maxmemory_bytes, persistence_args, reconcile_pdb,
};
use crate::crd::RedisCluster;
use crate::crd::redis_cluster::{NodeStatus, RedisClusterStatus, total_pods};
use crate::error::{Error, Result};

const TOTAL_SLOTS: i32 = 16384;

const REDIS_PORT: i32 = 6379;
const CLUSTER_BUS_PORT: i32 = 16379;
const DATA_VOLUME: &str = "data";

pub async fn run(ctx: Arc<Context>) -> anyhow::Result<()> {
    let rc_api: Api<RedisCluster> = Api::all(ctx.client.clone());
    rc_api
        .list(&Default::default())
        .await
        .map_err(|e| anyhow::anyhow!("RedisCluster CRD not installed or inaccessible: {e}"))?;

    let sts_api: Api<StatefulSet> = Api::all(ctx.client.clone());
    let svc_api: Api<Service> = Api::all(ctx.client.clone());
    let pdb_api: Api<PodDisruptionBudget> = Api::all(ctx.client.clone());

    info!("starting RedisCluster controller");

    Controller::new(rc_api, watcher::Config::default())
        .owns(sts_api, watcher::Config::default())
        .owns(svc_api, watcher::Config::default())
        .owns(pdb_api, watcher::Config::default())
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

/// Times the reconcile and counts its outcome.
///
/// A wrapper rather than instrumentation inside the body, and rather than
/// `error_policy`: the body has several early returns, and `error_policy` only
/// ever sees the failures, so neither would count the successes exactly once.
async fn reconcile(rc: Arc<RedisCluster>, ctx: Arc<Context>) -> Result<Action> {
    let timer = ctx.metrics.reconcile_started("RedisCluster");
    let result = reconcile_inner(rc, ctx.clone()).await;
    timer.finish(result.is_ok());
    result
}

async fn reconcile_inner(rc: Arc<RedisCluster>, ctx: Arc<Context>) -> Result<Action> {
    let name = rc.name_any();
    let ns = rc.namespace().ok_or(Error::MissingMetadata("namespace"))?;
    info!(%name, %ns, "reconciling RedisCluster");

    let owner = rc
        .controller_owner_ref(&())
        .ok_or(Error::MissingMetadata("uid"))?;

    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &ns);
    let pdb_api: Api<PodDisruptionBudget> = Api::namespaced(ctx.client.clone(), &ns);
    let rc_api: Api<RedisCluster> = Api::namespaced(ctx.client.clone(), &ns);

    apply(&svc_api, &name, &build_service(&rc, &ns, owner.clone())).await?;

    let total = rc.spec.total_pods();

    let current_replicas = sts_api
        .get_opt(&name)
        .await?
        .and_then(|s| s.spec)
        .and_then(|s| s.replicas)
        .unwrap_or(0);

    if current_replicas > total {
        info!(
            %current_replicas,
            %total,
            "scale-down detected; draining cluster nodes before shrinking StatefulSet"
        );
        drain_for_scale_down(&ctx.client, &ns, &name, total, current_replicas).await?;
    }

    apply(&sts_api, &name, &build_statefulset(&rc, &ns, owner.clone())).await?;

    // After the StatefulSet, never before: a rejected budget must not be able to
    // stop the workload itself from reconciling.
    let obj_ref = rc.object_ref(&());
    let pdb_verdict = reconcile_pdb(
        &pdb_api,
        &ctx,
        PdbRequest {
            name: &name,
            ns: &ns,
            labels: &labels(&name),
            owner,
            obj_ref: &obj_ref,
            desired: rc.spec.pod_disruption_budget.clone(),
            total,
            safe_max_unavailable: rc.spec.safe_max_unavailable(),
        },
    )
    .await?;

    if !pdb_verdict.should_apply() && pdb_verdict != PdbVerdict::Empty {
        set_phase(&rc_api, &name, "Degraded").await?;
        return Ok(Action::requeue(Duration::from_secs(60)));
    }

    let ready = sts_api
        .get_opt(&name)
        .await?
        .and_then(|s| s.status)
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);

    if ready < total {
        info!(%ready, %total, "waiting for pods to be ready before bootstrap");
        set_phase(&rc_api, &name, "Pending").await?;
        return Ok(Action::requeue(Duration::from_secs(10)));
    }

    ensure_cluster_topology(
        &ctx.client,
        &ns,
        &name,
        rc.spec.masters,
        rc.spec.replicas_per_master,
    )
    .await?;

    let status =
        compute_status(&ctx.client, &ns, &name, total, rc.spec.replicas_per_master).await?;
    set_status(&rc_api, &name, &status).await?;

    Ok(Action::requeue(Duration::from_secs(60)))
}

async fn set_status(
    api: &Api<RedisCluster>,
    name: &str,
    status: &RedisClusterStatus,
) -> Result<()> {
    let patch = json!({ "status": status });
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    Ok(())
}

/// Partial status patch — only updates `phase`, leaving other fields intact.
/// Used during the readiness gate when we can't observe cluster topology yet.
async fn set_phase(api: &Api<RedisCluster>, name: &str, phase: &str) -> Result<()> {
    let patch = json!({ "status": { "phase": phase } });
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    Ok(())
}

/// Snapshot the cluster's topology from a member pod and derive the
/// status payload — aggregates plus per-node breakdown.
async fn compute_status(
    client: &Client,
    ns: &str,
    sts_name: &str,
    total: i32,
    replicas_per_master: i32,
) -> Result<RedisClusterStatus> {
    let Some((seed_pod, info)) = find_seed(client, ns, sts_name, total).await? else {
        return Ok(RedisClusterStatus {
            phase: Some("Pending".to_string()),
            ..Default::default()
        });
    };
    let nodes = parse_cluster_nodes(
        &exec(client, ns, &seed_pod, &["redis-cli", "cluster", "nodes"]).await?,
    );

    // Initialize replica-count per master so masters with zero replicas appear.
    let mut replica_count_by_master: BTreeMap<String, i32> = nodes
        .iter()
        .filter(|n| n.is_master())
        .map(|n| (n.id.clone(), 0))
        .collect();
    for n in &nodes {
        if let Some(mid) = &n.master_id {
            *replica_count_by_master.entry(mid.clone()).or_insert(0) += 1;
        }
    }

    let masters = nodes.iter().filter(|n| n.is_master()).count() as i32;
    let replicas = nodes.iter().filter(|n| !n.is_master()).count() as i32;
    let slots_assigned: i32 = nodes
        .iter()
        .filter(|n| n.is_master())
        .map(|n| n.slot_count() as i32)
        .sum();
    let replica_imbalance: i32 = replica_count_by_master
        .values()
        .map(|c| (c - replicas_per_master).abs())
        .sum();

    let mut node_statuses: Vec<NodeStatus> = nodes
        .iter()
        .map(|n| {
            let is_master = n.is_master();
            NodeStatus {
                id: n.id.clone(),
                hostname: n.hostname.clone(),
                role: if is_master {
                    "master".into()
                } else {
                    "replica".into()
                },
                master_id: n.master_id.clone().unwrap_or_default(),
                slot_count: if is_master { n.slot_count() as i32 } else { 0 },
                replica_count: if is_master {
                    *replica_count_by_master.get(&n.id).unwrap_or(&0)
                } else {
                    0
                },
            }
        })
        .collect();
    node_statuses.sort_by(|a, b| a.hostname.cmp(&b.hostname));

    let phase =
        if info.cluster_state == "ok" && slots_assigned == TOTAL_SLOTS && replica_imbalance == 0 {
            "Running"
        } else if info.cluster_state == "ok" {
            "Degraded"
        } else {
            "Pending"
        };

    Ok(RedisClusterStatus {
        ready_nodes: info.cluster_known_nodes,
        phase: Some(phase.to_string()),
        cluster_state: Some(info.cluster_state),
        masters,
        replicas,
        replica_imbalance,
        slots_assigned,
        nodes: node_statuses,
    })
}

fn error_policy(_obj: Arc<RedisCluster>, err: &Error, _ctx: Arc<Context>) -> Action {
    warn!(?err, "RedisCluster reconcile error");
    Action::requeue(Duration::from_secs(15))
}

fn labels(name: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert("app.kubernetes.io/name".into(), "redis-cluster".into());
    m.insert("app.kubernetes.io/instance".into(), name.to_string());
    m.insert("app.kubernetes.io/managed-by".into(), FIELD_MANAGER.into());
    m
}

fn build_service(rc: &RedisCluster, ns: &str, owner: OwnerReference) -> Service {
    let name = rc.name_any();
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
            ports: Some(vec![
                ServicePort {
                    name: Some("redis".into()),
                    port: REDIS_PORT,
                    target_port: Some(IntOrString::Int(REDIS_PORT)),
                    protocol: Some("TCP".into()),
                    ..Default::default()
                },
                ServicePort {
                    name: Some("cluster-bus".into()),
                    port: CLUSTER_BUS_PORT,
                    target_port: Some(IntOrString::Int(CLUSTER_BUS_PORT)),
                    protocol: Some("TCP".into()),
                    ..Default::default()
                },
            ]),
            publish_not_ready_addresses: Some(true),
            ..Default::default()
        }),
        status: None,
    }
}

fn build_statefulset(rc: &RedisCluster, ns: &str, owner: OwnerReference) -> StatefulSet {
    let name = rc.name_any();
    let l = labels(&name);
    let replicas = rc.spec.total_pods();

    // `/data` is mounted either way. Beyond the RDB/AOF files it holds
    // `nodes.conf`, the cluster identity and slot map — previously that landed
    // on the container's writable layer when `storage` was unset, so a mere
    // container restart lost the node's identity and forced the heal path in
    // `reconcile_stale_members` to forget and re-add it. An emptyDir survives
    // that, though still not a reschedule.
    let volume_mounts = Some(vec![VolumeMount {
        name: DATA_VOLUME.into(),
        mount_path: "/data".into(),
        ..Default::default()
    }]);

    let (volume_claim_templates, pod_volumes) = match &rc.spec.storage {
        Some(s) => {
            let mut requests = BTreeMap::new();
            requests.insert("storage".into(), Quantity(s.size.clone()));
            let pvcs = vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some(DATA_VOLUME.into()),
                    ..Default::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".into()]),
                    resources: Some(VolumeResourceRequirements {
                        requests: Some(requests),
                        limits: None,
                    }),
                    storage_class_name: s.storage_class.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }];
            (Some(pvcs), None)
        }
        None => (
            None,
            Some(vec![Volume {
                name: DATA_VOLUME.into(),
                empty_dir: Some(EmptyDirVolumeSource::default()),
                ..Default::default()
            }]),
        ),
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

    let resources = effective_redis_resources(rc.spec.resources.as_ref());
    let maxmem = maxmemory_bytes(&resources)
        .map(|b| format!(" --maxmemory {b}"))
        .unwrap_or_default();
    let spread = effective_topology_spread(rc.spec.topology_spread_constraints.as_ref(), &l);

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(ns.to_string()),
            labels: Some(l.clone()),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(replicas),
            service_name: Some(name.clone()),
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
                        name: "redis".into(),
                        image: Some(rc.spec.image.clone()),
                        command: Some(vec!["sh".into(), "-c".into()]),
                        env: Some(vec![EnvVar {
                            name: "SKIP_DROP_PRIVS".to_string(),
                            value: Some("1".to_string()),
                            value_from: None,
                        }]),
                        args: Some(vec![format!(
                            "exec docker-entrypoint.sh redis-server \
                                --cluster-enabled yes \
                                --cluster-config-file /data/nodes.conf \
                                --cluster-node-timeout 5000 \
                                --cluster-announce-hostname \
                                  \"$(hostname).{svc}.{ns}.svc.cluster.local\" \
                                --cluster-preferred-endpoint-type hostname\
                                {persistence}{maxmem}{eviction}",
                            svc = name,
                            ns = ns,
                            persistence = persistence_args(rc.spec.persistence.as_ref()),
                            eviction = eviction_args(rc.spec.eviction_policy),
                        )]),
                        ports: Some(vec![
                            ContainerPort {
                                name: Some("redis".into()),
                                container_port: REDIS_PORT,
                                protocol: Some("TCP".into()),
                                ..Default::default()
                            },
                            ContainerPort {
                                name: Some("cluster-bus".into()),
                                container_port: CLUSTER_BUS_PORT,
                                protocol: Some("TCP".into()),
                                ..Default::default()
                            },
                        ]),
                        liveness_probe: probe.clone(),
                        readiness_probe: probe,
                        resources: Some(resources),
                        volume_mounts,
                        ..Default::default()
                    }],
                    topology_spread_constraints: spread,
                    volumes: pod_volumes,
                    ..Default::default()
                }),
            },
            // Gives the cluster bus time to converge between pod restarts.
            // Outside the controller-revision hash, so it triggers no roll of
            // its own.
            min_ready_seconds: Some(10),
            volume_claim_templates,
            persistent_volume_claim_retention_policy: rc
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

#[derive(Default, Debug)]
struct ClusterInfo {
    cluster_state: String,
    cluster_known_nodes: i32,
}

fn parse_cluster_info(s: &str) -> ClusterInfo {
    let mut info = ClusterInfo::default();
    for line in s.lines() {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        match k.trim() {
            "cluster_state" => info.cluster_state = v.trim().to_string(),
            "cluster_known_nodes" => info.cluster_known_nodes = v.trim().parse().unwrap_or(0),
            _ => {}
        }
    }
    info
}

#[derive(Debug, Clone)]
struct ClusterNode {
    id: String,
    /// `ip:port` (bus port stripped) as recorded in the cluster table.
    addr: String,
    hostname: String,
    flags: Vec<String>,
    master_id: Option<String>,
    slots: Vec<(u16, u16)>,
}

impl ClusterNode {
    fn is_master(&self) -> bool {
        self.flags.iter().any(|f| f == "master")
    }

    fn slot_count(&self) -> u32 {
        self.slots
            .iter()
            .map(|(a, b)| u32::from(*b) - u32::from(*a) + 1)
            .sum()
    }
}

/// Parse `redis-cli cluster nodes` output. Format per line:
/// `<id> <ip:port@cport[,hostname]> <flags> <master-id-or-dash> <ping> <pong> <epoch> <link> [<slot>...]`
/// Nodes without an announced hostname are skipped — we rely on hostnames for
/// stable pod identity across IP changes.
fn parse_cluster_nodes(s: &str) -> Vec<ClusterNode> {
    let mut out = Vec::new();
    for line in s.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 8 {
            continue;
        }
        let Some((addr, hostname)) = parts[1].split_once(',') else {
            continue;
        };
        let addr = addr.split('@').next().unwrap_or(addr);
        let flags: Vec<String> = parts[2].split(',').map(|s| s.to_string()).collect();
        let master_id = (parts[3] != "-").then(|| parts[3].to_string());
        let slots = parse_slots(&parts[8..]);
        out.push(ClusterNode {
            id: parts[0].to_string(),
            addr: addr.to_string(),
            hostname: hostname.to_string(),
            flags,
            master_id,
            slots,
        });
    }
    out
}

fn parse_slots(parts: &[&str]) -> Vec<(u16, u16)> {
    let mut ranges = Vec::new();
    for p in parts {
        // Skip migration markers like "[12-><id>]" or "[12-<-<id>]".
        if p.starts_with('[') {
            continue;
        }
        match p.split_once('-') {
            Some((a, b)) => {
                if let (Ok(a), Ok(b)) = (a.parse(), b.parse()) {
                    ranges.push((a, b));
                }
            }
            None => {
                if let Ok(s) = p.parse() {
                    ranges.push((s, s));
                }
            }
        }
    }
    ranges
}

fn pod_hostname(sts_name: &str, ns: &str, ordinal: i32) -> String {
    format!("{sts_name}-{ordinal}.{sts_name}.{ns}.svc.cluster.local")
}

/// First DNS label of an announced hostname — the pod name.
fn pod_of(hostname: &str) -> &str {
    hostname.split('.').next().unwrap_or(hostname)
}

/// Find a pod that's a member of a formed cluster (knows more than itself) to
/// use as the contact point for topology reads and cluster-management
/// commands. Scanning past pod 0 matters: if pod 0 loses its cluster state
/// (e.g. a restart wipes nodes.conf), reading the topology from it would make
/// a formed cluster look un-bootstrapped.
///
/// Prefers a pod whose own `cluster_state` is `ok`: a node isolated from the
/// rest of the cluster still reports the full node count but sees every slot
/// as pfail, and its stale view must not be treated as authoritative when a
/// healthy member is available.
async fn find_seed(
    client: &Client,
    ns: &str,
    sts_name: &str,
    total: i32,
) -> Result<Option<(String, ClusterInfo)>> {
    let mut fallback = None;
    for ordinal in 0..total {
        let pod = format!("{sts_name}-{ordinal}");
        match cluster_info(client, ns, &pod).await {
            Ok(info) if info.cluster_known_nodes > 1 && info.cluster_state == "ok" => {
                return Ok(Some((pod, info)));
            }
            Ok(info) if info.cluster_known_nodes > 1 => {
                if fallback.is_none() {
                    fallback = Some((pod, info));
                }
            }
            Ok(_) => {}
            Err(err) => warn!(%pod, %err, "cluster info failed; trying next pod"),
        }
    }
    Ok(fallback)
}

/// Current IP of a pod, from the Kubernetes API. `CLUSTER MEET` needs a live
/// address — the cluster bus doesn't dial announced hostnames.
async fn pod_ip(client: &Client, ns: &str, pod: &str) -> Result<Option<String>> {
    let api: Api<Pod> = Api::namespaced(client.clone(), ns);
    Ok(api.get(pod).await?.status.and_then(|s| s.pod_ip))
}

async fn cluster_info(client: &Client, ns: &str, pod: &str) -> Result<ClusterInfo> {
    let out = exec(client, ns, pod, &["redis-cli", "cluster", "info"]).await?;
    Ok(parse_cluster_info(&out))
}

async fn ensure_cluster_topology(
    client: &Client,
    ns: &str,
    sts_name: &str,
    masters: i32,
    replicas_per_master: i32,
) -> Result<()> {
    let total = total_pods(masters, replicas_per_master);

    let Some((seed_pod, _)) = find_seed(client, ns, sts_name, total).await? else {
        bootstrap(client, ns, sts_name, masters, replicas_per_master).await?;
        return Ok(());
    };
    let seed = format!("{seed_pod}.{sts_name}.{ns}.svc.cluster.local:{REDIS_PORT}");

    let mut nodes = parse_cluster_nodes(
        &exec(client, ns, &seed_pod, &["redis-cli", "cluster", "nodes"]).await?,
    );

    if heal_failed_nodes(client, ns, &seed_pod, &nodes).await? {
        nodes = parse_cluster_nodes(
            &exec(client, ns, &seed_pod, &["redis-cli", "cluster", "nodes"]).await?,
        );
    }

    let in_cluster: HashSet<&str> = nodes.iter().map(|n| n.hostname.as_str()).collect();

    let unassigned: Vec<i32> = (0..total)
        .filter(|i| !in_cluster.contains(pod_hostname(sts_name, ns, *i).as_str()))
        .collect();

    if !unassigned.is_empty() {
        let mut current_master_count = nodes.iter().filter(|n| n.is_master()).count() as i32;
        let mut replicas_per: BTreeMap<String, i32> = nodes
            .iter()
            .filter(|n| n.is_master())
            .map(|n| (n.id.clone(), 0))
            .collect();
        for n in &nodes {
            if let Some(mid) = &n.master_id {
                *replicas_per.entry(mid.clone()).or_insert(0) += 1;
            }
        }

        let mut added_masters = false;
        for ordinal in unassigned {
            let pod_addr = format!("{}:{REDIS_PORT}", pod_hostname(sts_name, ns, ordinal));
            if current_master_count < masters {
                info!(%pod_addr, "adding new master to cluster");
                exec(
                    client,
                    ns,
                    &seed_pod,
                    &["redis-cli", "--cluster", "add-node", &pod_addr, &seed],
                )
                .await?;
                current_master_count += 1;
                added_masters = true;
            } else {
                let Some((target_id, _)) = replicas_per.iter().min_by_key(|(_, c)| **c) else {
                    warn!(%pod_addr, "no master available to attach replica to");
                    continue;
                };
                let target_id = target_id.clone();
                info!(%pod_addr, %target_id, "adding new replica to cluster");
                exec(
                    client,
                    ns,
                    &seed_pod,
                    &[
                        "redis-cli",
                        "--cluster",
                        "add-node",
                        &pod_addr,
                        &seed,
                        "--cluster-slave",
                        "--cluster-master-id",
                        &target_id,
                    ],
                )
                .await?;
                *replicas_per.entry(target_id).or_insert(0) += 1;
            }
        }

        if added_masters {
            info!("rebalancing slots after adding masters");
            exec(
                client,
                ns,
                &seed_pod,
                &[
                    "redis-cli",
                    "--cluster",
                    "rebalance",
                    &seed,
                    "--cluster-use-empty-masters",
                ],
            )
            .await?;
        }
    }

    // Must run before balance_replicas: repair compares each replica's live
    // target against this (pre-move) table, so it must not run after balance
    // re-points replicas or it would see those moves as stale and undo them.
    repair_replication(client, ns, &nodes).await?;

    balance_replicas(client, ns, &seed_pod, replicas_per_master).await?;

    Ok(())
}

/// Re-point replicas whose replication link targets a stale address. Gossip
/// updates a node's cluster table when a peer's address changes, but the
/// replication link doesn't always follow (observed after re-introducing an
/// isolated node: its table showed the master's new address while replication
/// kept dialing the dead one). The cluster table can't reveal this — the
/// replica's entry stays `connected` — so ask each replica pod directly and
/// compare its live replication target against its master's table address.
/// `CLUSTER REPLICATE` against the already-assigned master re-resolves the
/// address and restarts the link.
async fn repair_replication(client: &Client, ns: &str, nodes: &[ClusterNode]) -> Result<()> {
    let is_failed = |n: &ClusterNode| n.flags.iter().any(|f| f == "fail");
    for replica in nodes.iter().filter(|n| !n.is_master() && !is_failed(n)) {
        let Some(master) = replica
            .master_id
            .as_ref()
            .and_then(|mid| nodes.iter().find(|m| &m.id == mid))
        else {
            continue;
        };
        if is_failed(master) {
            continue;
        }
        let pod = pod_of(&replica.hostname);
        let repl = match exec(client, ns, pod, &["redis-cli", "info", "replication"]).await {
            Ok(out) => out,
            Err(err) => {
                warn!(%pod, %err, "could not read replication info");
                continue;
            }
        };
        let Some((host, port)) = parse_replication_target(&repl) else {
            continue;
        };
        let Some((master_ip, master_port)) = master.addr.split_once(':') else {
            continue;
        };
        if port == master_port && (host == master_ip || host == master.hostname) {
            continue;
        }
        info!(
            %pod,
            replicating_from = %format!("{host}:{port}"),
            master_addr = %master.addr,
            master_id = %master.id,
            "replica is syncing from a stale master address; re-pointing"
        );
        exec(
            client,
            ns,
            pod,
            &["redis-cli", "cluster", "replicate", &master.id],
        )
        .await?;
    }
    Ok(())
}

/// Extract `(master_host, master_port)` from `INFO replication` output.
/// Returns None for masters or unparseable output.
fn parse_replication_target(s: &str) -> Option<(String, String)> {
    let mut is_replica = false;
    let mut host = None;
    let mut port = None;
    for line in s.lines() {
        let line = line.trim();
        if let Some(v) = line.strip_prefix("role:") {
            is_replica = v == "slave";
        } else if let Some(v) = line.strip_prefix("master_host:") {
            host = Some(v.to_string());
        } else if let Some(v) = line.strip_prefix("master_port:") {
            port = Some(v.to_string());
        }
    }
    if !is_replica {
        return None;
    }
    Some((host?, port?))
}

/// Recover pods behind `fail`-flagged cluster entries. Asking the pod at the
/// entry's hostname for its current ID distinguishes two failure modes:
///
/// - Same ID: the pod kept its identity but is cut off — every peer address
///   in its own node table is stale (e.g. every pod got a new IP at once
///   during a node-pool rotation), so the cluster bus redials dead IPs
///   forever and the node can never rejoin on its own. A `CLUSTER MEET`
///   pointed at a current member address re-establishes the link; gossip
///   then refreshes addresses on both sides and clears the fail flags.
///   Idempotent — the ID is already a member of this cluster.
///
/// - Different ID: the pod lost its cluster state (nodes.conf wiped) and came
///   back as a brand-new empty node, while the cluster gossips the old ID as
///   `fail` forever — and because the stale entry still announces the pod's
///   hostname, the hostname-based membership check never re-adds the pod.
///   Forget the stale ID on every healthy member and wipe the reset pod so
///   the caller's re-add path picks it up as unassigned.
///
/// Returns true if the topology changed and should be re-read.
async fn heal_failed_nodes(
    client: &Client,
    ns: &str,
    seed_pod: &str,
    nodes: &[ClusterNode],
) -> Result<bool> {
    let is_failed = |n: &ClusterNode| n.flags.iter().any(|f| f == "fail");
    let mut healed = false;
    for stale in nodes.iter().filter(|n| is_failed(n)) {
        if stale.slot_count() > 0 {
            warn!(
                hostname = %stale.hostname,
                id = %stale.id,
                "failed node still owns slots; refusing to forget it \
                 (would orphan its slots) — needs manual intervention"
            );
            continue;
        }
        let pod = pod_of(&stale.hostname);
        let myid = match exec(client, ns, pod, &["redis-cli", "cluster", "myid"]).await {
            Ok(out) => out.trim().to_string(),
            Err(err) => {
                info!(%pod, %err, "failed node's pod unreachable; will retry next reconcile");
                continue;
            }
        };
        if myid.is_empty() {
            continue;
        }
        if myid == stale.id {
            let Some(ip) = pod_ip(client, ns, seed_pod).await? else {
                warn!(%seed_pod, "seed pod has no IP; cannot re-introduce isolated node");
                continue;
            };
            info!(
                %pod,
                id = %myid,
                seed = %seed_pod,
                "failed node is alive with its original identity but isolated; \
                 re-introducing via CLUSTER MEET"
            );
            let port = REDIS_PORT.to_string();
            exec(
                client,
                ns,
                pod,
                &["redis-cli", "cluster", "meet", &ip, &port],
            )
            .await?;
            healed = true;
            continue;
        }

        info!(
            hostname = %stale.hostname,
            stale_id = %stale.id,
            current_id = %myid,
            "pod lost its cluster identity; forgetting stale entry"
        );
        for member in nodes.iter().filter(|m| m.id != stale.id && !is_failed(m)) {
            if let Err(err) = exec(
                client,
                ns,
                pod_of(&member.hostname),
                &["redis-cli", "cluster", "forget", &stale.id],
            )
            .await
            {
                warn!(member = %member.hostname, %err, "cluster forget failed");
            }
        }

        // If the pod already rejoined under its new ID, forgetting the stale
        // entry is all that's needed; don't touch a live member. Otherwise the
        // pod isn't a cluster member, so any data it replayed from an old AOF
        // is an unreachable stale copy — clear it so add-node accepts the
        // node as empty.
        if nodes.iter().all(|m| m.id != myid) {
            exec(client, ns, pod, &["redis-cli", "flushall"]).await?;
            exec(client, ns, pod, &["redis-cli", "cluster", "reset", "hard"]).await?;
        }
        healed = true;
    }
    Ok(healed)
}

/// Re-attaches replicas so each master ends up with `replicas_per_master`
/// replicas, by issuing `CLUSTER REPLICATE` on excess replicas of overloaded
/// masters. Doesn't add or remove nodes — surplus replicas (e.g. after
/// `replicasPerMaster` was decreased without a scale-down) are left alone for
/// the next scale-down pass to handle.
async fn balance_replicas(
    client: &Client,
    ns: &str,
    seed_pod: &str,
    replicas_per_master: i32,
) -> Result<()> {
    let nodes =
        parse_cluster_nodes(&exec(client, ns, seed_pod, &["redis-cli", "cluster", "nodes"]).await?);

    let masters: Vec<&ClusterNode> = nodes.iter().filter(|n| n.is_master()).collect();
    let mut replicas_by_master: BTreeMap<String, Vec<&ClusterNode>> =
        masters.iter().map(|m| (m.id.clone(), Vec::new())).collect();
    for n in &nodes {
        if let Some(mid) = &n.master_id {
            replicas_by_master.entry(mid.clone()).or_default().push(n);
        }
    }

    let target = replicas_per_master;
    let mut deficits: Vec<(String, i32)> = masters
        .iter()
        .filter_map(|m| {
            let count = replicas_by_master
                .get(&m.id)
                .map(|v| v.len() as i32)
                .unwrap_or(0);
            (target - count > 0).then(|| (m.id.clone(), target - count))
        })
        .collect();

    let mut moves: Vec<(String, String, String)> = Vec::new();
    for m in &masters {
        let Some(replicas) = replicas_by_master.get(&m.id) else {
            continue;
        };
        let count = replicas.len() as i32;
        if count <= target {
            continue;
        }
        for replica in &replicas[target as usize..] {
            let Some(idx) = deficits.iter().position(|(_, d)| *d > 0) else {
                break;
            };
            let to_master = deficits[idx].0.clone();
            deficits[idx].1 -= 1;
            moves.push((
                pod_of(&replica.hostname).to_string(),
                m.id.clone(),
                to_master,
            ));
        }
    }

    for (replica_pod, from_master, to_master) in moves {
        info!(
            %replica_pod,
            %from_master,
            %to_master,
            "rebalancing replica to under-loaded master"
        );
        exec(
            client,
            ns,
            &replica_pod,
            &["redis-cli", "cluster", "replicate", &to_master],
        )
        .await?;
    }

    Ok(())
}

async fn drain_for_scale_down(
    client: &Client,
    ns: &str,
    sts_name: &str,
    new_total: i32,
    old_total: i32,
) -> Result<()> {
    let Some((seed_pod, _)) = find_seed(client, ns, sts_name, old_total).await? else {
        // Cluster never bootstrapped; nothing to drain.
        return Ok(());
    };
    let seed = format!("{seed_pod}.{sts_name}.{ns}.svc.cluster.local:{REDIS_PORT}");

    let nodes = parse_cluster_nodes(
        &exec(client, ns, &seed_pod, &["redis-cli", "cluster", "nodes"]).await?,
    );

    let removed: HashSet<String> = (new_total..old_total)
        .map(|i| pod_hostname(sts_name, ns, i))
        .collect();

    let surviving_master = nodes
        .iter()
        .find(|n| n.is_master() && !removed.contains(&n.hostname))
        .ok_or_else(|| Error::Exec("scale-down would leave zero surviving masters".into()))?
        .clone();

    // Pass 1: remove replicas (no slot migration needed).
    for n in &nodes {
        if removed.contains(&n.hostname) && !n.is_master() {
            info!(hostname = %n.hostname, "removing replica from cluster");
            exec(
                client,
                ns,
                &seed_pod,
                &["redis-cli", "--cluster", "del-node", &seed, &n.id],
            )
            .await?;
        }
    }

    // Pass 2: migrate slots away from each master being removed, then del-node.
    let mut migrated_any = false;
    for n in &nodes {
        if !(removed.contains(&n.hostname) && n.is_master()) {
            continue;
        }
        let slots = n.slot_count();
        if slots > 0 {
            info!(
                hostname = %n.hostname,
                slots,
                target = %surviving_master.hostname,
                "migrating slots from master being removed"
            );
            exec(
                client,
                ns,
                &seed_pod,
                &[
                    "redis-cli",
                    "--cluster",
                    "reshard",
                    &seed,
                    "--cluster-from",
                    &n.id,
                    "--cluster-to",
                    &surviving_master.id,
                    "--cluster-slots",
                    &slots.to_string(),
                    "--cluster-yes",
                ],
            )
            .await?;
            migrated_any = true;
        }
        info!(hostname = %n.hostname, "removing master from cluster");
        exec(
            client,
            ns,
            &seed_pod,
            &["redis-cli", "--cluster", "del-node", &seed, &n.id],
        )
        .await?;
    }

    if migrated_any {
        info!("rebalancing slots across surviving masters");
        exec(
            client,
            ns,
            &seed_pod,
            &[
                "redis-cli",
                "--cluster",
                "rebalance",
                &seed,
                "--cluster-use-empty-masters",
            ],
        )
        .await?;
    }

    Ok(())
}

async fn bootstrap(
    client: &Client,
    ns: &str,
    sts_name: &str,
    masters: i32,
    replicas_per_master: i32,
) -> Result<()> {
    info!(%sts_name, "bootstrapping Redis cluster");
    let pod0 = format!("{sts_name}-0");
    let total = total_pods(masters, replicas_per_master);
    let endpoints: Vec<String> = (0..total)
        .map(|i| format!("{}:{REDIS_PORT}", pod_hostname(sts_name, ns, i)))
        .collect();

    let mut cmd: Vec<String> = vec!["redis-cli".into(), "--cluster".into(), "create".into()];
    cmd.extend(endpoints);
    cmd.push("--cluster-replicas".into());
    cmd.push(replicas_per_master.to_string());
    cmd.push("--cluster-yes".into());

    let out = pod_exec(client, ns, &pod0, &cmd).await?;
    info!(%out, "cluster create finished");
    Ok(())
}

async fn exec(client: &Client, ns: &str, pod: &str, cmd: &[&str]) -> Result<String> {
    let v: Vec<String> = cmd.iter().map(|s| (*s).to_string()).collect();
    pod_exec(client, ns, pod, &v).await
}

async fn pod_exec(client: &Client, ns: &str, pod: &str, cmd: &[String]) -> Result<String> {
    let api: Api<Pod> = Api::namespaced(client.clone(), ns);
    let mut process = api
        .exec(pod, cmd, &AttachParams::default().stderr(true))
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
    use crate::crd::redis::EvictionPolicy;
    use crate::crd::redis_cluster::RedisClusterSpec;

    #[test]
    fn cluster_statefulset_gets_default_topology_spread() {
        let rc = RedisCluster::new(
            "redis",
            RedisClusterSpec {
                masters: 3,
                replicas_per_master: 1,
                ..Default::default()
            },
        );
        let sts = build_statefulset(&rc, "ns", OwnerReference::default());
        let spec = sts.spec.unwrap();
        assert_eq!(spec.replicas, Some(6));
        let c = spec
            .template
            .spec
            .unwrap()
            .topology_spread_constraints
            .expect("expected a default spread constraint");
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].max_skew, 1);
        assert_eq!(c[0].topology_key, "kubernetes.io/hostname");
        assert_eq!(c[0].when_unsatisfiable, "ScheduleAnyway");
        assert_eq!(
            c[0].label_selector.clone().unwrap().match_labels.unwrap(),
            labels("redis")
        );
    }

    #[test]
    fn cluster_statefulset_carries_the_eviction_policy() {
        let rc = RedisCluster::new(
            "redis",
            RedisClusterSpec {
                eviction_policy: Some(EvictionPolicy::AllkeysLfu),
                ..Default::default()
            },
        );
        let sts = build_statefulset(&rc, "ns", OwnerReference::default());
        let args = sts.spec.unwrap().template.spec.unwrap().containers[0]
            .args
            .clone()
            .expect("container should have args");
        assert!(args[0].contains("--maxmemory-policy allkeys-lfu"));
    }

    #[test]
    fn cluster_statefulset_defaults_to_noeviction() {
        let rc = RedisCluster::new("redis", RedisClusterSpec::default());
        let sts = build_statefulset(&rc, "ns", OwnerReference::default());
        let args = sts.spec.unwrap().template.spec.unwrap().containers[0]
            .args
            .clone()
            .expect("container should have args");
        assert!(args[0].contains("--maxmemory-policy noeviction"));
    }

    #[test]
    fn cluster_statefulset_omits_spread_when_opted_out() {
        let rc = RedisCluster::new(
            "redis",
            RedisClusterSpec {
                masters: 3,
                replicas_per_master: 1,
                topology_spread_constraints: Some(vec![]),
                ..Default::default()
            },
        );
        let sts = build_statefulset(&rc, "ns", OwnerReference::default());
        assert!(
            sts.spec
                .unwrap()
                .template
                .spec
                .unwrap()
                .topology_spread_constraints
                .is_none()
        );
    }
}
