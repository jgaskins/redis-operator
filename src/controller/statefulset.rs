//! Applying a StatefulSet whose `volumeClaimTemplates` may have changed.
//!
//! The API server rejects any change to `volumeClaimTemplates` once a
//! StatefulSet exists (only `replicas`, `ordinals`, `template`,
//! `updateStrategy`, `persistentVolumeClaimRetentionPolicy` and
//! `minReadySeconds` are mutable). A plain apply of a rebuilt StatefulSet
//! therefore 422s on every reconcile after a `storage` edit, and because the
//! StatefulSet is applied early, nothing after it reconciles either.
//!
//! Instead, the live templates are sent back unchanged and the one storage
//! change Kubernetes can actually honour, growing a volume, is applied to the
//! PVCs directly. Any other template change needs the StatefulSet recreated,
//! which the operator reports rather than attempting.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ObjectReference, PersistentVolumeClaim};
use kube::Api;
use kube::api::{Patch, PatchParams};
use kube::runtime::events::{Event, EventType};
use serde_json::json;
use tracing::{info, warn};

use crate::controller::{Context, apply, emit, parse_quantity_bytes};
use crate::error::Result;

/// Whether the StatefulSet was sent to the API server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatefulSetOutcome {
    Applied,
    /// The desired `volumeClaimTemplates` differ from the live ones in a way
    /// only recreating the StatefulSet can resolve. Nothing was applied; a
    /// Warning Event on the CR says why.
    Blocked,
}

/// What a change between live and desired `volumeClaimTemplates` amounts to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ClaimPlan {
    Unchanged,
    /// Only storage requests differ. Each entry is a template name and the
    /// size now wanted for every PVC stamped from it.
    Resize(Vec<(String, String)>),
    /// Anything else — templates added or removed (`storage` toggled), a
    /// changed storage class or access mode.
    Incompatible(String),
}

/// The fields of a claim template the operator sets, keyed by template name.
/// Comparing just these ignores what the API server defaults on the live
/// object (`volumeMode`, an empty `status`).
#[derive(Debug, PartialEq, Eq)]
struct ClaimShape {
    access_modes: Option<Vec<String>>,
    storage_class: Option<String>,
}

fn claim_shapes(
    templates: Option<&Vec<PersistentVolumeClaim>>,
) -> BTreeMap<String, (ClaimShape, Option<String>)> {
    templates
        .into_iter()
        .flatten()
        .map(|t| {
            let spec = t.spec.as_ref();
            let shape = ClaimShape {
                access_modes: spec.and_then(|s| s.access_modes.clone()),
                storage_class: spec.and_then(|s| s.storage_class_name.clone()),
            };
            (
                t.metadata.name.clone().unwrap_or_default(),
                (shape, storage_request(t)),
            )
        })
        .collect()
}

fn storage_request(pvc: &PersistentVolumeClaim) -> Option<String> {
    pvc.spec
        .as_ref()?
        .resources
        .as_ref()?
        .requests
        .as_ref()?
        .get("storage")
        .map(|q| q.0.clone())
}

fn plan_claims(
    live: Option<&Vec<PersistentVolumeClaim>>,
    desired: Option<&Vec<PersistentVolumeClaim>>,
) -> ClaimPlan {
    let live = claim_shapes(live);
    let desired = claim_shapes(desired);

    if !live.keys().eq(desired.keys()) {
        return ClaimPlan::Incompatible(if live.is_empty() {
            "storage was added".to_string()
        } else if desired.is_empty() {
            "storage was removed".to_string()
        } else {
            "the set of volume claim templates changed".to_string()
        });
    }

    let mut resize = Vec::new();
    for (name, (live_shape, live_size)) in &live {
        let (desired_shape, desired_size) = &desired[name];
        if live_shape.storage_class != desired_shape.storage_class {
            return ClaimPlan::Incompatible(format!(
                "storage class changed from {} to {}",
                live_shape.storage_class.as_deref().unwrap_or("the default"),
                desired_shape
                    .storage_class
                    .as_deref()
                    .unwrap_or("the default"),
            ));
        }
        if live_shape.access_modes != desired_shape.access_modes {
            return ClaimPlan::Incompatible("access modes changed".to_string());
        }
        if live_size != desired_size
            && let Some(size) = desired_size
        {
            resize.push((name.clone(), size.clone()));
        }
    }

    if resize.is_empty() {
        ClaimPlan::Unchanged
    } else {
        ClaimPlan::Resize(resize)
    }
}

/// Server-side apply `desired`, keeping the live `volumeClaimTemplates` and
/// growing PVCs in place when only their size changed.
///
/// Returns `Err` only for genuine API failures, so a storage edit the
/// operator cannot carry out never wedges the rest of the reconcile.
pub async fn apply_statefulset(
    ctx: &Context,
    ns: &str,
    obj_ref: &ObjectReference,
    mut desired: StatefulSet,
) -> Result<StatefulSetOutcome> {
    let name = desired.metadata.name.clone().unwrap_or_default();
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), ns);

    let Some(live) = sts_api.get_opt(&name).await? else {
        apply(&sts_api, &name, &desired).await?;
        return Ok(StatefulSetOutcome::Applied);
    };
    let live_spec = live.spec.unwrap_or_default();
    let desired_spec = desired.spec.as_mut().expect("builders always set spec");

    match plan_claims(
        live_spec.volume_claim_templates.as_ref(),
        desired_spec.volume_claim_templates.as_ref(),
    ) {
        ClaimPlan::Unchanged => {
            apply(&sts_api, &name, &desired).await?;
        }
        ClaimPlan::Resize(sizes) => {
            // Byte-for-byte the live templates, so the immutable field is
            // untouched and the rest of the spec still applies.
            desired_spec.volume_claim_templates = live_spec.volume_claim_templates;
            let replicas = desired_spec
                .replicas
                .unwrap_or(1)
                .max(live_spec.replicas.unwrap_or(1));
            apply(&sts_api, &name, &desired).await?;
            resize_claims(ctx, ns, obj_ref, &name, replicas, &sizes).await?;
        }
        ClaimPlan::Incompatible(why) => {
            let note = format!(
                "StatefulSet {name} was not updated: {why}, and volumeClaimTemplates cannot \
                 be changed on an existing StatefulSet. Revert the change, or recreate the \
                 StatefulSet without touching its pods: \
                 kubectl -n {ns} delete statefulset {name} --cascade=orphan"
            );
            warn!(statefulset = %name, %note, "refusing volume claim template change");
            emit(
                ctx,
                obj_ref,
                Event {
                    type_: EventType::Warning,
                    reason: "StorageChangeRequiresRecreate".to_string(),
                    note: Some(note),
                    action: "ReconcileStatefulSet".to_string(),
                    secondary: None,
                },
            )
            .await;
            return Ok(StatefulSetOutcome::Blocked);
        }
    }
    Ok(StatefulSetOutcome::Applied)
}

/// What to do with one existing PVC given the size now wanted for it.
#[derive(Debug, PartialEq, Eq)]
enum ResizeStep {
    Expand,
    /// Already the wanted size. Also covers sizes that can't be parsed, which
    /// are left alone rather than guessed at.
    Keep,
    /// Kubernetes can't shrink a bound PVC.
    WouldShrink,
}

fn resize_step(current: Option<&str>, wanted: &str) -> ResizeStep {
    let (Some(current), Some(wanted)) = (
        current.and_then(parse_quantity_bytes),
        parse_quantity_bytes(wanted),
    ) else {
        return ResizeStep::Keep;
    };
    match wanted.cmp(&current) {
        std::cmp::Ordering::Greater => ResizeStep::Expand,
        std::cmp::Ordering::Equal => ResizeStep::Keep,
        std::cmp::Ordering::Less => ResizeStep::WouldShrink,
    }
}

/// Patch the storage request of each PVC the StatefulSet has stamped out,
/// named `<template>-<statefulset>-<ordinal>`. Ordinals without a PVC yet are
/// skipped: the controller creates them from the old template size, and the
/// next reconcile grows them.
async fn resize_claims(
    ctx: &Context,
    ns: &str,
    obj_ref: &ObjectReference,
    sts_name: &str,
    replicas: i32,
    sizes: &[(String, String)],
) -> Result<()> {
    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), ns);
    let mut expanded = Vec::new();
    let mut too_large = Vec::new();
    let mut refused = Vec::new();

    for (template, wanted) in sizes {
        for ordinal in 0..replicas {
            let pvc_name = format!("{template}-{sts_name}-{ordinal}");
            let Some(pvc) = pvc_api.get_opt(&pvc_name).await? else {
                continue;
            };
            match resize_step(storage_request(&pvc).as_deref(), wanted) {
                ResizeStep::Keep => {}
                ResizeStep::WouldShrink => too_large.push(pvc_name),
                ResizeStep::Expand => {
                    let patch = json!({"spec": {"resources": {"requests": {"storage": wanted}}}});
                    match pvc_api
                        .patch(&pvc_name, &PatchParams::default(), &Patch::Merge(&patch))
                        .await
                    {
                        Ok(_) => expanded.push(pvc_name),
                        // The storage class doesn't allow expansion, or a
                        // quota forbids it. The user's to fix, not the
                        // reconcile's.
                        Err(kube::Error::Api(e)) if matches!(e.code, 403 | 422) => {
                            refused.push(format!("{pvc_name} ({})", e.message));
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
    }

    let wanted = sizes
        .iter()
        .map(|(_, s)| s.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let mut events = Vec::new();
    if !expanded.is_empty() {
        info!(pvcs = ?expanded, %wanted, "expanding persistent volume claims");
        events.push((
            EventType::Normal,
            "ExpandingVolumes",
            format!("Requested {wanted} for {}", expanded.join(", ")),
        ));
    }
    if !too_large.is_empty() {
        events.push((
            EventType::Warning,
            "StorageShrinkUnsupported",
            format!(
                "storage.size is {wanted}, but {} already request more and Kubernetes cannot \
                 shrink a volume; leaving them as they are",
                too_large.join(", ")
            ),
        ));
    }
    if !refused.is_empty() {
        events.push((
            EventType::Warning,
            "VolumeExpansionFailed",
            format!(
                "Could not grow {} to {wanted}; the storage class may not set \
                 allowVolumeExpansion",
                refused.join(", ")
            ),
        ));
    }
    for (type_, reason, note) in events {
        if type_ == EventType::Warning {
            warn!(statefulset = %sts_name, %note, "{reason}");
        }
        emit(
            ctx,
            obj_ref,
            Event {
                type_,
                reason: reason.to_string(),
                note: Some(note),
                action: "ResizeVolumes".to_string(),
                secondary: None,
            },
        )
        .await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use k8s_openapi::api::core::v1::{PersistentVolumeClaimSpec, VolumeResourceRequirements};
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use kube::api::ObjectMeta;

    fn template(size: &str, class: Option<&str>) -> PersistentVolumeClaim {
        PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some("data".to_string()),
                ..Default::default()
            },
            spec: Some(PersistentVolumeClaimSpec {
                access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                resources: Some(VolumeResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".to_string(),
                        Quantity(size.to_string()),
                    )])),
                    limits: None,
                }),
                storage_class_name: class.map(str::to_string),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn identical_templates_are_unchanged() {
        let t = vec![template("20Gi", None)];
        assert_eq!(plan_claims(Some(&t), Some(&t)), ClaimPlan::Unchanged);
        assert_eq!(plan_claims(None, None), ClaimPlan::Unchanged);
    }

    #[test]
    fn server_defaulted_fields_are_ignored() {
        let desired = vec![template("20Gi", None)];
        let mut live = desired.clone();
        live[0].spec.as_mut().unwrap().volume_mode = Some("Filesystem".to_string());
        assert_eq!(
            plan_claims(Some(&live), Some(&desired)),
            ClaimPlan::Unchanged
        );
    }

    #[test]
    fn size_change_is_a_resize() {
        assert_eq!(
            plan_claims(
                Some(&vec![template("20Gi", None)]),
                Some(&vec![template("30Gi", None)])
            ),
            ClaimPlan::Resize(vec![("data".to_string(), "30Gi".to_string())])
        );
    }

    #[test]
    fn smaller_size_is_still_a_resize_so_the_rest_applies() {
        // The PVCs decide whether it is a shrink; the StatefulSet must still
        // apply either way.
        assert!(matches!(
            plan_claims(
                Some(&vec![template("30Gi", None)]),
                Some(&vec![template("20Gi", None)])
            ),
            ClaimPlan::Resize(_)
        ));
    }

    #[test]
    fn storage_class_change_is_incompatible() {
        assert!(matches!(
            plan_claims(
                Some(&vec![template("20Gi", None)]),
                Some(&vec![template("20Gi", Some("fast"))])
            ),
            ClaimPlan::Incompatible(_)
        ));
    }

    #[test]
    fn toggling_storage_is_incompatible() {
        let t = vec![template("20Gi", None)];
        assert_eq!(
            plan_claims(None, Some(&t)),
            ClaimPlan::Incompatible("storage was added".to_string())
        );
        assert_eq!(
            plan_claims(Some(&t), None),
            ClaimPlan::Incompatible("storage was removed".to_string())
        );
    }

    #[test]
    fn resize_step_expands_only_upward() {
        assert_eq!(resize_step(Some("20Gi"), "30Gi"), ResizeStep::Expand);
        assert_eq!(resize_step(Some("30Gi"), "30Gi"), ResizeStep::Keep);
        assert_eq!(resize_step(Some("60Gi"), "30Gi"), ResizeStep::WouldShrink);
    }

    #[test]
    fn resize_step_compares_across_units() {
        assert_eq!(resize_step(Some("1024Mi"), "1Gi"), ResizeStep::Keep);
        assert_eq!(resize_step(Some("1G"), "1Gi"), ResizeStep::Expand);
    }

    #[test]
    fn resize_step_leaves_unparseable_sizes_alone() {
        assert_eq!(resize_step(Some("lots"), "30Gi"), ResizeStep::Keep);
        assert_eq!(resize_step(None, "30Gi"), ResizeStep::Keep);
    }
}
