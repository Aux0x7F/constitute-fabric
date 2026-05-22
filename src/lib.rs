use anyhow::{Result, anyhow};
use constitute_protocol::{
    FABRIC_FULFILLMENT_PLAN_BLOCKED, FABRIC_FULFILLMENT_PLAN_DEGRADED,
    FABRIC_FULFILLMENT_PLAN_READY, FABRIC_LIFECYCLE_PLAN_BLOCKED, FABRIC_LIFECYCLE_PLAN_DEGRADED,
    FABRIC_LIFECYCLE_PLAN_EXPIRED, FABRIC_MEMBER_CONTRIBUTION_ACCEPTED,
    FABRIC_MEMBER_CONTRIBUTION_BLOCKED, FABRIC_MEMBER_CONTRIBUTION_CLAIMED,
    FABRIC_MEMBER_CONTRIBUTION_DEGRADED, FABRIC_MEMBER_CONTRIBUTION_EXPIRED,
    FABRIC_MEMBER_CONTRIBUTION_RELEASED, FABRIC_MEMBER_CONTRIBUTION_RUNNING,
    FABRIC_MEMBER_CONTRIBUTION_SUPERSEDED, HostFabricFulfillmentPlan, HostFabricMemberContribution,
    LifecyclePlanPosture, RECORD_HOST_FABRIC_FULFILLMENT_PLAN,
    validate_host_fabric_fulfillment_plan, validate_host_fabric_member_contribution,
    validate_lifecycle_plan_posture,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

pub const DEFAULT_MATERIALIZATION_BUDGET_REF: &str = "materialization-budget:host-fabric";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HostFabricRoleRequirement {
    pub role_ref: String,
    #[serde(default = "one")]
    pub min_ready: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HostFabricReductionInput {
    pub plan_id: String,
    pub fabric_ref: String,
    pub host_ref: String,
    pub contract_ref: String,
    #[serde(default)]
    pub required_roles: Vec<HostFabricRoleRequirement>,
    #[serde(default)]
    pub contributions: Vec<HostFabricMemberContribution>,
    #[serde(default)]
    pub lifecycle_plans: Vec<LifecyclePlanPosture>,
    #[serde(default)]
    pub materialization_budget_refs: Vec<String>,
    #[serde(default)]
    pub known_missing_role_refs: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub blocked_reasons: Vec<String>,
    pub association_handoff_ref: Option<String>,
    pub observed_at: u64,
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HostFabricReduction {
    pub fulfillment_plan: HostFabricFulfillmentPlan,
    pub ready_contribution_refs: Vec<String>,
    pub degraded_contribution_refs: Vec<String>,
    pub blocked_contribution_refs: Vec<String>,
    pub filtered_contribution_refs: Vec<String>,
    pub lifecycle_plan_refs: Vec<String>,
    pub blocked_reasons: Vec<String>,
}

fn one() -> usize {
    1
}

pub fn reduce_host_fabric(input: HostFabricReductionInput) -> Result<HostFabricReduction> {
    require_ref(&input.plan_id, "planId")?;
    require_ref(&input.fabric_ref, "fabricRef")?;
    require_ref(&input.host_ref, "hostRef")?;
    require_ref(&input.contract_ref, "contractRef")?;
    if input.required_roles.is_empty() {
        return Err(anyhow!("host fabric reduction requires requiredRoles"));
    }
    if input
        .expires_at
        .is_some_and(|expires_at| expires_at <= input.observed_at)
    {
        return Err(anyhow!(
            "host fabric reduction expiresAt must be after observedAt"
        ));
    }

    let mut required_role_refs = Vec::new();
    for role in &input.required_roles {
        require_ref(&role.role_ref, "requiredRoles roleRef")?;
        if role.min_ready == 0 {
            return Err(anyhow!(
                "host fabric role requirement minReady must be positive"
            ));
        }
        required_role_refs.push(role.role_ref.clone());
    }

    let mut ready_by_role: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut usable_by_role: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut ready_contribution_refs = BTreeSet::new();
    let mut degraded_contribution_refs = BTreeSet::new();
    let mut blocked_contribution_refs = BTreeSet::new();
    let mut filtered_contribution_refs = BTreeSet::new();
    let mut lifecycle_plan_refs = BTreeSet::new();
    let mut blocked_reasons = BTreeSet::from_iter(input.blocked_reasons.clone());

    for contribution in &input.contributions {
        validate_host_fabric_member_contribution(contribution)?;
        if contribution.fabric_ref != input.fabric_ref || contribution.host_ref != input.host_ref {
            filtered_contribution_refs.insert(contribution.contribution_id.clone());
            continue;
        }
        let role_ref = role_ref(&contribution.role);
        if is_ready_contribution(&contribution.state) {
            ready_by_role
                .entry(role_ref.clone())
                .or_default()
                .push(contribution.contribution_id.clone());
            usable_by_role
                .entry(role_ref)
                .or_default()
                .push(contribution.contribution_id.clone());
            ready_contribution_refs.insert(contribution.contribution_id.clone());
        } else if is_degraded_contribution(&contribution.state) {
            usable_by_role
                .entry(role_ref)
                .or_default()
                .push(contribution.contribution_id.clone());
            degraded_contribution_refs.insert(contribution.contribution_id.clone());
        } else if is_blocked_contribution(&contribution.state) {
            blocked_contribution_refs.insert(contribution.contribution_id.clone());
            blocked_reasons.extend(contribution.blocked_reasons.clone());
            blocked_reasons.insert(format!(
                "hostFabric:contribution:{}:{}",
                contribution.state, contribution.contribution_id
            ));
        }
        lifecycle_plan_refs.extend(contribution.lifecycle_plan_refs.clone());
    }

    let mut missing_role_refs = BTreeSet::from_iter(normalize_refs(input.known_missing_role_refs));
    for missing in &missing_role_refs {
        blocked_reasons.insert(format!("hostFabric:missingRole:{missing}"));
    }
    let mut degraded_roles = BTreeSet::new();
    for required in &input.required_roles {
        let ready = ready_by_role
            .get(&required.role_ref)
            .map_or(0, std::vec::Vec::len);
        let usable = usable_by_role
            .get(&required.role_ref)
            .map_or(0, std::vec::Vec::len);
        if ready < required.min_ready {
            if usable >= required.min_ready {
                degraded_roles.insert(required.role_ref.clone());
            } else {
                missing_role_refs.insert(required.role_ref.clone());
                blocked_reasons.insert(format!("hostFabric:missingRole:{}", required.role_ref));
            }
        }
    }

    for lifecycle_plan in &input.lifecycle_plans {
        validate_lifecycle_plan_posture(lifecycle_plan)?;
        lifecycle_plan_refs.insert(lifecycle_plan.lifecycle_plan_id.clone());
        if matches!(
            lifecycle_plan.state.as_str(),
            FABRIC_LIFECYCLE_PLAN_BLOCKED | FABRIC_LIFECYCLE_PLAN_EXPIRED
        ) {
            blocked_reasons.extend(lifecycle_plan.blocked_reasons.clone());
            blocked_reasons.insert(format!(
                "hostFabric:lifecycle:{}:{}",
                lifecycle_plan.state, lifecycle_plan.lifecycle_plan_id
            ));
        } else if lifecycle_plan.state == FABRIC_LIFECYCLE_PLAN_DEGRADED {
            degraded_roles.insert(format!("lifecycle:{}", lifecycle_plan.lifecycle_plan_id));
        }
    }

    let member_contribution_refs = usable_by_role
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let missing_role_refs = missing_role_refs.into_iter().collect::<Vec<_>>();
    let blocked_reasons = blocked_reasons.into_iter().collect::<Vec<_>>();
    let lifecycle_plan_refs = lifecycle_plan_refs.into_iter().collect::<Vec<_>>();
    let state = if !blocked_reasons.is_empty() {
        FABRIC_FULFILLMENT_PLAN_BLOCKED
    } else if !degraded_roles.is_empty() || !degraded_contribution_refs.is_empty() {
        FABRIC_FULFILLMENT_PLAN_DEGRADED
    } else {
        FABRIC_FULFILLMENT_PLAN_READY
    };

    let materialization_budget_refs = if input.materialization_budget_refs.is_empty() {
        vec![DEFAULT_MATERIALIZATION_BUDGET_REF.to_string()]
    } else {
        normalize_refs(input.materialization_budget_refs)
    };
    let mut evidence_refs = normalize_refs(input.evidence_refs);
    evidence_refs.push(format!("evidence:host-fabric-reduction:{}", input.plan_id));
    evidence_refs = normalize_refs(evidence_refs);

    let fulfillment_plan = HostFabricFulfillmentPlan {
        kind: Some(RECORD_HOST_FABRIC_FULFILLMENT_PLAN.to_string()),
        plan_id: input.plan_id,
        fabric_ref: input.fabric_ref,
        host_ref: input.host_ref,
        contract_ref: input.contract_ref,
        state: state.to_string(),
        required_role_refs,
        member_contribution_refs,
        missing_role_refs,
        lifecycle_plan_refs: lifecycle_plan_refs.clone(),
        materialization_budget_refs,
        association_handoff_ref: input.association_handoff_ref,
        evidence_refs,
        blocked_reasons: blocked_reasons.clone(),
        safe_facts: json!({
            "reducer": "host-fabric",
            "state": state,
            "readyContributionCount": ready_contribution_refs.len(),
            "degradedContributionCount": degraded_contribution_refs.len(),
            "blockedContributionCount": blocked_contribution_refs.len(),
            "filteredContributionCount": filtered_contribution_refs.len()
        }),
        observed_at: input.observed_at,
        expires_at: input.expires_at,
    };
    validate_host_fabric_fulfillment_plan(&fulfillment_plan)?;

    Ok(HostFabricReduction {
        fulfillment_plan,
        ready_contribution_refs: ready_contribution_refs.into_iter().collect(),
        degraded_contribution_refs: degraded_contribution_refs.into_iter().collect(),
        blocked_contribution_refs: blocked_contribution_refs.into_iter().collect(),
        filtered_contribution_refs: filtered_contribution_refs.into_iter().collect(),
        lifecycle_plan_refs,
        blocked_reasons,
    })
}

fn require_ref(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(anyhow!("host fabric reduction missing {label}"))
    } else {
        Ok(())
    }
}

fn role_ref(role: &str) -> String {
    if role.starts_with("role:") {
        role.to_string()
    } else {
        format!("role:{role}")
    }
}

fn is_ready_contribution(state: &str) -> bool {
    matches!(
        state,
        FABRIC_MEMBER_CONTRIBUTION_ACCEPTED | FABRIC_MEMBER_CONTRIBUTION_RUNNING
    )
}

fn is_degraded_contribution(state: &str) -> bool {
    matches!(
        state,
        FABRIC_MEMBER_CONTRIBUTION_CLAIMED | FABRIC_MEMBER_CONTRIBUTION_DEGRADED
    )
}

fn is_blocked_contribution(state: &str) -> bool {
    matches!(
        state,
        FABRIC_MEMBER_CONTRIBUTION_BLOCKED
            | FABRIC_MEMBER_CONTRIBUTION_EXPIRED
            | FABRIC_MEMBER_CONTRIBUTION_RELEASED
            | FABRIC_MEMBER_CONTRIBUTION_SUPERSEDED
    )
}

fn normalize_refs(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use constitute_protocol::{
        FABRIC_LIFECYCLE_PHASE_OBSERVE, FABRIC_LIFECYCLE_PHASE_READY, FABRIC_LIFECYCLE_PHASE_RUN,
        FABRIC_LIFECYCLE_PHASE_RUNNING, FABRIC_LIFECYCLE_PLAN_READY,
        FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER, LifecyclePhasePosture,
        RECORD_HOST_FABRIC_MEMBER_CONTRIBUTION, RECORD_LIFECYCLE_PLAN_POSTURE,
    };

    const MEMBER_REF: &str = "4a29ff60c5c3837e9e20555bfeb2a046be3eb140818144628691fcf7efb1d2f1";

    fn contribution(
        id: &str,
        state: &str,
        fabric_ref: &str,
        host_ref: &str,
    ) -> HostFabricMemberContribution {
        HostFabricMemberContribution {
            kind: Some(RECORD_HOST_FABRIC_MEMBER_CONTRIBUTION.to_string()),
            contribution_id: id.to_string(),
            fabric_ref: fabric_ref.to_string(),
            host_ref: host_ref.to_string(),
            member_ref: MEMBER_REF.to_string(),
            role: FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER.to_string(),
            state: state.to_string(),
            contract_ref: "contract:host-service-adapter.service-manager@0.1.0".to_string(),
            subject_ref: "service:lab-managed".to_string(),
            capability_refs: vec!["capability:host-service:start".to_string()],
            grant_refs: vec!["grant:service-manager:lab-service".to_string()],
            input_refs: vec!["contract-target:desktop-dev:msa-transition".to_string()],
            output_refs: vec!["service:lab-managed".to_string()],
            evidence_refs: vec![format!("evidence:{id}")],
            lifecycle_plan_refs: vec!["lifecycle-plan:lab-service:start".to_string()],
            release_refs: vec![],
            resource_posture: None,
            blocked_reasons: if state == FABRIC_MEMBER_CONTRIBUTION_BLOCKED {
                vec!["host-service-adapter:blocked".to_string()]
            } else {
                vec![]
            },
            safe_facts: json!({ "fixture": id }),
            observed_at: 1_700_000_000,
            expires_at: Some(1_700_003_600),
        }
    }

    fn lifecycle(state: &str) -> LifecyclePlanPosture {
        LifecyclePlanPosture {
            kind: Some(RECORD_LIFECYCLE_PLAN_POSTURE.to_string()),
            lifecycle_plan_id: "lifecycle-plan:lab-service:start".to_string(),
            subject_ref: "service:lab-managed".to_string(),
            contract_ref: "contract:lifecycle.host-service-adapter@0.1.0".to_string(),
            state: state.to_string(),
            lifecycle_contract_refs: vec![
                "contract:lifecycle.host-service-adapter@0.1.0".to_string(),
            ],
            phase_postures: vec![
                LifecyclePhasePosture {
                    phase: FABRIC_LIFECYCLE_PHASE_RUN.to_string(),
                    state: FABRIC_LIFECYCLE_PHASE_RUNNING.to_string(),
                    evidence_refs: vec!["evidence:run".to_string()],
                    output_refs: vec!["service:lab-managed".to_string()],
                    blocked_reasons: vec![],
                    safe_facts: json!({ "phase": "run" }),
                },
                LifecyclePhasePosture {
                    phase: FABRIC_LIFECYCLE_PHASE_OBSERVE.to_string(),
                    state: FABRIC_LIFECYCLE_PHASE_READY.to_string(),
                    evidence_refs: vec!["evidence:observe".to_string()],
                    output_refs: vec!["proof:service:health".to_string()],
                    blocked_reasons: vec![],
                    safe_facts: json!({ "phase": "observe" }),
                },
            ],
            member_contribution_refs: vec!["fabric-contribution:service-manager".to_string()],
            evidence_refs: vec!["evidence:lifecycle".to_string()],
            release_refs: vec![],
            blocked_reasons: vec![],
            safe_facts: json!({ "fixture": "lifecycle" }),
            observed_at: 1_700_000_000,
            expires_at: Some(1_700_003_600),
        }
    }

    fn input(contributions: Vec<HostFabricMemberContribution>) -> HostFabricReductionInput {
        HostFabricReductionInput {
            plan_id: "fabric-plan:lab-gateway:service-manager".to_string(),
            fabric_ref: "fabric:lab-gateway".to_string(),
            host_ref: "host:lab-service-manager".to_string(),
            contract_ref: "contract:host-fabric.lab-gateway@0.1.0".to_string(),
            required_roles: vec![HostFabricRoleRequirement {
                role_ref: role_ref(FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER),
                min_ready: 1,
            }],
            contributions,
            lifecycle_plans: vec![lifecycle(FABRIC_LIFECYCLE_PLAN_READY)],
            materialization_budget_refs: vec!["materialization-budget:service-manager".to_string()],
            known_missing_role_refs: vec![],
            evidence_refs: vec!["evidence:operator:fixture".to_string()],
            blocked_reasons: vec![],
            association_handoff_ref: Some(
                "handoff:substrate:lab-gateway:initial-owner".to_string(),
            ),
            observed_at: 1_700_000_000,
            expires_at: Some(1_700_003_600),
        }
    }

    #[test]
    fn reduces_ready_host_adapter_contribution_into_ready_plan() {
        let reduction = reduce_host_fabric(input(vec![contribution(
            "fabric-contribution:service-manager",
            FABRIC_MEMBER_CONTRIBUTION_RUNNING,
            "fabric:lab-gateway",
            "host:lab-service-manager",
        )]))
        .expect("reduction");

        assert_eq!(
            reduction.fulfillment_plan.state,
            FABRIC_FULFILLMENT_PLAN_READY
        );
        assert_eq!(
            reduction.fulfillment_plan.member_contribution_refs,
            vec!["fabric-contribution:service-manager"]
        );
        assert!(reduction.fulfillment_plan.missing_role_refs.is_empty());
        assert_eq!(reduction.ready_contribution_refs.len(), 1);
    }

    #[test]
    fn degrades_when_role_is_only_claimed() {
        let reduction = reduce_host_fabric(input(vec![contribution(
            "fabric-contribution:service-manager",
            FABRIC_MEMBER_CONTRIBUTION_CLAIMED,
            "fabric:lab-gateway",
            "host:lab-service-manager",
        )]))
        .expect("reduction");

        assert_eq!(
            reduction.fulfillment_plan.state,
            FABRIC_FULFILLMENT_PLAN_DEGRADED
        );
        assert_eq!(reduction.degraded_contribution_refs.len(), 1);
        assert!(reduction.blocked_reasons.is_empty());
    }

    #[test]
    fn blocks_missing_required_role() {
        let reduction = reduce_host_fabric(input(vec![])).expect("reduction");

        assert_eq!(
            reduction.fulfillment_plan.state,
            FABRIC_FULFILLMENT_PLAN_BLOCKED
        );
        assert_eq!(
            reduction.fulfillment_plan.missing_role_refs,
            vec!["role:hostServiceAdapter"]
        );
        assert!(
            reduction
                .blocked_reasons
                .contains(&"hostFabric:missingRole:role:hostServiceAdapter".to_string())
        );
    }

    #[test]
    fn filters_contributions_for_other_hosts() {
        let reduction = reduce_host_fabric(input(vec![
            contribution(
                "fabric-contribution:other-host",
                FABRIC_MEMBER_CONTRIBUTION_RUNNING,
                "fabric:lab-gateway",
                "host:other",
            ),
            contribution(
                "fabric-contribution:service-manager",
                FABRIC_MEMBER_CONTRIBUTION_ACCEPTED,
                "fabric:lab-gateway",
                "host:lab-service-manager",
            ),
        ]))
        .expect("reduction");

        assert_eq!(
            reduction.fulfillment_plan.state,
            FABRIC_FULFILLMENT_PLAN_READY
        );
        assert_eq!(
            reduction.filtered_contribution_refs,
            vec!["fabric-contribution:other-host"]
        );
        assert_eq!(
            reduction.fulfillment_plan.member_contribution_refs,
            vec!["fabric-contribution:service-manager"]
        );
    }
}
