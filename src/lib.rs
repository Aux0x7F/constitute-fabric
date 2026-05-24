use anyhow::{Result, anyhow};
use constitute_protocol::{
    CARRIER_EDGE_ADAPTER_IPC, CARRIER_EDGE_ADAPTER_LOOPBACK, CARRIER_EDGE_ADAPTER_NAMED_PIPE,
    CARRIER_EDGE_ADAPTER_QUIC, CARRIER_EDGE_ADAPTER_RELAY, CARRIER_EDGE_ADAPTER_WEB_SOCKET,
    CARRIER_EDGE_ADAPTER_WORKER, CARRIER_EDGE_BACKPRESSURE_BLOCKED,
    CARRIER_EDGE_BACKPRESSURE_CLEAR, CARRIER_EDGE_BACKPRESSURE_DEGRADED,
    CARRIER_EDGE_NETWORK_EXTERNAL_NETWORK, CARRIER_EDGE_NETWORK_HOST_LOCAL,
    CARRIER_EDGE_NETWORK_LOCAL_NETWORK, CARRIER_EDGE_NETWORK_LOOPBACK, CARRIER_EDGE_NETWORK_NONE,
    CARRIER_EDGE_NETWORK_PROCESS_LOCAL, CARRIER_EDGE_REQUIREMENT_ACTIONABLE,
    CARRIER_EDGE_REQUIREMENT_BLOCKED, CARRIER_EDGE_SELECTION_ACTIONABLE,
    CARRIER_EDGE_SELECTION_BLOCKED, CARRIER_EDGE_SELECTION_DEGRADED, CarrierEdgeRequirement,
    CarrierEdgeSelection, ContractTarget, ContractTargetRegistryPosture, ContractTargetSlotPosture,
    FABRIC_ADAPTER_EXECUTION_BLOCKED, FABRIC_ADAPTER_EXECUTION_FAILED,
    FABRIC_CONTRACT_TARGET_PLATFORM_FIT_COMPATIBLE, FABRIC_CONTRACT_TARGET_PLATFORM_FIT_UNKNOWN,
    FABRIC_CONTRACT_TARGET_REGISTRY_BLOCKED, FABRIC_CONTRACT_TARGET_REGISTRY_READY,
    FABRIC_CONTRACT_TARGET_SLOT_AVAILABLE, FABRIC_CONTRACT_TARGET_SLOT_MISSING,
    FABRIC_CONTROL_DECISION_BLOCKED, FABRIC_CONTROL_DECISION_DEGRADED,
    FABRIC_CONTROL_DECISION_EXPIRED, FABRIC_CONTROL_DECISION_READY,
    FABRIC_FULFILLMENT_PLAN_BLOCKED, FABRIC_FULFILLMENT_PLAN_DEGRADED,
    FABRIC_FULFILLMENT_PLAN_EXPIRED, FABRIC_FULFILLMENT_PLAN_READY,
    FABRIC_LIFECYCLE_DEPENDENCY_BLOCKED, FABRIC_LIFECYCLE_DEPENDENCY_DEGRADED,
    FABRIC_LIFECYCLE_DEPENDENCY_MISSING, FABRIC_LIFECYCLE_PLAN_BLOCKED,
    FABRIC_LIFECYCLE_PLAN_DEGRADED, FABRIC_LIFECYCLE_PLAN_EXPIRED,
    FABRIC_MEMBER_CONTRIBUTION_ACCEPTED, FABRIC_MEMBER_CONTRIBUTION_BLOCKED,
    FABRIC_MEMBER_CONTRIBUTION_CLAIMED, FABRIC_MEMBER_CONTRIBUTION_DEGRADED,
    FABRIC_MEMBER_CONTRIBUTION_EXPIRED, FABRIC_MEMBER_CONTRIBUTION_RELEASED,
    FABRIC_MEMBER_CONTRIBUTION_RUNNING, FABRIC_MEMBER_CONTRIBUTION_SUPERSEDED,
    FABRIC_MEMBER_ROLE_DOMAIN_SERVICE, FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION,
    FABRIC_MEMBER_ROLE_PLATFORM_ADAPTER, FABRIC_MEMBER_ROLE_RUNTIME,
    FABRIC_MEMBER_ROLE_SERVICE_EDGE_ADAPTER, FABRIC_TOPOLOGY_ROLE_BLOCKED,
    FABRIC_TOPOLOGY_ROLE_DEGRADED, FABRIC_TOPOLOGY_ROLE_MISSING, FABRIC_TOPOLOGY_ROLE_READY,
    HostFabricAdapterExecutionEvidence, HostFabricControlDecision, HostFabricFulfillmentPlan,
    HostFabricMemberContribution, HostFabricTopologyProjection, HostFabricTopologyRolePosture,
    LifecyclePlanPosture, RECORD_CARRIER_EDGE_REQUIREMENT, RECORD_CARRIER_EDGE_SELECTION,
    RECORD_CONTRACT_TARGET_REGISTRY_POSTURE, RECORD_HOST_FABRIC_ADAPTER_EXECUTION_EVIDENCE,
    RECORD_HOST_FABRIC_CONTROL_DECISION, RECORD_HOST_FABRIC_FULFILLMENT_PLAN,
    RECORD_HOST_FABRIC_MEMBER_CONTRIBUTION, RECORD_HOST_FABRIC_TOPOLOGY_PROJECTION,
    ResourcePosture, validate_carrier_edge_requirement, validate_carrier_edge_selection,
    validate_contract_target, validate_contract_target_registry_posture,
    validate_host_fabric_adapter_execution_evidence, validate_host_fabric_control_decision,
    validate_host_fabric_fulfillment_plan, validate_host_fabric_member_contribution,
    validate_host_fabric_topology_projection, validate_lifecycle_plan_posture,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
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
    pub topology_projection: HostFabricTopologyProjection,
    pub ready_contribution_refs: Vec<String>,
    pub degraded_contribution_refs: Vec<String>,
    pub blocked_contribution_refs: Vec<String>,
    pub filtered_contribution_refs: Vec<String>,
    pub lifecycle_plan_refs: Vec<String>,
    pub dependency_edge_refs: Vec<String>,
    pub blocked_reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HostFabricControlDecisionInput {
    pub decision_id: String,
    pub operation_ref: String,
    pub subject_ref: String,
    #[serde(default)]
    pub control_owner_ref: Option<String>,
    #[serde(default)]
    pub delegated_role_ref: Option<String>,
    #[serde(default)]
    pub execution_delegation_ref: Option<String>,
    #[serde(default)]
    pub authorization_refs: Vec<String>,
    #[serde(default)]
    pub fallback_refs: Vec<String>,
    #[serde(default)]
    pub quarantine_refs: Vec<String>,
    #[serde(default)]
    pub rollback_ref: Option<String>,
    #[serde(default)]
    pub release_refs: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub blocked_reasons: Vec<String>,
    #[serde(default)]
    pub safe_facts: serde_json::Value,
    pub observed_at: u64,
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HostFabricAdapterExecutionInput {
    pub evidence_id: String,
    pub adapter_ref: String,
    pub state: String,
    #[serde(default)]
    pub source_bridge_ref: Option<String>,
    #[serde(default)]
    pub delegated_role_ref: Option<String>,
    #[serde(default)]
    pub action_authority_refs: Vec<String>,
    #[serde(default)]
    pub evidence_requirement_refs: Vec<String>,
    #[serde(default)]
    pub input_refs: Vec<String>,
    #[serde(default)]
    pub output_refs: Vec<String>,
    #[serde(default)]
    pub fallback_refs: Vec<String>,
    #[serde(default)]
    pub quarantine_refs: Vec<String>,
    #[serde(default)]
    pub rollback_refs: Vec<String>,
    #[serde(default)]
    pub release_refs: Vec<String>,
    #[serde(default)]
    pub cleanup_refs: Vec<String>,
    #[serde(default)]
    pub blocked_reasons: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub safe_facts: serde_json::Value,
    pub observed_at: u64,
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HostFabricShadowParityInput {
    pub reduction: HostFabricReductionInput,
    #[serde(default)]
    pub legacy_ready_role_refs: Vec<String>,
    #[serde(default)]
    pub legacy_blocked_role_refs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HostFabricShadowParity {
    pub reduction: HostFabricReduction,
    pub agreement_role_refs: Vec<String>,
    pub disagreement_role_refs: Vec<String>,
    pub blocked_reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HostFabricMemberContributionSpec {
    pub contribution_id: String,
    pub fabric_ref: String,
    pub host_ref: String,
    pub member_ref: String,
    pub participant_ref: String,
    pub role: String,
    pub role_ref: String,
    pub state: String,
    pub contract_ref: String,
    pub subject_ref: String,
    #[serde(default)]
    pub module_refs: Vec<String>,
    #[serde(default)]
    pub source_refs: Vec<String>,
    #[serde(default)]
    pub capability_refs: Vec<String>,
    #[serde(default)]
    pub grant_refs: Vec<String>,
    #[serde(default)]
    pub input_refs: Vec<String>,
    #[serde(default)]
    pub output_refs: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub lifecycle_plan_refs: Vec<String>,
    #[serde(default)]
    pub release_refs: Vec<String>,
    #[serde(default)]
    pub resource_posture: Option<ResourcePosture>,
    #[serde(default)]
    pub blocked_reasons: Vec<String>,
    #[serde(default)]
    pub safe_facts: serde_json::Value,
    pub observed_at: u64,
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContractTargetRegistryReductionInput {
    pub plan_id: String,
    pub fabric_ref: String,
    pub host_ref: String,
    pub contract_ref: String,
    pub target: ContractTarget,
    #[serde(default)]
    pub contributions: Vec<HostFabricMemberContribution>,
    #[serde(default)]
    pub lifecycle_plans: Vec<LifecyclePlanPosture>,
    #[serde(default)]
    pub materialization_budget_refs: Vec<String>,
    pub association_handoff_ref: Option<String>,
    pub observed_at: u64,
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContractTargetRegistryReduction {
    pub registry: ContractTargetRegistryPosture,
    pub fulfillment_plan: HostFabricFulfillmentPlan,
    pub selected_gateway_ref: Option<String>,
    pub candidate_gateway_refs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CarrierEdgeCandidate {
    pub adapter_ref: String,
    pub adapter_kind: String,
    #[serde(default)]
    pub contribution_ref: Option<String>,
    #[serde(default)]
    pub session_binding_ref: Option<String>,
    #[serde(default)]
    pub network_sensitivity: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub proof_substrate_refs: Vec<String>,
    #[serde(default)]
    pub resource_posture_refs: Vec<String>,
    #[serde(default)]
    pub blocked_reasons: Vec<String>,
    pub state: String,
    #[serde(default)]
    pub priority: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CarrierEdgeSelectionInput {
    pub requirement_id: String,
    pub selection_id: String,
    pub subject_ref: String,
    pub fabric_ref: String,
    pub host_ref: String,
    #[serde(default)]
    pub source_ref: Option<String>,
    #[serde(default)]
    pub consumer_ref: Option<String>,
    #[serde(default)]
    pub route_association_ref: Option<String>,
    #[serde(default)]
    pub policy_ref: Option<String>,
    #[serde(default)]
    pub required_capability_refs: Vec<String>,
    #[serde(default)]
    pub candidates: Vec<CarrierEdgeCandidate>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub observed_at: u64,
    #[serde(default)]
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CarrierEdgeSelectionReduction {
    pub requirement: CarrierEdgeRequirement,
    pub selection: CarrierEdgeSelection,
    pub candidate_adapter_refs: Vec<String>,
    pub selected_adapter_ref: Option<String>,
    pub blocked_reasons: Vec<String>,
}

pub fn reduce_contract_target_registry_from_fabric(
    input: ContractTargetRegistryReductionInput,
) -> Result<ContractTargetRegistryReduction> {
    validate_contract_target(&input.target)?;
    let slot_specs = [
        (
            "slot:gateway-association",
            FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION,
        ),
        (
            "slot:service-edge-adapter",
            FABRIC_MEMBER_ROLE_SERVICE_EDGE_ADAPTER,
        ),
        ("slot:platform-adapter", FABRIC_MEMBER_ROLE_PLATFORM_ADAPTER),
        ("slot:runtime", FABRIC_MEMBER_ROLE_RUNTIME),
        ("slot:nvr-service", FABRIC_MEMBER_ROLE_DOMAIN_SERVICE),
    ];
    let mut slot_postures = Vec::new();
    let mut candidate_fulfillment_refs = Vec::new();
    let mut blocked_reasons = BTreeSet::new();

    for (slot_ref, role) in slot_specs {
        let candidates = contribution_outputs_for_role(&input.contributions, role);
        let evidence_refs = contribution_evidence_for_role(&input.contributions, role);
        let slot_blockers = if candidates.is_empty() {
            vec![format!("targetSlot:missing:{slot_ref}")]
        } else {
            Vec::new()
        };
        blocked_reasons.extend(slot_blockers.iter().cloned());
        candidate_fulfillment_refs.extend(candidates.clone());
        slot_postures.push(ContractTargetSlotPosture {
            slot_ref: slot_ref.to_string(),
            state: if candidates.is_empty() {
                FABRIC_CONTRACT_TARGET_SLOT_MISSING
            } else {
                FABRIC_CONTRACT_TARGET_SLOT_AVAILABLE
            }
            .to_string(),
            platform_fit_state: if candidates.is_empty() {
                FABRIC_CONTRACT_TARGET_PLATFORM_FIT_UNKNOWN
            } else {
                FABRIC_CONTRACT_TARGET_PLATFORM_FIT_COMPATIBLE
            }
            .to_string(),
            candidate_fulfillment_refs: candidates.clone(),
            selected_fulfillment_ref: candidates.first().cloned(),
            source_refs: Vec::new(),
            build_refs: Vec::new(),
            platform_refs: Vec::new(),
            adapter_refs: Vec::new(),
            proof_requirement_refs: Vec::new(),
            proof_refs: Vec::new(),
            evidence_refs,
            blocked_reasons: slot_blockers,
            safe_facts: json!({ "role": role, "reducedBy": "host-fabric" }),
        });
    }

    let blocked_reasons = normalize_refs(blocked_reasons.into_iter().collect());
    let gateway_candidates =
        contribution_outputs_for_role(&input.contributions, FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION);
    let service_edge_candidates = contribution_outputs_for_role(
        &input.contributions,
        FABRIC_MEMBER_ROLE_SERVICE_EDGE_ADAPTER,
    );
    let registry = ContractTargetRegistryPosture {
        kind: Some(RECORD_CONTRACT_TARGET_REGISTRY_POSTURE.to_string()),
        registry_ref: contract_target_registry_ref(&input.target.target_ref),
        target_ref: input.target.target_ref.clone(),
        contract_ref: input.target.contract_ref.clone(),
        state: if blocked_reasons.is_empty() {
            FABRIC_CONTRACT_TARGET_REGISTRY_READY
        } else {
            FABRIC_CONTRACT_TARGET_REGISTRY_BLOCKED
        }
        .to_string(),
        slot_postures,
        candidate_fulfillment_refs: unique_preserve_order(candidate_fulfillment_refs),
        source_refs: input.target.capability_slot_refs.clone(),
        build_refs: input.target.proof_refs.clone(),
        adapter_refs: input.target.adapter_refs.clone(),
        proof_requirement_refs: vec![
            "proof-requirement:candidate-reduction".to_string(),
            "proof-requirement:selected-fulfillment".to_string(),
            "proof-requirement:no-service-identity-mutation".to_string(),
        ],
        proof_refs: input.target.proof_refs.clone(),
        evidence_refs: normalize_refs(
            input
                .target
                .evidence_refs
                .iter()
                .cloned()
                .chain(["evidence:target-registry:fabric-reduction".to_string()])
                .collect(),
        ),
        blocked_reasons: blocked_reasons.clone(),
        safe_facts: json!({
            "gatewayCandidates": gateway_candidates.len(),
            "serviceEdgeCandidates": service_edge_candidates.len(),
            "selectedGateway": gateway_candidates.first().cloned()
        }),
        observed_at: input.observed_at,
        expires_at: input.expires_at,
    };
    validate_contract_target_registry_posture(&registry)?;

    let fulfillment_plan = reduce_host_fabric(HostFabricReductionInput {
        plan_id: input.plan_id,
        fabric_ref: input.fabric_ref,
        host_ref: input.host_ref,
        contract_ref: input.contract_ref,
        required_roles: slot_specs
            .iter()
            .map(|(_, role)| HostFabricRoleRequirement {
                role_ref: role_ref(role),
                min_ready: 1,
            })
            .collect(),
        contributions: input.contributions.clone(),
        lifecycle_plans: input.lifecycle_plans,
        materialization_budget_refs: input.materialization_budget_refs,
        known_missing_role_refs: registry
            .slot_postures
            .iter()
            .filter(|slot| slot.state == FABRIC_CONTRACT_TARGET_SLOT_MISSING)
            .map(|slot| role_ref_for_slot(&slot.slot_ref))
            .collect(),
        evidence_refs: registry.evidence_refs.clone(),
        blocked_reasons,
        association_handoff_ref: input.association_handoff_ref,
        observed_at: input.observed_at,
        expires_at: input.expires_at,
    })?
    .fulfillment_plan;
    let candidate_gateway_refs = contribution_outputs_for_role_from_slots(
        &registry.slot_postures,
        "slot:gateway-association",
    );
    Ok(ContractTargetRegistryReduction {
        selected_gateway_ref: registry
            .slot_postures
            .iter()
            .find(|slot| slot.slot_ref == "slot:gateway-association")
            .and_then(|slot| slot.selected_fulfillment_ref.clone()),
        candidate_gateway_refs,
        registry,
        fulfillment_plan,
    })
}

pub fn reduce_carrier_edge_selection_from_fabric(
    input: CarrierEdgeSelectionInput,
) -> Result<CarrierEdgeSelectionReduction> {
    require_ref(&input.requirement_id, "requirementId")?;
    require_ref(&input.selection_id, "selectionId")?;
    require_ref(&input.subject_ref, "subjectRef")?;
    require_ref(&input.fabric_ref, "fabricRef")?;
    require_ref(&input.host_ref, "hostRef")?;
    if input
        .expires_at
        .is_some_and(|expires_at| expires_at <= input.observed_at)
    {
        return Err(anyhow!(
            "carrier edge selection expiresAt must be after observedAt"
        ));
    }

    let mut candidates = input.candidates.clone();
    candidates.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.adapter_ref.cmp(&right.adapter_ref))
    });
    let candidate_adapter_refs = unique_preserve_order(
        candidates
            .iter()
            .map(|candidate| candidate.adapter_ref.clone())
            .collect(),
    );
    let allowed_adapter_kinds = unique_preserve_order(
        candidates
            .iter()
            .map(|candidate| candidate.adapter_kind.clone())
            .collect(),
    );
    let mut evidence_refs = input.evidence_refs.clone();
    let mut proof_substrate_refs = Vec::new();
    let mut resource_posture_refs = Vec::new();
    let mut blocked_reasons = BTreeSet::new();
    let mut fallback_refs = Vec::new();
    let mut selected_candidate: Option<CarrierEdgeCandidate> = None;
    let mut degraded_candidate: Option<CarrierEdgeCandidate> = None;

    for candidate in &candidates {
        require_ref(&candidate.adapter_ref, "candidate adapterRef")?;
        require_ref(&candidate.adapter_kind, "candidate adapterKind")?;
        if !matches!(
            candidate.adapter_kind.as_str(),
            CARRIER_EDGE_ADAPTER_WEB_SOCKET
                | CARRIER_EDGE_ADAPTER_QUIC
                | CARRIER_EDGE_ADAPTER_IPC
                | CARRIER_EDGE_ADAPTER_WORKER
                | CARRIER_EDGE_ADAPTER_LOOPBACK
                | CARRIER_EDGE_ADAPTER_RELAY
                | CARRIER_EDGE_ADAPTER_NAMED_PIPE
        ) {
            return Err(anyhow!("unsupported carrier edge candidate adapterKind"));
        }
        if let Some(session_binding_ref) = &candidate.session_binding_ref {
            require_ref(session_binding_ref, "candidate sessionBindingRef")?;
        }
        if let Some(network_sensitivity) = &candidate.network_sensitivity {
            validate_carrier_edge_network_sensitivity(network_sensitivity)?;
        }
        evidence_refs.extend(candidate.evidence_refs.clone());
        proof_substrate_refs.extend(candidate.proof_substrate_refs.clone());
        resource_posture_refs.extend(candidate.resource_posture_refs.clone());
        match candidate.state.as_str() {
            "actionable" => {
                if selected_candidate.is_none() {
                    selected_candidate = Some(candidate.clone());
                } else {
                    fallback_refs.push(candidate.adapter_ref.clone());
                }
            }
            "degraded" => {
                if degraded_candidate.is_none() {
                    degraded_candidate = Some(candidate.clone());
                } else {
                    fallback_refs.push(candidate.adapter_ref.clone());
                }
                blocked_reasons.extend(candidate.blocked_reasons.clone());
            }
            "blocked" | "released" | "expired" => {
                blocked_reasons.extend(candidate.blocked_reasons.clone());
                blocked_reasons.insert(format!(
                    "carrierEdge:candidate:{}:{}",
                    candidate.state, candidate.adapter_ref
                ));
            }
            _ => return Err(anyhow!("unsupported carrier edge candidate state")),
        }
    }

    let selected = selected_candidate.or(degraded_candidate);
    let state = if let Some(candidate) = selected.as_ref() {
        if candidate.state == "degraded" {
            CARRIER_EDGE_SELECTION_DEGRADED
        } else {
            CARRIER_EDGE_SELECTION_ACTIONABLE
        }
    } else {
        blocked_reasons.insert("carrierEdge:noActionableAdapter".to_string());
        CARRIER_EDGE_SELECTION_BLOCKED
    };
    let requirement_state = if selected.is_some() {
        CARRIER_EDGE_REQUIREMENT_ACTIONABLE
    } else {
        CARRIER_EDGE_REQUIREMENT_BLOCKED
    };
    let backpressure_state = if state == CARRIER_EDGE_SELECTION_BLOCKED {
        CARRIER_EDGE_BACKPRESSURE_BLOCKED
    } else if state == CARRIER_EDGE_SELECTION_DEGRADED {
        CARRIER_EDGE_BACKPRESSURE_DEGRADED
    } else {
        CARRIER_EDGE_BACKPRESSURE_CLEAR
    };
    let blocked_reasons = normalize_refs(blocked_reasons.into_iter().collect());
    let evidence_refs = normalize_refs(evidence_refs);
    let proof_substrate_refs = normalize_refs(proof_substrate_refs);
    let resource_posture_refs = normalize_refs(resource_posture_refs);
    let selected_adapter_ref = selected
        .as_ref()
        .map(|candidate| candidate.adapter_ref.clone());
    let selected_adapter_kind = selected
        .as_ref()
        .map(|candidate| candidate.adapter_kind.clone())
        .unwrap_or_else(|| {
            allowed_adapter_kinds
                .first()
                .cloned()
                .unwrap_or_else(|| CARRIER_EDGE_ADAPTER_WEB_SOCKET.to_string())
        });
    let network_sensitivity = selected
        .as_ref()
        .and_then(|candidate| candidate.network_sensitivity.clone())
        .unwrap_or_else(|| carrier_edge_network_sensitivity(&selected_adapter_kind).to_string());
    let session_binding_ref = selected
        .as_ref()
        .and_then(|candidate| candidate.session_binding_ref.clone())
        .unwrap_or_else(|| format!("carrier-binding:{}", input.selection_id));
    let selected_proof_substrate_refs = selected
        .as_ref()
        .map(|candidate| normalize_refs(candidate.proof_substrate_refs.clone()))
        .unwrap_or_default();
    let selected_resource_posture_refs = selected
        .as_ref()
        .map(|candidate| normalize_refs(candidate.resource_posture_refs.clone()))
        .unwrap_or_default();

    let requirement = CarrierEdgeRequirement {
        kind: Some(RECORD_CARRIER_EDGE_REQUIREMENT.to_string()),
        requirement_id: input.requirement_id.clone(),
        subject_ref: input.subject_ref.clone(),
        source_ref: input.source_ref.clone(),
        consumer_ref: input.consumer_ref.clone(),
        fabric_ref: Some(input.fabric_ref.clone()),
        host_ref: Some(input.host_ref.clone()),
        route_association_ref: input.route_association_ref.clone(),
        required_capability_refs: normalize_refs(input.required_capability_refs.clone()),
        allowed_adapter_kinds: if allowed_adapter_kinds.is_empty() {
            vec![
                CARRIER_EDGE_ADAPTER_WEB_SOCKET.to_string(),
                CARRIER_EDGE_ADAPTER_QUIC.to_string(),
            ]
        } else {
            allowed_adapter_kinds.clone()
        },
        candidate_adapter_refs: candidate_adapter_refs.clone(),
        policy_ref: input.policy_ref.clone(),
        network_sensitivity: Some(network_sensitivity.clone()),
        state: requirement_state.to_string(),
        safe_facts: json!({
            "reducer": "host-fabric-carrier-edge",
            "candidateCount": candidate_adapter_refs.len(),
            "selectedAdapterKind": selected_adapter_kind
        }),
        evidence_refs: evidence_refs.clone(),
        proof_substrate_refs,
        resource_posture_refs,
        blocked_reasons: blocked_reasons.clone(),
        issued_at: input.observed_at,
        expires_at: input.expires_at,
    };
    validate_carrier_edge_requirement(&requirement)?;

    let selection = CarrierEdgeSelection {
        kind: Some(RECORD_CARRIER_EDGE_SELECTION.to_string()),
        selection_id: input.selection_id.clone(),
        requirement_ref: input.requirement_id.clone(),
        fabric_ref: Some(input.fabric_ref),
        host_ref: Some(input.host_ref),
        adapter_kind: selected_adapter_kind,
        selected_adapter_ref: selected_adapter_ref.clone(),
        candidate_adapter_refs: candidate_adapter_refs.clone(),
        fallback_refs: normalize_refs(fallback_refs),
        selector_ref: Some("selector:host-fabric-carrier-edge".to_string()),
        session_binding_ref: Some(session_binding_ref),
        network_sensitivity: Some(network_sensitivity),
        state: state.to_string(),
        backpressure_state: Some(backpressure_state.to_string()),
        safe_facts: json!({
            "reducer": "host-fabric-carrier-edge",
            "candidateCount": candidate_adapter_refs.len()
        }),
        evidence_refs,
        proof_substrate_refs: selected_proof_substrate_refs,
        resource_posture_refs: selected_resource_posture_refs,
        blocked_reasons: blocked_reasons.clone(),
        observed_at: input.observed_at,
        expires_at: input.expires_at,
    };
    validate_carrier_edge_selection(&selection)?;

    Ok(CarrierEdgeSelectionReduction {
        requirement,
        selection,
        candidate_adapter_refs,
        selected_adapter_ref,
        blocked_reasons,
    })
}

fn contribution_outputs_for_role(
    contributions: &[HostFabricMemberContribution],
    role: &str,
) -> Vec<String> {
    unique_preserve_order(
        contributions
            .iter()
            .filter(|contribution| {
                contribution.role == role && is_ready_contribution(&contribution.state)
            })
            .flat_map(|contribution| {
                if contribution.output_refs.is_empty() {
                    vec![contribution.contribution_id.clone()]
                } else {
                    contribution.output_refs.clone()
                }
            })
            .collect(),
    )
}

fn contribution_evidence_for_role(
    contributions: &[HostFabricMemberContribution],
    role: &str,
) -> Vec<String> {
    unique_preserve_order(
        contributions
            .iter()
            .filter(|contribution| contribution.role == role)
            .flat_map(|contribution| contribution.evidence_refs.clone())
            .collect(),
    )
}

fn contribution_outputs_for_role_from_slots(
    slots: &[ContractTargetSlotPosture],
    slot_ref: &str,
) -> Vec<String> {
    slots
        .iter()
        .find(|slot| slot.slot_ref == slot_ref)
        .map(|slot| slot.candidate_fulfillment_refs.clone())
        .unwrap_or_default()
}

fn role_ref_for_slot(slot_ref: &str) -> String {
    match slot_ref {
        "slot:gateway-association" => role_ref(FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION),
        "slot:service-edge-adapter" => role_ref(FABRIC_MEMBER_ROLE_SERVICE_EDGE_ADAPTER),
        "slot:platform-adapter" => role_ref(FABRIC_MEMBER_ROLE_PLATFORM_ADAPTER),
        "slot:runtime" => role_ref(FABRIC_MEMBER_ROLE_RUNTIME),
        "slot:nvr-service" => role_ref(FABRIC_MEMBER_ROLE_DOMAIN_SERVICE),
        _ => slot_ref.to_string(),
    }
}

fn contract_target_registry_ref(target_ref: &str) -> String {
    target_ref
        .strip_prefix("contract-target:")
        .map(|tail| format!("contract-target-registry:{tail}"))
        .unwrap_or_else(|| format!("contract-target-registry:{target_ref}"))
}

fn unique_preserve_order(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for value in values {
        let value = value.trim().to_string();
        if value.is_empty() || !seen.insert(value.clone()) {
            continue;
        }
        out.push(value);
    }
    out
}

pub fn reduce_host_fabric_shadow_parity(
    input: HostFabricShadowParityInput,
) -> Result<HostFabricShadowParity> {
    let mut reduction_input = input.reduction;
    let legacy_ready_role_refs = normalize_role_refs(input.legacy_ready_role_refs);
    let legacy_blocked_role_refs = normalize_role_refs(input.legacy_blocked_role_refs);
    let required_min_ready = reduction_input
        .required_roles
        .iter()
        .map(|role| (role_ref(&role.role_ref), role.min_ready))
        .collect::<BTreeMap<_, _>>();
    let mut ready_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut usable_counts: BTreeMap<String, usize> = BTreeMap::new();

    for contribution in &reduction_input.contributions {
        validate_host_fabric_member_contribution(contribution)?;
        if contribution.fabric_ref != reduction_input.fabric_ref
            || contribution.host_ref != reduction_input.host_ref
        {
            continue;
        }
        let role = role_ref(&contribution.role);
        if is_ready_contribution(&contribution.state) {
            *ready_counts.entry(role.clone()).or_default() += 1;
            *usable_counts.entry(role).or_default() += 1;
        } else if is_degraded_contribution(&contribution.state) {
            *usable_counts.entry(role).or_default() += 1;
        }
    }

    let mut agreement_role_refs = BTreeSet::new();
    let mut disagreement_role_refs = BTreeSet::new();
    let mut parity_blockers = BTreeSet::new();

    for role in &legacy_ready_role_refs {
        let Some(min_ready) = required_min_ready.get(role).copied() else {
            disagreement_role_refs.insert(role.clone());
            parity_blockers.insert(format!(
                "hostFabric:legacyDisagreement:unmodeledRole:{role}"
            ));
            continue;
        };
        let ready_count = ready_counts.get(role).copied().unwrap_or_default();
        let usable_count = usable_counts.get(role).copied().unwrap_or_default();
        if ready_count >= min_ready {
            agreement_role_refs.insert(role.clone());
        } else {
            disagreement_role_refs.insert(role.clone());
            let posture = if usable_count >= min_ready {
                "degradedRole"
            } else {
                "missingRole"
            };
            parity_blockers.insert(format!("hostFabric:legacyDisagreement:{posture}:{role}"));
        }
    }

    for role in &legacy_blocked_role_refs {
        if ready_counts.get(role).copied().unwrap_or_default() > 0 {
            disagreement_role_refs.insert(role.clone());
            parity_blockers.insert(format!(
                "hostFabric:legacyDisagreement:legacyBlockedFabricReady:{role}"
            ));
        } else {
            agreement_role_refs.insert(role.clone());
        }
    }

    reduction_input
        .blocked_reasons
        .extend(parity_blockers.iter().cloned());
    let reduction = reduce_host_fabric(reduction_input)?;
    let blocked_reasons = normalize_refs(
        reduction
            .blocked_reasons
            .iter()
            .cloned()
            .chain(parity_blockers)
            .collect(),
    );

    Ok(HostFabricShadowParity {
        reduction,
        agreement_role_refs: agreement_role_refs.into_iter().collect(),
        disagreement_role_refs: disagreement_role_refs.into_iter().collect(),
        blocked_reasons,
    })
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
    let mut dependency_edge_refs = BTreeSet::new();
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

    let mut missing_role_refs =
        BTreeSet::from_iter(normalize_refs(input.known_missing_role_refs.clone()));
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
        for edge in &lifecycle_plan.dependency_edges {
            dependency_edge_refs.insert(edge.dependency_ref.clone());
            if matches!(
                edge.state.as_str(),
                FABRIC_LIFECYCLE_DEPENDENCY_BLOCKED | FABRIC_LIFECYCLE_DEPENDENCY_MISSING
            ) {
                blocked_reasons.extend(edge.blocked_reasons.clone());
                blocked_reasons.insert(format!(
                    "hostFabric:lifecycleDependency:{}:{}",
                    edge.state, edge.dependency_ref
                ));
            } else if edge.state == FABRIC_LIFECYCLE_DEPENDENCY_DEGRADED {
                degraded_roles.insert(format!("dependency:{}", edge.dependency_ref));
            }
        }
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

    let plan_id = input.plan_id.clone();
    let fabric_ref = input.fabric_ref.clone();
    let host_ref = input.host_ref.clone();
    let contract_ref = input.contract_ref.clone();
    let association_handoff_ref = input.association_handoff_ref.clone();
    let observed_at = input.observed_at;
    let expires_at = input.expires_at;
    let member_contribution_refs = usable_by_role
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let missing_role_refs = missing_role_refs.into_iter().collect::<Vec<_>>();
    let blocked_reasons = blocked_reasons.into_iter().collect::<Vec<_>>();
    let dependency_edge_refs = dependency_edge_refs.into_iter().collect::<Vec<_>>();
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
        normalize_refs(input.materialization_budget_refs.clone())
    };
    let mut evidence_refs = normalize_refs(input.evidence_refs.clone());
    evidence_refs.push(format!("evidence:host-fabric-reduction:{plan_id}"));
    evidence_refs = normalize_refs(evidence_refs);

    let fulfillment_plan = HostFabricFulfillmentPlan {
        kind: Some(RECORD_HOST_FABRIC_FULFILLMENT_PLAN.to_string()),
        plan_id: plan_id.clone(),
        fabric_ref: fabric_ref.clone(),
        host_ref: host_ref.clone(),
        contract_ref: contract_ref.clone(),
        state: state.to_string(),
        required_role_refs: required_role_refs.clone(),
        member_contribution_refs: member_contribution_refs.clone(),
        missing_role_refs: missing_role_refs.clone(),
        lifecycle_plan_refs: lifecycle_plan_refs.clone(),
        materialization_budget_refs: materialization_budget_refs.clone(),
        action_authority_refs: vec![format!("authority:host-fabric:{contract_ref}")],
        delegated_role_refs: required_role_refs.clone(),
        fallback_refs: vec![format!("fallback:host-fabric:{plan_id}")],
        quarantine_refs: vec![format!("quarantine:host-fabric:{plan_id}")],
        rollback_refs: vec![format!("rollback:host-fabric:{plan_id}")],
        evidence_requirement_refs: required_role_refs
            .iter()
            .map(|role_ref| format!("proof-requirement:host-fabric:{role_ref}"))
            .collect(),
        association_handoff_ref: association_handoff_ref.clone(),
        evidence_refs: evidence_refs.clone(),
        blocked_reasons: blocked_reasons.clone(),
        safe_facts: json!({
            "reducer": "host-fabric",
            "state": state,
            "readyContributionCount": ready_contribution_refs.len(),
            "degradedContributionCount": degraded_contribution_refs.len(),
            "blockedContributionCount": blocked_contribution_refs.len(),
            "filteredContributionCount": filtered_contribution_refs.len(),
            "dependencyEdgeCount": dependency_edge_refs.len()
        }),
        observed_at,
        expires_at,
    };
    validate_host_fabric_fulfillment_plan(&fulfillment_plan)?;
    let topology_projection = reduce_host_fabric_topology_projection(
        &input,
        &fulfillment_plan,
        state,
        &required_role_refs,
        &missing_role_refs,
        &materialization_budget_refs,
        &evidence_refs,
    )?;

    Ok(HostFabricReduction {
        fulfillment_plan,
        topology_projection,
        ready_contribution_refs: ready_contribution_refs.into_iter().collect(),
        degraded_contribution_refs: degraded_contribution_refs.into_iter().collect(),
        blocked_contribution_refs: blocked_contribution_refs.into_iter().collect(),
        filtered_contribution_refs: filtered_contribution_refs.into_iter().collect(),
        lifecycle_plan_refs,
        dependency_edge_refs,
        blocked_reasons,
    })
}

pub fn reduce_host_fabric_control_decision(
    plan: &HostFabricFulfillmentPlan,
    input: HostFabricControlDecisionInput,
) -> Result<HostFabricControlDecision> {
    validate_host_fabric_fulfillment_plan(plan)?;
    require_ref(&input.decision_id, "decisionId")?;
    require_ref(&input.operation_ref, "operationRef")?;
    require_ref(&input.subject_ref, "subjectRef")?;

    let input_blocked_reasons = input.blocked_reasons;
    let state = if input_blocked_reasons.is_empty() {
        match plan.state.as_str() {
            FABRIC_FULFILLMENT_PLAN_READY => FABRIC_CONTROL_DECISION_READY,
            FABRIC_FULFILLMENT_PLAN_DEGRADED => FABRIC_CONTROL_DECISION_DEGRADED,
            FABRIC_FULFILLMENT_PLAN_BLOCKED => FABRIC_CONTROL_DECISION_BLOCKED,
            FABRIC_FULFILLMENT_PLAN_EXPIRED => FABRIC_CONTROL_DECISION_EXPIRED,
            _ => FABRIC_CONTROL_DECISION_BLOCKED,
        }
    } else {
        FABRIC_CONTROL_DECISION_BLOCKED
    };
    let delegated_role_ref = input
        .delegated_role_ref
        .or_else(|| plan.delegated_role_refs.first().cloned());
    let mut authorization_refs = input.authorization_refs;
    if authorization_refs.is_empty() && state == FABRIC_CONTROL_DECISION_READY {
        authorization_refs.push(format!(
            "authorization:host-fabric:{}:{}",
            plan.plan_id, input.operation_ref
        ));
    }
    let mut blocked_reasons = BTreeSet::from_iter(
        input_blocked_reasons
            .into_iter()
            .chain(plan.blocked_reasons.clone()),
    );
    if state == FABRIC_CONTROL_DECISION_DEGRADED && blocked_reasons.is_empty() {
        blocked_reasons.insert(format!("hostFabric:plan:degraded:{}", plan.plan_id));
    }
    if state == FABRIC_CONTROL_DECISION_BLOCKED && blocked_reasons.is_empty() {
        blocked_reasons.insert(format!("hostFabric:plan:blocked:{}", plan.plan_id));
    }
    if state == FABRIC_CONTROL_DECISION_EXPIRED && blocked_reasons.is_empty() {
        blocked_reasons.insert(format!("hostFabric:plan:expired:{}", plan.plan_id));
    }
    let mut evidence_refs = input.evidence_refs;
    evidence_refs.extend(plan.evidence_refs.clone());
    evidence_refs.push(format!(
        "evidence:host-fabric-control:{}",
        input.decision_id
    ));
    let mut safe_facts = match input.safe_facts {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    safe_facts.insert("reducer".to_string(), json!("host-fabric"));
    safe_facts.insert("sourcePlanRef".to_string(), json!(plan.plan_id));
    safe_facts.insert("planState".to_string(), json!(plan.state));

    let decision = HostFabricControlDecision {
        kind: Some(RECORD_HOST_FABRIC_CONTROL_DECISION.to_string()),
        decision_id: input.decision_id,
        fabric_ref: plan.fabric_ref.clone(),
        host_ref: plan.host_ref.clone(),
        operation_ref: input.operation_ref,
        subject_ref: input.subject_ref,
        control_owner_ref: input
            .control_owner_ref
            .unwrap_or_else(|| plan.fabric_ref.clone()),
        delegated_role_ref,
        state: state.to_string(),
        source_plan_ref: Some(plan.plan_id.clone()),
        source_plan_observed_at: Some(plan.observed_at),
        source_plan_expires_at: plan.expires_at,
        plan_state: Some(plan.state.clone()),
        execution_delegation_ref: input.execution_delegation_ref,
        authorization_refs: normalize_refs(authorization_refs),
        fallback_refs: if input.fallback_refs.is_empty() {
            plan.fallback_refs.clone()
        } else {
            normalize_refs(input.fallback_refs)
        },
        quarantine_refs: if input.quarantine_refs.is_empty() {
            plan.quarantine_refs.clone()
        } else {
            normalize_refs(input.quarantine_refs)
        },
        rollback_ref: input
            .rollback_ref
            .or_else(|| plan.rollback_refs.first().cloned()),
        release_refs: normalize_refs(input.release_refs),
        blocked_reasons: blocked_reasons.into_iter().collect(),
        evidence_refs: normalize_refs(evidence_refs),
        safe_facts: Value::Object(safe_facts),
        observed_at: input.observed_at,
        expires_at: input.expires_at,
    };
    validate_host_fabric_control_decision(&decision)?;
    Ok(decision)
}

pub fn reduce_host_fabric_adapter_execution_evidence(
    plan: &HostFabricFulfillmentPlan,
    decision: &HostFabricControlDecision,
    input: HostFabricAdapterExecutionInput,
) -> Result<HostFabricAdapterExecutionEvidence> {
    validate_host_fabric_fulfillment_plan(plan)?;
    validate_host_fabric_control_decision(decision)?;
    require_ref(&input.evidence_id, "evidenceId")?;
    require_ref(&input.adapter_ref, "adapterRef")?;
    require_ref(&input.state, "state")?;

    let mut blocked_reasons = BTreeSet::from_iter(input.blocked_reasons);
    if matches!(
        input.state.as_str(),
        FABRIC_ADAPTER_EXECUTION_BLOCKED | FABRIC_ADAPTER_EXECUTION_FAILED
    ) && blocked_reasons.is_empty()
    {
        blocked_reasons.insert(format!(
            "hostFabric:adapterExecution:{}:{}",
            input.state, input.evidence_id
        ));
    }
    let mut evidence_refs = input.evidence_refs;
    evidence_refs.extend(decision.evidence_refs.clone());
    evidence_refs.push(format!(
        "evidence:host-fabric-adapter-execution:{}",
        input.evidence_id
    ));

    let evidence = HostFabricAdapterExecutionEvidence {
        kind: Some(RECORD_HOST_FABRIC_ADAPTER_EXECUTION_EVIDENCE.to_string()),
        evidence_id: input.evidence_id,
        fabric_ref: plan.fabric_ref.clone(),
        host_ref: plan.host_ref.clone(),
        adapter_ref: input.adapter_ref,
        subject_ref: decision.subject_ref.clone(),
        operation_ref: decision.operation_ref.clone(),
        state: input.state,
        source_decision_ref: Some(decision.decision_id.clone()),
        source_plan_ref: decision.source_plan_ref.clone(),
        source_plan_observed_at: decision.source_plan_ref.as_ref().map(|_| plan.observed_at),
        source_plan_expires_at: decision
            .source_plan_ref
            .as_ref()
            .and_then(|_| plan.expires_at),
        source_bridge_ref: input.source_bridge_ref,
        delegated_role_ref: input
            .delegated_role_ref
            .or_else(|| decision.delegated_role_ref.clone()),
        authorization_refs: decision.authorization_refs.clone(),
        action_authority_refs: if input.action_authority_refs.is_empty() {
            plan.action_authority_refs.clone()
        } else {
            normalize_refs(input.action_authority_refs)
        },
        evidence_requirement_refs: if input.evidence_requirement_refs.is_empty() {
            plan.evidence_requirement_refs.clone()
        } else {
            normalize_refs(input.evidence_requirement_refs)
        },
        input_refs: normalize_refs(input.input_refs),
        output_refs: normalize_refs(input.output_refs),
        fallback_refs: if input.fallback_refs.is_empty() {
            decision.fallback_refs.clone()
        } else {
            normalize_refs(input.fallback_refs)
        },
        quarantine_refs: if input.quarantine_refs.is_empty() {
            decision.quarantine_refs.clone()
        } else {
            normalize_refs(input.quarantine_refs)
        },
        rollback_refs: if input.rollback_refs.is_empty() {
            plan.rollback_refs.clone()
        } else {
            normalize_refs(input.rollback_refs)
        },
        release_refs: if input.release_refs.is_empty() {
            decision.release_refs.clone()
        } else {
            normalize_refs(input.release_refs)
        },
        cleanup_refs: normalize_refs(input.cleanup_refs),
        blocked_reasons: blocked_reasons.into_iter().collect(),
        evidence_refs: normalize_refs(evidence_refs),
        safe_facts: input.safe_facts,
        observed_at: input.observed_at,
        expires_at: input.expires_at,
    };
    validate_host_fabric_adapter_execution_evidence(&evidence)?;
    Ok(evidence)
}

fn reduce_host_fabric_topology_projection(
    input: &HostFabricReductionInput,
    fulfillment_plan: &HostFabricFulfillmentPlan,
    state: &str,
    required_role_refs: &[String],
    missing_role_refs: &[String],
    materialization_budget_refs: &[String],
    evidence_refs: &[String],
) -> Result<HostFabricTopologyProjection> {
    let relevant_contributions = input
        .contributions
        .iter()
        .filter(|contribution| {
            contribution.fabric_ref == input.fabric_ref && contribution.host_ref == input.host_ref
        })
        .collect::<Vec<_>>();
    let mut role_refs = BTreeSet::from_iter(required_role_refs.iter().cloned());
    for contribution in &relevant_contributions {
        role_refs.insert(role_ref(&contribution.role));
    }

    let mut ready_role_refs = BTreeSet::new();
    let mut degraded_role_refs = BTreeSet::new();
    let mut blocked_role_refs = BTreeSet::new();
    let mut missing_role_refs_set = BTreeSet::from_iter(missing_role_refs.iter().cloned());
    let mut member_contribution_refs = BTreeSet::new();
    let mut participant_refs = BTreeSet::new();
    let mut module_refs = BTreeSet::new();
    let mut source_refs = BTreeSet::new();
    let mut lifecycle_plan_refs = BTreeSet::new();
    let mut topology_evidence_refs = BTreeSet::from_iter(evidence_refs.iter().cloned());
    let mut role_postures = Vec::new();

    for role in role_refs {
        let contributions = relevant_contributions
            .iter()
            .copied()
            .filter(|contribution| role_ref(&contribution.role) == role)
            .collect::<Vec<_>>();
        let ready = contributions
            .iter()
            .filter(|contribution| is_ready_contribution(&contribution.state))
            .count();
        let degraded = contributions
            .iter()
            .filter(|contribution| is_degraded_contribution(&contribution.state))
            .count();
        let blocked = contributions
            .iter()
            .filter(|contribution| is_blocked_contribution(&contribution.state))
            .count();
        let role_state = if ready > 0 {
            ready_role_refs.insert(role.clone());
            FABRIC_TOPOLOGY_ROLE_READY
        } else if degraded > 0 {
            degraded_role_refs.insert(role.clone());
            FABRIC_TOPOLOGY_ROLE_DEGRADED
        } else if blocked > 0 {
            blocked_role_refs.insert(role.clone());
            FABRIC_TOPOLOGY_ROLE_BLOCKED
        } else {
            missing_role_refs_set.insert(role.clone());
            FABRIC_TOPOLOGY_ROLE_MISSING
        };

        let contribution_refs = contributions
            .iter()
            .map(|contribution| contribution.contribution_id.clone())
            .collect::<Vec<_>>();
        let mut role_participant_refs = Vec::new();
        let mut role_member_refs = Vec::new();
        let mut role_module_refs = Vec::new();
        let mut role_source_refs = Vec::new();
        let mut role_lifecycle_plan_refs = Vec::new();
        let mut role_evidence_refs = Vec::new();
        let mut role_blocked_reasons = Vec::new();
        for contribution in contributions {
            member_contribution_refs.insert(contribution.contribution_id.clone());
            participant_refs.insert(contribution.participant_ref.clone());
            role_participant_refs.push(contribution.participant_ref.clone());
            role_member_refs.push(contribution.member_ref.clone());
            for reference in &contribution.module_refs {
                module_refs.insert(reference.clone());
                role_module_refs.push(reference.clone());
            }
            for reference in &contribution.source_refs {
                source_refs.insert(reference.clone());
                role_source_refs.push(reference.clone());
            }
            for reference in &contribution.lifecycle_plan_refs {
                lifecycle_plan_refs.insert(reference.clone());
                role_lifecycle_plan_refs.push(reference.clone());
            }
            for reference in &contribution.evidence_refs {
                topology_evidence_refs.insert(reference.clone());
                role_evidence_refs.push(reference.clone());
            }
            role_blocked_reasons.extend(contribution.blocked_reasons.clone());
        }
        if role_state == FABRIC_TOPOLOGY_ROLE_MISSING {
            role_blocked_reasons.push(format!("hostFabric:missingRole:{role}"));
        } else if role_state == FABRIC_TOPOLOGY_ROLE_BLOCKED && role_blocked_reasons.is_empty() {
            role_blocked_reasons.push(format!("hostFabric:blockedRole:{role}"));
        }

        role_postures.push(HostFabricTopologyRolePosture {
            role_ref: role,
            state: role_state.to_string(),
            contribution_refs: normalize_refs(contribution_refs),
            participant_refs: normalize_refs(role_participant_refs),
            member_refs: normalize_refs(role_member_refs),
            module_refs: normalize_refs(role_module_refs),
            source_refs: normalize_refs(role_source_refs),
            lifecycle_plan_refs: normalize_refs(role_lifecycle_plan_refs),
            evidence_refs: normalize_refs(role_evidence_refs),
            blocked_reasons: normalize_refs(role_blocked_reasons),
            safe_facts: json!({
                "readyContributionCount": ready,
                "degradedContributionCount": degraded,
                "blockedContributionCount": blocked
            }),
        });
    }

    lifecycle_plan_refs.extend(fulfillment_plan.lifecycle_plan_refs.iter().cloned());
    let projection = HostFabricTopologyProjection {
        kind: Some(RECORD_HOST_FABRIC_TOPOLOGY_PROJECTION.to_string()),
        projection_id: format!("host-fabric-topology:{}", fulfillment_plan.plan_id),
        fabric_ref: input.fabric_ref.clone(),
        host_ref: input.host_ref.clone(),
        contract_ref: input.contract_ref.clone(),
        source_plan_ref: fulfillment_plan.plan_id.clone(),
        state: state.to_string(),
        role_postures,
        required_role_refs: normalize_refs(required_role_refs.to_vec()),
        ready_role_refs: ready_role_refs.into_iter().collect(),
        degraded_role_refs: degraded_role_refs.into_iter().collect(),
        blocked_role_refs: blocked_role_refs.into_iter().collect(),
        missing_role_refs: missing_role_refs_set.into_iter().collect(),
        member_contribution_refs: member_contribution_refs.into_iter().collect(),
        participant_refs: participant_refs.into_iter().collect(),
        module_refs: module_refs.into_iter().collect(),
        source_refs: source_refs.into_iter().collect(),
        lifecycle_plan_refs: lifecycle_plan_refs.into_iter().collect(),
        materialization_budget_refs: materialization_budget_refs.to_vec(),
        association_handoff_ref: input.association_handoff_ref.clone(),
        evidence_refs: topology_evidence_refs.into_iter().collect(),
        blocked_reasons: fulfillment_plan.blocked_reasons.clone(),
        safe_facts: json!({
            "reducer": "host-fabric",
            "sourcePlanRef": fulfillment_plan.plan_id,
            "rolePostureCount": fulfillment_plan.required_role_refs.len()
        }),
        observed_at: input.observed_at,
        expires_at: input.expires_at,
    };
    validate_host_fabric_topology_projection(&projection)?;
    Ok(projection)
}

pub fn build_host_fabric_member_contribution(
    spec: HostFabricMemberContributionSpec,
) -> Result<HostFabricMemberContribution> {
    require_ref(&spec.contribution_id, "contributionId")?;
    require_ref(&spec.fabric_ref, "fabricRef")?;
    require_ref(&spec.host_ref, "hostRef")?;
    require_ref(&spec.member_ref, "memberRef")?;
    require_ref(&spec.participant_ref, "participantRef")?;
    require_ref(&spec.role, "role")?;
    require_ref(&spec.role_ref, "roleRef")?;
    require_ref(&spec.state, "state")?;
    require_ref(&spec.contract_ref, "contractRef")?;
    require_ref(&spec.subject_ref, "subjectRef")?;
    let contribution = HostFabricMemberContribution {
        kind: Some(RECORD_HOST_FABRIC_MEMBER_CONTRIBUTION.to_string()),
        contribution_id: spec.contribution_id,
        fabric_ref: spec.fabric_ref,
        host_ref: spec.host_ref,
        member_ref: spec.member_ref,
        participant_ref: spec.participant_ref,
        role: spec.role,
        role_ref: spec.role_ref,
        state: spec.state,
        contract_ref: spec.contract_ref,
        subject_ref: spec.subject_ref,
        module_refs: normalize_refs(spec.module_refs),
        source_refs: normalize_refs(spec.source_refs),
        capability_refs: normalize_refs(spec.capability_refs),
        grant_refs: normalize_refs(spec.grant_refs),
        input_refs: normalize_refs(spec.input_refs),
        output_refs: normalize_refs(spec.output_refs),
        evidence_refs: normalize_refs(spec.evidence_refs),
        lifecycle_plan_refs: normalize_refs(spec.lifecycle_plan_refs),
        release_refs: normalize_refs(spec.release_refs),
        resource_posture: spec.resource_posture,
        blocked_reasons: normalize_refs(spec.blocked_reasons),
        safe_facts: spec.safe_facts,
        observed_at: spec.observed_at,
        expires_at: spec.expires_at,
    };
    validate_host_fabric_member_contribution(&contribution)?;
    Ok(contribution)
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

fn normalize_role_refs(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| role_ref(&value))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
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

fn carrier_edge_network_sensitivity(adapter_kind: &str) -> &'static str {
    match adapter_kind {
        CARRIER_EDGE_ADAPTER_WORKER => CARRIER_EDGE_NETWORK_PROCESS_LOCAL,
        CARRIER_EDGE_ADAPTER_IPC | CARRIER_EDGE_ADAPTER_NAMED_PIPE => {
            CARRIER_EDGE_NETWORK_HOST_LOCAL
        }
        CARRIER_EDGE_ADAPTER_LOOPBACK => CARRIER_EDGE_NETWORK_LOOPBACK,
        CARRIER_EDGE_ADAPTER_WEB_SOCKET | CARRIER_EDGE_ADAPTER_QUIC => {
            CARRIER_EDGE_NETWORK_LOCAL_NETWORK
        }
        CARRIER_EDGE_ADAPTER_RELAY => CARRIER_EDGE_NETWORK_EXTERNAL_NETWORK,
        _ => CARRIER_EDGE_NETWORK_EXTERNAL_NETWORK,
    }
}

fn validate_carrier_edge_network_sensitivity(value: &str) -> Result<()> {
    if matches!(
        value,
        CARRIER_EDGE_NETWORK_NONE
            | CARRIER_EDGE_NETWORK_PROCESS_LOCAL
            | CARRIER_EDGE_NETWORK_HOST_LOCAL
            | CARRIER_EDGE_NETWORK_LOOPBACK
            | CARRIER_EDGE_NETWORK_LOCAL_NETWORK
            | CARRIER_EDGE_NETWORK_EXTERNAL_NETWORK
    ) {
        Ok(())
    } else {
        Err(anyhow!(
            "unsupported carrier edge candidate networkSensitivity"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constitute_protocol::{
        FABRIC_ADAPTER_EXECUTION_SUCCEEDED, FABRIC_LIFECYCLE_DEPENDENCY_MISSING,
        FABRIC_LIFECYCLE_DEPENDENCY_READY, FABRIC_LIFECYCLE_PHASE_OBSERVE,
        FABRIC_LIFECYCLE_PHASE_READY, FABRIC_LIFECYCLE_PHASE_RUN, FABRIC_LIFECYCLE_PHASE_RUNNING,
        FABRIC_LIFECYCLE_PLAN_READY, FABRIC_MEMBER_ROLE_BUILD_PROCESSOR,
        FABRIC_MEMBER_ROLE_DOMAIN_SERVICE, FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION,
        FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER, FABRIC_MEMBER_ROLE_PLATFORM_ADAPTER,
        FABRIC_MEMBER_ROLE_RUNTIME, FABRIC_MEMBER_ROLE_SERVICE_EDGE_ADAPTER,
        FABRIC_MEMBER_ROLE_SURFACE, LifecycleDependencyEdge, LifecyclePhasePosture,
        RECORD_LIFECYCLE_DEPENDENCY_EDGE, RECORD_LIFECYCLE_PLAN_POSTURE,
    };

    const MEMBER_REF: &str = "4a29ff60c5c3837e9e20555bfeb2a046be3eb140818144628691fcf7efb1d2f1";

    fn contribution(
        id: &str,
        state: &str,
        fabric_ref: &str,
        host_ref: &str,
    ) -> HostFabricMemberContribution {
        contribution_for_role(
            id,
            FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER,
            state,
            fabric_ref,
            host_ref,
        )
    }

    fn contribution_for_role(
        id: &str,
        role: &str,
        state: &str,
        fabric_ref: &str,
        host_ref: &str,
    ) -> HostFabricMemberContribution {
        build_host_fabric_member_contribution(HostFabricMemberContributionSpec {
            contribution_id: id.to_string(),
            fabric_ref: fabric_ref.to_string(),
            host_ref: host_ref.to_string(),
            member_ref: MEMBER_REF.to_string(),
            participant_ref: format!("participant:{role}:test"),
            role: role.to_string(),
            role_ref: format!("role:{role}:test"),
            state: state.to_string(),
            contract_ref: format!("contract:{role}.test@0.1.0"),
            subject_ref: format!("subject:{role}:test"),
            module_refs: vec![format!("module:{role}:test")],
            source_refs: vec![format!("content-index:source:{role}:test")],
            capability_refs: vec![format!("capability:{role}:fulfill")],
            grant_refs: vec![format!("grant:{role}:test")],
            input_refs: vec!["contract-target:desktop-dev:msa-transition".to_string()],
            output_refs: vec![format!("output:{role}:test")],
            evidence_refs: vec![format!("evidence:{id}")],
            lifecycle_plan_refs: vec![format!("lifecycle-plan:{role}:test")],
            release_refs: vec![],
            resource_posture: None,
            blocked_reasons: if state == FABRIC_MEMBER_CONTRIBUTION_BLOCKED {
                vec![format!("{role}:blocked")]
            } else {
                vec![]
            },
            safe_facts: json!({ "fixture": id }),
            observed_at: 1_700_000_000,
            expires_at: Some(1_700_003_600),
        })
        .expect("contribution validates")
    }

    fn contribution_for_role_with_outputs(
        id: &str,
        role: &str,
        output_refs: Vec<&str>,
    ) -> HostFabricMemberContribution {
        build_host_fabric_member_contribution(HostFabricMemberContributionSpec {
            contribution_id: id.to_string(),
            fabric_ref: "fabric:multi-gateway-dev".to_string(),
            host_ref: "host:fabric-dev".to_string(),
            member_ref: MEMBER_REF.to_string(),
            participant_ref: format!("participant:{role}:test"),
            role: role.to_string(),
            role_ref: format!("role:{role}:test"),
            state: FABRIC_MEMBER_CONTRIBUTION_RUNNING.to_string(),
            contract_ref: format!("contract:{role}.test@0.1.0"),
            subject_ref: format!("subject:{role}:test"),
            module_refs: vec![format!("module:{role}:test")],
            source_refs: vec![format!("content-index:source:{role}:test")],
            capability_refs: vec![format!("capability:{role}:fulfill")],
            grant_refs: vec![format!("grant:{role}:test")],
            input_refs: vec!["contract-target:multi-gateway:msa-transition".to_string()],
            output_refs: output_refs.into_iter().map(str::to_string).collect(),
            evidence_refs: vec![format!("evidence:{id}")],
            lifecycle_plan_refs: vec![],
            release_refs: vec![],
            resource_posture: None,
            blocked_reasons: vec![],
            safe_facts: json!({ "fixture": id }),
            observed_at: 1_700_000_000,
            expires_at: Some(1_700_003_600),
        })
        .expect("contribution validates")
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
                    dependency_refs: vec![],
                    evidence_refs: vec!["evidence:run".to_string()],
                    output_refs: vec!["service:lab-managed".to_string()],
                    blocked_reasons: vec![],
                    safe_facts: json!({ "phase": "run" }),
                },
                LifecyclePhasePosture {
                    phase: FABRIC_LIFECYCLE_PHASE_OBSERVE.to_string(),
                    state: FABRIC_LIFECYCLE_PHASE_READY.to_string(),
                    dependency_refs: vec![],
                    evidence_refs: vec!["evidence:observe".to_string()],
                    output_refs: vec!["proof:service:health".to_string()],
                    blocked_reasons: vec![],
                    safe_facts: json!({ "phase": "observe" }),
                },
            ],
            dependency_edges: vec![],
            member_contribution_refs: vec!["fabric-contribution:service-manager".to_string()],
            evidence_refs: vec!["evidence:lifecycle".to_string()],
            release_refs: vec![],
            blocked_reasons: vec![],
            safe_facts: json!({ "fixture": "lifecycle" }),
            observed_at: 1_700_000_000,
            expires_at: Some(1_700_003_600),
        }
    }

    fn lifecycle_with_dependency(dependency_state: &str) -> LifecyclePlanPosture {
        let mut lifecycle = lifecycle(FABRIC_LIFECYCLE_PLAN_READY);
        let dependency_ref = "lifecycle-dependency:runtime:gateway".to_string();
        lifecycle.phase_postures[0].dependency_refs = vec![dependency_ref.clone()];
        lifecycle.dependency_edges = vec![LifecycleDependencyEdge {
            kind: Some(RECORD_LIFECYCLE_DEPENDENCY_EDGE.to_string()),
            dependency_ref,
            source_ref: role_ref(FABRIC_MEMBER_ROLE_RUNTIME),
            target_ref: role_ref(FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION),
            state: dependency_state.to_string(),
            required: true,
            order: Some(10),
            evidence_refs: if dependency_state == FABRIC_LIFECYCLE_DEPENDENCY_READY {
                vec!["evidence:dependency:gateway-ready".to_string()]
            } else {
                vec![]
            },
            blocked_reasons: if dependency_state == FABRIC_LIFECYCLE_DEPENDENCY_MISSING {
                vec!["lifecycleDependency:missing:role:gatewayAssociation".to_string()]
            } else {
                vec![]
            },
            safe_facts: json!({ "dependency": "runtime-needs-gateway" }),
        }];
        lifecycle
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
            lifecycle_plans: vec![lifecycle_with_dependency(FABRIC_LIFECYCLE_DEPENDENCY_READY)],
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
        assert_eq!(
            reduction.topology_projection.source_plan_ref,
            reduction.fulfillment_plan.plan_id
        );
        assert_eq!(
            reduction.topology_projection.ready_role_refs,
            vec!["role:hostServiceAdapter"]
        );
        assert!(reduction.fulfillment_plan.missing_role_refs.is_empty());
        assert_eq!(reduction.ready_contribution_refs.len(), 1);
    }

    #[test]
    fn reduces_ready_plan_into_authorized_adapter_execution() {
        let reduction = reduce_host_fabric(input(vec![contribution(
            "fabric-contribution:service-manager",
            FABRIC_MEMBER_CONTRIBUTION_RUNNING,
            "fabric:lab-gateway",
            "host:lab-service-manager",
        )]))
        .expect("reduction");

        let decision = reduce_host_fabric_control_decision(
            &reduction.fulfillment_plan,
            HostFabricControlDecisionInput {
                decision_id: "fabric-control:service-manager:health-check".to_string(),
                operation_ref: "operation:service-manager:health-check".to_string(),
                subject_ref: "service:manager".to_string(),
                control_owner_ref: None,
                delegated_role_ref: Some(role_ref(FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER)),
                execution_delegation_ref: Some(
                    "delegation:host-service-adapter:health-check".to_string(),
                ),
                authorization_refs: vec![],
                fallback_refs: vec![],
                quarantine_refs: vec![],
                rollback_ref: None,
                release_refs: vec!["release:service-manager:health-check".to_string()],
                evidence_refs: vec!["evidence:operator:health-check-request".to_string()],
                blocked_reasons: vec![],
                safe_facts: json!({ "operation": "healthCheck", "dryRun": true }),
                observed_at: 1_700_000_001,
                expires_at: Some(1_700_000_061),
            },
        )
        .expect("control decision");

        assert_eq!(decision.state, FABRIC_CONTROL_DECISION_READY);
        assert_eq!(
            decision.source_plan_ref.as_deref(),
            Some(reduction.fulfillment_plan.plan_id.as_str())
        );
        assert!(!decision.authorization_refs.is_empty());

        let execution = reduce_host_fabric_adapter_execution_evidence(
            &reduction.fulfillment_plan,
            &decision,
            HostFabricAdapterExecutionInput {
                evidence_id: "adapter-execution:service-manager:health-check".to_string(),
                adapter_ref: "adapter:host-service:windows-service-control".to_string(),
                state: FABRIC_ADAPTER_EXECUTION_SUCCEEDED.to_string(),
                source_bridge_ref: Some("legacy-bridge:service-manager:dry-run".to_string()),
                delegated_role_ref: None,
                action_authority_refs: vec![],
                evidence_requirement_refs: vec![],
                input_refs: vec![decision.operation_ref.clone()],
                output_refs: vec!["evidence:service-manager:health-check:ok".to_string()],
                fallback_refs: vec![],
                quarantine_refs: vec![],
                rollback_refs: vec![],
                release_refs: vec![],
                cleanup_refs: vec!["cleanup:service-manager:health-check".to_string()],
                blocked_reasons: vec![],
                evidence_refs: vec!["evidence:adapter:health-check:ok".to_string()],
                safe_facts: json!({ "operation": "healthCheck", "dryRun": true }),
                observed_at: 1_700_000_002,
                expires_at: Some(1_700_000_062),
            },
        )
        .expect("adapter execution evidence");

        assert_eq!(execution.state, FABRIC_ADAPTER_EXECUTION_SUCCEEDED);
        assert_eq!(
            execution.source_decision_ref.as_deref(),
            Some(decision.decision_id.as_str())
        );
        assert_eq!(execution.authorization_refs, decision.authorization_refs);
        assert!(
            execution
                .output_refs
                .iter()
                .any(|reference| { reference == "evidence:service-manager:health-check:ok" })
        );
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
        assert_eq!(
            reduction.topology_projection.missing_role_refs,
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

    #[test]
    fn reduces_dependency_knot_roles_without_execution_semantics() {
        let roles = [
            FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION,
            FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER,
            FABRIC_MEMBER_ROLE_BUILD_PROCESSOR,
            FABRIC_MEMBER_ROLE_RUNTIME,
            FABRIC_MEMBER_ROLE_SURFACE,
            FABRIC_MEMBER_ROLE_PLATFORM_ADAPTER,
            FABRIC_MEMBER_ROLE_DOMAIN_SERVICE,
        ];
        let contributions = roles
            .iter()
            .map(|role| {
                contribution_for_role(
                    &format!("fabric-contribution:{role}"),
                    role,
                    FABRIC_MEMBER_CONTRIBUTION_RUNNING,
                    "fabric:lab-gateway",
                    "host:lab-service-manager",
                )
            })
            .collect::<Vec<_>>();
        let reduction = reduce_host_fabric(HostFabricReductionInput {
            plan_id: "fabric-plan:dependency-knot".to_string(),
            fabric_ref: "fabric:lab-gateway".to_string(),
            host_ref: "host:lab-service-manager".to_string(),
            contract_ref: "contract:host-fabric.dependency-knot@0.1.0".to_string(),
            required_roles: roles
                .iter()
                .map(|role| HostFabricRoleRequirement {
                    role_ref: role_ref(role),
                    min_ready: 1,
                })
                .collect(),
            contributions,
            lifecycle_plans: vec![lifecycle_with_dependency(FABRIC_LIFECYCLE_DEPENDENCY_READY)],
            materialization_budget_refs: vec!["materialization-budget:dependency-knot".to_string()],
            known_missing_role_refs: vec![],
            evidence_refs: vec!["evidence:dependency-knot:fixture".to_string()],
            blocked_reasons: vec![],
            association_handoff_ref: Some(
                "handoff:substrate:lab-gateway:initial-owner".to_string(),
            ),
            observed_at: 1_700_000_000,
            expires_at: Some(1_700_003_600),
        })
        .expect("reduction");

        assert_eq!(
            reduction.fulfillment_plan.state,
            FABRIC_FULFILLMENT_PLAN_READY
        );
        assert_eq!(
            reduction.fulfillment_plan.required_role_refs.len(),
            roles.len()
        );
        assert_eq!(
            reduction.fulfillment_plan.member_contribution_refs.len(),
            roles.len()
        );
        assert_eq!(
            reduction.topology_projection.role_postures.len(),
            roles.len()
        );
        assert_eq!(
            reduction.dependency_edge_refs,
            vec!["lifecycle-dependency:runtime:gateway"]
        );
        assert!(
            reduction.fulfillment_plan.safe_facts["readyContributionCount"]
                .as_u64()
                .unwrap()
                >= roles.len() as u64
        );
    }

    #[test]
    fn blocks_reduction_on_required_lifecycle_dependency_miss() {
        let reduction = reduce_host_fabric(HostFabricReductionInput {
            plan_id: "fabric-plan:dependency-missing".to_string(),
            fabric_ref: "fabric:lab-gateway".to_string(),
            host_ref: "host:lab-service-manager".to_string(),
            contract_ref: "contract:host-fabric.dependency-knot@0.1.0".to_string(),
            required_roles: vec![HostFabricRoleRequirement {
                role_ref: role_ref(FABRIC_MEMBER_ROLE_RUNTIME),
                min_ready: 1,
            }],
            contributions: vec![contribution_for_role(
                "fabric-contribution:runtime",
                FABRIC_MEMBER_ROLE_RUNTIME,
                FABRIC_MEMBER_CONTRIBUTION_RUNNING,
                "fabric:lab-gateway",
                "host:lab-service-manager",
            )],
            lifecycle_plans: vec![lifecycle_with_dependency(
                FABRIC_LIFECYCLE_DEPENDENCY_MISSING,
            )],
            materialization_budget_refs: vec!["materialization-budget:dependency-knot".to_string()],
            known_missing_role_refs: vec![],
            evidence_refs: vec!["evidence:dependency-knot:missing".to_string()],
            blocked_reasons: vec![],
            association_handoff_ref: Some(
                "handoff:substrate:lab-gateway:initial-owner".to_string(),
            ),
            observed_at: 1_700_000_000,
            expires_at: Some(1_700_003_600),
        })
        .expect("reduction");

        assert_eq!(
            reduction.fulfillment_plan.state,
            FABRIC_FULFILLMENT_PLAN_BLOCKED
        );
        assert!(
            reduction
                .blocked_reasons
                .iter()
                .any(|reason| { reason.contains("hostFabric:lifecycleDependency:missing") })
        );
    }

    #[test]
    fn reduces_multi_gateway_target_registry_from_member_contributions() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../constitute-protocol/vectors/contract-target-multi-gateway-v1.json"
        ))
        .expect("multi-gateway vector");
        let target: ContractTarget =
            serde_json::from_value(vector["target"].clone()).expect("target");
        let reduction =
            reduce_contract_target_registry_from_fabric(ContractTargetRegistryReductionInput {
                plan_id: "fabric-plan:multi-gateway:target-registry".to_string(),
                fabric_ref: "fabric:multi-gateway-dev".to_string(),
                host_ref: "host:fabric-dev".to_string(),
                contract_ref: "app:contract:constitute-nvr@0.1.0".to_string(),
                target,
                contributions: vec![
                    contribution_for_role_with_outputs(
                        "fabric-contribution:gateway-association:local-dev",
                        FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION,
                        vec!["fulfillment:gateway-association:local-dev"],
                    ),
                    contribution_for_role_with_outputs(
                        "fabric-contribution:gateway-association:lab-dev",
                        FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION,
                        vec!["fulfillment:gateway-association:lab-dev"],
                    ),
                    contribution_for_role_with_outputs(
                        "fabric-contribution:service-edge:nvr",
                        FABRIC_MEMBER_ROLE_SERVICE_EDGE_ADAPTER,
                        vec![
                            "fulfillment:nvr-service-edge:local-network",
                            "fulfillment:nvr-service-edge:lab-network",
                        ],
                    ),
                    contribution_for_role_with_outputs(
                        "fabric-contribution:platform-adapter:browser-webrtc",
                        FABRIC_MEMBER_ROLE_PLATFORM_ADAPTER,
                        vec!["fulfillment:platform-adapter:browser-webrtc"],
                    ),
                    contribution_for_role_with_outputs(
                        "fabric-contribution:runtime:browser",
                        FABRIC_MEMBER_ROLE_RUNTIME,
                        vec!["fulfillment:runtime:browser"],
                    ),
                    contribution_for_role_with_outputs(
                        "fabric-contribution:nvr-service",
                        FABRIC_MEMBER_ROLE_DOMAIN_SERVICE,
                        vec![
                            "fulfillment:nvr-service:local-network",
                            "fulfillment:nvr-service:lab-network",
                        ],
                    ),
                ],
                lifecycle_plans: vec![],
                materialization_budget_refs: vec![
                    "materialization-budget:target-registry:hot".to_string(),
                ],
                association_handoff_ref: Some(
                    "handoff:substrate:lab-gateway:initial-owner".to_string(),
                ),
                observed_at: 1_700_000_000,
                expires_at: Some(1_700_003_600),
            })
            .expect("target reduction");

        assert_eq!(
            reduction.registry.state,
            FABRIC_CONTRACT_TARGET_REGISTRY_READY
        );
        assert_eq!(
            reduction.fulfillment_plan.state,
            FABRIC_FULFILLMENT_PLAN_READY
        );
        assert_eq!(
            reduction.selected_gateway_ref.as_deref(),
            Some("fulfillment:gateway-association:local-dev")
        );
        assert_eq!(
            reduction.candidate_gateway_refs,
            vec![
                "fulfillment:gateway-association:local-dev".to_string(),
                "fulfillment:gateway-association:lab-dev".to_string()
            ]
        );
        assert!(
            reduction
                .registry
                .slot_postures
                .iter()
                .all(|slot| slot.safe_facts.get("serviceIdentityMutation").is_none())
        );
        assert!(
            reduction
                .fulfillment_plan
                .member_contribution_refs
                .contains(&"fabric-contribution:gateway-association:lab-dev".to_string())
        );
    }

    #[test]
    fn reduces_carrier_edge_requirement_and_selection_from_fabric_candidates() {
        let reduction = reduce_carrier_edge_selection_from_fabric(CarrierEdgeSelectionInput {
            requirement_id: "carrier-req:gateway-edge:nvr".to_string(),
            selection_id: "carrier-select:gateway-edge:nvr".to_string(),
            subject_ref: "service:nvr".to_string(),
            fabric_ref: "fabric:lab-gateway".to_string(),
            host_ref: "host:lab-service-manager".to_string(),
            source_ref: Some("service:nvr".to_string()),
            consumer_ref: Some("runtime:browser".to_string()),
            route_association_ref: Some("association:gateway:lab".to_string()),
            policy_ref: Some("policy:carrier:default".to_string()),
            required_capability_refs: vec![
                constitute_protocol::CAPABILITY_SWARM_EDGE_ATTACH.to_string(),
            ],
            candidates: vec![
                CarrierEdgeCandidate {
                    adapter_ref: "adapter:gateway:websocket".to_string(),
                    adapter_kind: CARRIER_EDGE_ADAPTER_WEB_SOCKET.to_string(),
                    contribution_ref: Some("fabric-contribution:gateway-association".to_string()),
                    session_binding_ref: Some("binding:gateway:websocket".to_string()),
                    network_sensitivity: Some(CARRIER_EDGE_NETWORK_LOCAL_NETWORK.to_string()),
                    evidence_refs: vec!["evidence:gateway:websocket:ready".to_string()],
                    proof_substrate_refs: vec!["proof-substrate:firewall:websocket".to_string()],
                    resource_posture_refs: vec!["resource:network:websocket".to_string()],
                    blocked_reasons: vec![],
                    state: "actionable".to_string(),
                    priority: 10,
                },
                CarrierEdgeCandidate {
                    adapter_ref: "adapter:gateway:quic".to_string(),
                    adapter_kind: CARRIER_EDGE_ADAPTER_QUIC.to_string(),
                    contribution_ref: Some("fabric-contribution:gateway-association".to_string()),
                    session_binding_ref: Some("binding:gateway:quic".to_string()),
                    network_sensitivity: Some(CARRIER_EDGE_NETWORK_LOCAL_NETWORK.to_string()),
                    evidence_refs: vec!["evidence:gateway:quic:degraded".to_string()],
                    proof_substrate_refs: vec!["proof-substrate:firewall:quic".to_string()],
                    resource_posture_refs: vec!["resource:network:quic".to_string()],
                    blocked_reasons: vec!["carrierEdge:firewall:quic".to_string()],
                    state: "degraded".to_string(),
                    priority: 20,
                },
            ],
            evidence_refs: vec!["evidence:fabric:gateway-association".to_string()],
            observed_at: 1_700_000_000,
            expires_at: Some(1_700_000_090),
        })
        .expect("carrier selection reduction");

        assert_eq!(
            reduction.requirement.kind.as_deref(),
            Some(RECORD_CARRIER_EDGE_REQUIREMENT)
        );
        assert_eq!(
            reduction.selection.kind.as_deref(),
            Some(RECORD_CARRIER_EDGE_SELECTION)
        );
        assert_eq!(
            reduction.selected_adapter_ref.as_deref(),
            Some("adapter:gateway:websocket")
        );
        assert_eq!(reduction.selection.state, CARRIER_EDGE_SELECTION_ACTIONABLE);
        assert_eq!(
            reduction.selection.session_binding_ref.as_deref(),
            Some("binding:gateway:websocket")
        );
        assert_eq!(
            reduction.selection.network_sensitivity.as_deref(),
            Some(CARRIER_EDGE_NETWORK_LOCAL_NETWORK)
        );
        assert_eq!(
            reduction.selection.proof_substrate_refs,
            vec!["proof-substrate:firewall:websocket".to_string()]
        );
        assert!(
            reduction
                .requirement
                .proof_substrate_refs
                .contains(&"proof-substrate:firewall:quic".to_string())
        );
        assert_eq!(
            reduction.selection.backpressure_state.as_deref(),
            Some(CARRIER_EDGE_BACKPRESSURE_CLEAR)
        );
        assert!(
            reduction
                .requirement
                .safe_facts
                .get("selectedAdapterKind")
                .is_some()
        );

        let blocked = reduce_carrier_edge_selection_from_fabric(CarrierEdgeSelectionInput {
            requirement_id: "carrier-req:blocked".to_string(),
            selection_id: "carrier-select:blocked".to_string(),
            subject_ref: "service:nvr".to_string(),
            fabric_ref: "fabric:lab-gateway".to_string(),
            host_ref: "host:lab-service-manager".to_string(),
            source_ref: None,
            consumer_ref: None,
            route_association_ref: None,
            policy_ref: None,
            required_capability_refs: vec![],
            candidates: vec![CarrierEdgeCandidate {
                adapter_ref: "adapter:gateway:websocket".to_string(),
                adapter_kind: CARRIER_EDGE_ADAPTER_WEB_SOCKET.to_string(),
                contribution_ref: None,
                session_binding_ref: None,
                network_sensitivity: None,
                evidence_refs: vec![],
                proof_substrate_refs: vec![],
                resource_posture_refs: vec![],
                blocked_reasons: vec!["carrierEdge:gatewayUnavailable".to_string()],
                state: "blocked".to_string(),
                priority: 10,
            }],
            evidence_refs: vec![],
            observed_at: 1_700_000_000,
            expires_at: Some(1_700_000_090),
        })
        .expect("blocked carrier selection reduction");
        assert_eq!(blocked.selection.state, CARRIER_EDGE_SELECTION_BLOCKED);
        assert!(
            blocked
                .blocked_reasons
                .contains(&"carrierEdge:noActionableAdapter".to_string())
        );
    }

    #[test]
    fn shadow_parity_preserves_ready_plan_when_legacy_and_fabric_agree() {
        let contributions = vec![
            contribution_for_role(
                "fabric-contribution:gatewayAssociation",
                FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION,
                FABRIC_MEMBER_CONTRIBUTION_RUNNING,
                "fabric:lab-gateway",
                "host:lab-service-manager",
            ),
            contribution_for_role(
                "fabric-contribution:hostServiceAdapter",
                FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER,
                FABRIC_MEMBER_CONTRIBUTION_RUNNING,
                "fabric:lab-gateway",
                "host:lab-service-manager",
            ),
        ];
        let parity = reduce_host_fabric_shadow_parity(HostFabricShadowParityInput {
            reduction: HostFabricReductionInput {
                plan_id: "fabric-plan:shadow-parity:ready".to_string(),
                fabric_ref: "fabric:lab-gateway".to_string(),
                host_ref: "host:lab-service-manager".to_string(),
                contract_ref: "contract:host-fabric.shadow@0.1.0".to_string(),
                required_roles: vec![
                    HostFabricRoleRequirement {
                        role_ref: role_ref(FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION),
                        min_ready: 1,
                    },
                    HostFabricRoleRequirement {
                        role_ref: role_ref(FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER),
                        min_ready: 1,
                    },
                ],
                contributions,
                lifecycle_plans: vec![lifecycle(FABRIC_LIFECYCLE_PLAN_READY)],
                materialization_budget_refs: vec!["materialization-budget:shadow".to_string()],
                known_missing_role_refs: vec![],
                evidence_refs: vec!["evidence:legacy-posture:ready".to_string()],
                blocked_reasons: vec![],
                association_handoff_ref: Some(
                    "handoff:substrate:lab-gateway:initial-owner".to_string(),
                ),
                observed_at: 1_700_000_000,
                expires_at: Some(1_700_003_600),
            },
            legacy_ready_role_refs: vec![
                FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION.to_string(),
                FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER.to_string(),
            ],
            legacy_blocked_role_refs: vec![],
        })
        .expect("shadow parity reduces");

        assert_eq!(
            parity.reduction.fulfillment_plan.state,
            FABRIC_FULFILLMENT_PLAN_READY
        );
        assert_eq!(parity.disagreement_role_refs, Vec::<String>::new());
        assert_eq!(parity.agreement_role_refs.len(), 2);
    }

    #[test]
    fn shadow_parity_blocks_legacy_ready_role_missing_from_fabric() {
        let parity = reduce_host_fabric_shadow_parity(HostFabricShadowParityInput {
            reduction: HostFabricReductionInput {
                plan_id: "fabric-plan:shadow-parity:missing".to_string(),
                fabric_ref: "fabric:lab-gateway".to_string(),
                host_ref: "host:lab-service-manager".to_string(),
                contract_ref: "contract:host-fabric.shadow@0.1.0".to_string(),
                required_roles: vec![
                    HostFabricRoleRequirement {
                        role_ref: role_ref(FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION),
                        min_ready: 1,
                    },
                    HostFabricRoleRequirement {
                        role_ref: role_ref(FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER),
                        min_ready: 1,
                    },
                ],
                contributions: vec![contribution_for_role(
                    "fabric-contribution:gatewayAssociation",
                    FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION,
                    FABRIC_MEMBER_CONTRIBUTION_RUNNING,
                    "fabric:lab-gateway",
                    "host:lab-service-manager",
                )],
                lifecycle_plans: vec![lifecycle(FABRIC_LIFECYCLE_PLAN_READY)],
                materialization_budget_refs: vec!["materialization-budget:shadow".to_string()],
                known_missing_role_refs: vec![],
                evidence_refs: vec!["evidence:legacy-posture:ready".to_string()],
                blocked_reasons: vec![],
                association_handoff_ref: Some(
                    "handoff:substrate:lab-gateway:initial-owner".to_string(),
                ),
                observed_at: 1_700_000_000,
                expires_at: Some(1_700_003_600),
            },
            legacy_ready_role_refs: vec![
                FABRIC_MEMBER_ROLE_GATEWAY_ASSOCIATION.to_string(),
                FABRIC_MEMBER_ROLE_HOST_SERVICE_ADAPTER.to_string(),
            ],
            legacy_blocked_role_refs: vec![],
        })
        .expect("shadow parity reduces");

        assert_eq!(
            parity.reduction.fulfillment_plan.state,
            FABRIC_FULFILLMENT_PLAN_BLOCKED
        );
        assert_eq!(
            parity.disagreement_role_refs,
            vec!["role:hostServiceAdapter".to_string()]
        );
        assert!(parity.blocked_reasons.contains(
            &"hostFabric:legacyDisagreement:missingRole:role:hostServiceAdapter".to_string()
        ));
        assert!(parity.reduction.fulfillment_plan.blocked_reasons.contains(
            &"hostFabric:legacyDisagreement:missingRole:role:hostServiceAdapter".to_string()
        ));
    }
}
