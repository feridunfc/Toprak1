from __future__ import annotations
import copy, importlib.util, os
from pathlib import Path

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location("v",ROOT/"scripts/sprint80c/validate_aggregate_boundary.py"); assert spec and spec.loader
v=importlib.util.module_from_spec(spec); spec.loader.exec_module(v)
E=ROOT/"docs/adr/sprint80/evidence/aggregate_boundary.run93.json"; M=ROOT/"docs/adr/sprint80/aggregate_boundary_matrix.json"; MF=ROOT/"docs/adr/sprint80/aggregate_boundary_manifest.json"
def e():return v.readj(E)
def m():return v.readj(M)

def test_frozen_evidence_exact(): assert v.sha(E.read_bytes())==v.EVIDENCE_SHA and v.validate_evidence(e())==[]
def test_selected_model_and_revision_owners():
 s=m()["selected_model"]; assert s["name"]=="INDEPENDENT_TASK_AGGREGATES_WITH_DURABLE_DEPENDENCY_PROCESS_MANAGER"; assert s["parent_revision_owner"]=="PARENT_TASK_AGGREGATE" and s["child_revision_owner"]=="CHILD_TASK_AGGREGATE"
def test_predecessor_is_not_falsely_accepted():
 p=m()["predecessor_governance"]; assert p["accepted_head_candidate"]=="9ad9503badd72afb0a935dbb8c02e828ea02d3e2"; assert p["acceptance_comment_id"] is None and p["human_architecture_acceptance"]=="PENDING"
def test_child_admission_commits_topology_authority():
 a=m()["transaction_boundaries"]["child_admission"]; assert a["expected_parent_edge_set_owner"]=="CHILD_TASK_AGGREGATE"; assert a["expected_parent_edge_set_mutability"]=="IMMUTABLE_AFTER_ADMISSION"; assert {"graph_identity","graph_revision_or_topology_hash","one_canonical_admission_record"}<=set(a["atomic_commit_members"])
def test_dependency_count_is_not_authority():
 a=m()["transaction_boundaries"]["child_admission"]; assert a["dependency_count_authority"] is False; assert a["dependency_count_rule"]=="MUST_EQUAL_CARDINALITY_OF_EXPECTED_PARENT_EDGE_SET"
def test_graph_identity_and_hash_required():
 t=m()["graph_topology_authority"]; assert t["graph_identity_format"]=="run:{run_id}:graph"; assert t["graph_revision_or_topology_hash"]=="REQUIRED_IMMUTABLE_SNAPSHOT_IDENTIFIER"
def test_fanout_intent_is_topology_bound():
 f=m()["fanout_intent_contract"]; assert all(f[k]=="REQUIRED" for k in ("parent_transition_id","graph_identity","graph_revision_or_topology_hash","child_edge_set_digest"))
def test_one_canonical_command_identity():
 x=m()["edge_identity_contract"]; assert x["sole_command_idempotency_authority"]=="edge_command_id"; assert x["receipt_key_authority"]=="edge_command_id"; assert "parent_transition_id" in x["edge_command_id_format"] and "edge_outcome" in x["edge_command_id_format"]
def test_duplicate_command_is_exact_noop():
 x=m()["edge_identity_contract"]["duplicate_same_edge_command_id"]; assert x=={"canonical_record_count":0,"child_revision_increment":0,"result":"ALREADY_APPLIED","retry":"STOP"}
def test_receipts_are_outcome_aware():
 r=m()["edge_receipt_contract"]; assert r["authority"]=="IDEMPOTENT_APPLIED_EDGE_RECEIPTS_WITH_OUTCOME"; assert set(r["allowed_outcomes"])=={"DEPENDENCY_SATISFIED","DEPENDENCY_FAILED"}
def test_failed_edge_has_deterministic_child_disposition():
 f=m()["dependency_policy_contract"]["failed_edge"]; assert f["child_disposition"]=="blocked_by_failure"; assert f["child_revision_increment"]==1; assert f["canonical_transition_record"]=="EXACTLY_ONE"; assert f["ready_projection_intent"]==0 and f["process_manager_retry"]=="STOP_AFTER_RECEIPT"
def test_unknown_policy_fails_closed(): assert m()["dependency_policy_contract"]["unknown_dependency_policy"]["result"]=="FAIL_CLOSED"
def test_terminal_and_late_commands_stop_retry():
 d=m()["terminal_and_late_command_dispositions"]; keys={"child_already_terminal_due_to_valid_external_cancellation","child_done_or_failed_before_required_edges_complete","child_ready_or_running_with_unsatisfied_required_edges","parent_transition_valid_but_graph_edge_missing","child_aggregate_missing"}; assert all(d[k]["child_mutation"]==0 and d[k]["retry"]=="STOP" for k in keys); assert d["record_owner"]=="DEPENDENCY_PROCESS_MANAGER_COORDINATION_STATE" and d["record_is_child_revision"] is False
def test_logical_and_physical_identity_are_separate():
 p=m()["physical_identity_strategy"]; assert p["logical_identity"]=="run_id_plus_task_id"; assert p["physical_key_layout"]=="EXISTING_TASK_ID_KEYS_TEMPORARILY_RETAINED"; assert p["physical_rekey_in_80C2"]=="NOT_SELECTED"
def test_findings_mapped_but_unresolved():
 rows=m()["finding_dispositions"]; assert {x["finding_id"] for x in rows}==v.FINDINGS; assert all(x["resolved_in_product"] is False and x["implementation_sprint"]==84 for x in rows)
def test_manifest_and_scope(): assert v.validate_manifest(ROOT,v.readj(MF))==[] and v.validate_scope(v.PATHS)==[]
def test_local_bundle_reports_governance_blocker_not_technical_failure():
 z=os.getenv("SPRINT80_RUN93_ZIP"); r=v.validate_bundle(ROOT,evidence_zip=Path(z) if z else None,changed_paths=v.PATHS); assert r["technical_status"]=="PASS"; assert r["status"]=="PASS_WITH_GOVERNANCE_BLOCKER"; assert r["governance_status"]=="BLOCKED" and r["implementation_authorized"] is False
def test_negative_missing_topology_owner():
 x=copy.deepcopy(m()); x["transaction_boundaries"]["child_admission"]["expected_parent_edge_set_owner"]="UNKNOWN"; assert "topology owner" in v.validate_matrix(x,e())
def test_negative_missing_graph_hash():
 x=copy.deepcopy(m()); x["graph_topology_authority"]["graph_revision_or_topology_hash"]="OPTIONAL"; assert "graph binding" in v.validate_matrix(x,e())
def test_negative_satisfied_only_receipts():
 x=copy.deepcopy(m()); x["edge_receipt_contract"]["allowed_outcomes"]=["DEPENDENCY_SATISFIED"]; assert "receipt outcomes" in v.validate_matrix(x,e())
def test_negative_split_command_identity():
 x=copy.deepcopy(m()); x["edge_identity_contract"]["receipt_key_authority"]="parent_revision"; assert "single command identity" in v.validate_matrix(x,e())
def test_negative_undefined_terminal_disposition():
 x=copy.deepcopy(m()); del x["terminal_and_late_command_dispositions"]["child_aggregate_missing"]; assert "terminal dispositions" in v.validate_matrix(x,e())
def test_negative_physical_migration_ambiguity():
 x=copy.deepcopy(m()); x["physical_identity_strategy"]["physical_rekey_in_80C2"]="IMPLICIT"; assert "identity invariant" in v.validate_matrix(x,e())
def test_negative_product_scope(): assert v.validate_scope([*v.PATHS,"hfa-core/src/hfa/lua/task_complete.lua"])
def test_negative_evidence_mutation():
 x=copy.deepcopy(e()); x["cross_task_mutation_observed"]=False; assert "cross-task observation" in v.validate_evidence(x)
