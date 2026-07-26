from __future__ import annotations

import argparse, hashlib, json, subprocess, zipfile
from pathlib import Path
from typing import Any, Iterable

BASE="9d7c7a6ad52a8a546a708dda048f28d4e23befc6"
SOURCE="5734ac60704f8546b8ce67e766e042ff2ce4c412"
ZIP_SHA="7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588"
EVIDENCE_SHA="ac5ab01455a3e8acb6bc0043167849e398df393819f5b4cdf14e9770dea9f478"
FINDINGS={"missing_remaining_counter_unlocks_fail_open","ready_marker_strands_pending_child"}
PATHS={
 ".github/workflows/sprint80c2-aggregate-boundary.yml",
 "docs/adr/sprint80/ADR-080C2-aggregate-boundary.md",
 "docs/adr/sprint80/evidence/aggregate_boundary.run93.json",
 "docs/adr/sprint80/aggregate_boundary_manifest.json",
 "docs/adr/sprint80/aggregate_boundary_matrix.json",
 "scripts/sprint80c/validate_aggregate_boundary.py",
 "tests/diagnostics/sprint80c/test_80c2_aggregate_boundary.py",
}
BINDINGS={
 "docs/adr/sprint80/ADR-080C-decision-package.md":"ebc15272498fafbe115c164df4075257c090d539",
 "docs/adr/sprint80/ADR-080C1-runtime-truth-authority.md":"659f77af1c8a99cf63626318cb050ac711352b74",
 "scripts/sprint80/aggregate_boundary.py":"1d519c10f952907d4ac7a73e0854cff913a61eb6",
 "tests/diagnostics/sprint80/test_80_07_aggregate_boundary.py":"7462e70146923711937eacf801af7cee778ae0eb",
 "tests/diagnostics/sprint80/test_80_04_transition_cardinality.py":"4a07352e33424e32332c916acdfd29e02b3eb71b",
 "hfa-core/src/hfa/lua/task_complete.lua":"8ee2b51cd94642f56d5e38680419a2e3988b26a9",
 "hfa-core/src/hfa/lua/task_admit.lua":"e5bd30953f4c7c3258e589d240113295f8bccadc",
 "hfa-core/src/hfa/dag/schema.py":"cd657e4b37975c3cf3080a221222c6f8caf56fec",
}
ZIP_MEMBERS={"aggregate_boundary.json","contracts_pytest.txt","environment.json","environment_after.json","final_summary.json","manifest.json","preflight.json","preflight_after.json","reality_pytest.txt","transition_cardinality.json","truth_contradictions.json","ttl_durability.json","writer_inventory.json"}

def sha(b:bytes)->str:return hashlib.sha256(b).hexdigest()
def readj(p:Path)->dict[str,Any]:return json.loads(p.read_text(encoding="utf-8"))
def git(root:Path,*a:str)->str:return subprocess.check_output(["git",*a],cwd=root,text=True).strip()
def err(ok:bool,msg:str,out:list[str])->None:
 if not ok:out.append(msg)
def bundle(entries:list[dict[str,Any]])->bytes:
 return "".join(f'{x["path"]}\0{x["size_bytes"]}\0{x["sha256"]}\n' for x in sorted(entries,key=lambda x:x["path"])).encode()

def validate_evidence(e:dict[str,Any])->list[str]:
 o=[]; err(e.get("schema_version")==1,"evidence schema",o); err(e.get("observation_count")==8,"evidence count",o)
 err(e.get("global_result")=="CROSS_TASK_MUTATION_WITHOUT_CHILD_AUTHORITY_RECORD","evidence result",o)
 err(e.get("cross_task_mutation_observed") is True,"cross-task observation",o)
 err(e.get("aggregate_revision_observed") is False and e.get("child_authority_record_observed") is False,"authority observation",o)
 err(set(e.get("blocking_findings",[]))==FINDINGS,"finding set",o); return o

def validate_matrix(m:dict[str,Any],e:dict[str,Any])->list[str]:
 o=[]; err(m.get("schema_version")==2 and m.get("decision_id")=="ADR-080C2","matrix identity",o)
 err(m.get("decision_status")=="CORRECTED_TECHNICAL_RECOMMENDATION_PREDECESSOR_ACCEPTANCE_PENDING","decision status",o)
 err(m.get("product_implementation_authorized") is False,"implementation flag",o)
 p=m.get("predecessor_governance",{}); err(p.get("accepted_head_candidate")=="9ad9503badd72afb0a935dbb8c02e828ea02d3e2","predecessor head",o)
 err(p.get("merge_commit")==BASE,"predecessor merge",o); err(p.get("acceptance_comment_id") is None and p.get("human_architecture_acceptance")=="PENDING","predecessor must remain pending without record",o)
 s=m.get("selected_model",{}); err(s.get("name")=="INDEPENDENT_TASK_AGGREGATES_WITH_DURABLE_DEPENDENCY_PROCESS_MANAGER","selected model",o)
 err(s.get("logical_task_aggregate_identity")=="task:{run_id}:{task_id}","logical identity",o)
 err(s.get("direct_parent_to_child_authority_mutation")=="FORBIDDEN" and s.get("distributed_transaction_across_tasks") is False,"aggregate boundary",o)
 phys=m.get("physical_identity_strategy",{}); err(phys.get("physical_key_layout")=="EXISTING_TASK_ID_KEYS_TEMPORARILY_RETAINED","physical key strategy",o)
 err(phys.get("required_invariant")=="TASK_ID_GLOBALLY_UNIQUE_ACROSS_ALL_RUNS" and phys.get("physical_rekey_in_80C2")=="NOT_SELECTED","identity invariant",o)
 adm=m.get("transaction_boundaries",{}).get("child_admission",{}); needed={"child_identity","child_initial_state","expected_parent_edge_set","dependency_policy","graph_identity","graph_revision_or_topology_hash","child_initial_revision","one_canonical_admission_record"}
 err(adm.get("authority_scope")=="ONE_CHILD_TASK_AGGREGATE" and set(adm.get("atomic_commit_members",[]))==needed,"child admission boundary",o)
 err(adm.get("expected_parent_edge_set_owner")=="CHILD_TASK_AGGREGATE" and adm.get("expected_parent_edge_set_mutability")=="IMMUTABLE_AFTER_ADMISSION","topology owner",o)
 err(adm.get("dependency_count_authority") is False and adm.get("dependency_count_rule")=="MUST_EQUAL_CARDINALITY_OF_EXPECTED_PARENT_EDGE_SET","count authority",o)
 top=m.get("graph_topology_authority",{}); err(top.get("graph_identity_format")=="run:{run_id}:graph" and top.get("graph_revision_or_topology_hash")=="REQUIRED_IMMUTABLE_SNAPSHOT_IDENTIFIER","graph binding",o)
 fan=m.get("fanout_intent_contract",{}); err(all(fan.get(k)=="REQUIRED" for k in ("parent_transition_id","graph_identity","graph_revision_or_topology_hash","child_edge_set_digest")),"fanout topology binding",o)
 ids=m.get("edge_identity_contract",{}); err(ids.get("sole_command_idempotency_authority")=="edge_command_id" and ids.get("receipt_key_authority")=="edge_command_id","single command identity",o)
 err(ids.get("edge_command_id_format")=="{parent_transition_id}:{child_task_id}:{edge_outcome}:{graph_revision_or_topology_hash}","command format",o)
 rec=m.get("edge_receipt_contract",{}); err(rec.get("authority")=="IDEMPOTENT_APPLIED_EDGE_RECEIPTS_WITH_OUTCOME","outcome receipt authority",o)
 err(set(rec.get("allowed_outcomes",[]))=={"DEPENDENCY_SATISFIED","DEPENDENCY_FAILED"},"receipt outcomes",o)
 pol=m.get("dependency_policy_contract",{}); failed=pol.get("failed_edge",{}); err(failed.get("child_disposition")=="blocked_by_failure" and failed.get("child_revision_increment")==1 and failed.get("canonical_transition_record")=="EXACTLY_ONE" and failed.get("process_manager_retry")=="STOP_AFTER_RECEIPT","failed edge contract",o)
 err(pol.get("unknown_dependency_policy",{}).get("result")=="FAIL_CLOSED","unknown policy",o)
 disp=m.get("terminal_and_late_command_dispositions",{}); required={"child_already_terminal_due_to_valid_external_cancellation","child_done_or_failed_before_required_edges_complete","child_ready_or_running_with_unsatisfied_required_edges","parent_transition_valid_but_graph_edge_missing","child_aggregate_missing"}
 err(required<=set(disp),"terminal dispositions",o); err(all((disp.get(k) or {}).get("retry")=="STOP" and (disp.get(k) or {}).get("child_mutation")==0 for k in required),"terminal retry termination",o)
 err(disp.get("record_owner")=="DEPENDENCY_PROCESS_MANAGER_COORDINATION_STATE" and disp.get("record_is_child_revision") is False,"disposition owner",o)
 f=m.get("finding_dispositions",[]); err({x.get("finding_id") for x in f}==FINDINGS and all(x.get("resolved_in_product") is False and x.get("implementation_sprint")==84 for x in f),"finding mapping",o)
 c=m.get("verification_contracts",{}); err(c.get("technical_unresolved_choice")==0 and c.get("canonical_command_identity_count")==1,"verification completeness",o)
 err(c.get("predecessor_human_acceptance_verified") is False and c.get("predecessor_acceptance_record_present") is False,"governance truth",o)
 err({x.get("path"):x.get("blob_sha") for x in m.get("source_bindings",[])}==BINDINGS,"source binding register",o)
 return o

def validate_manifest(root:Path,m:dict[str,Any])->list[str]:
 o=[]; err(m.get("schema_version")==2 and m.get("decision_id")=="ADR-080C2","manifest identity",o)
 entries=m.get("artifacts",[]); expected=PATHS-{"docs/adr/sprint80/aggregate_boundary_manifest.json"}; err({x.get("path") for x in entries}==expected and len(entries)==6,"manifest paths",o)
 for x in entries:
  p=root/x["path"]; err(p.exists(),f'missing {x["path"]}',o)
  if p.exists(): err(p.stat().st_size==x["size_bytes"] and sha(p.read_bytes())==x["sha256"],f'hash {x["path"]}',o)
 err(m.get("bundle_index_sha256")==sha(bundle(entries)),"bundle index",o); return o

def validate_scope(paths:Iterable[str])->list[str]:
 got={str(x).replace("\\","/") for x in paths}; return [] if got==PATHS else [json.dumps({"missing":sorted(PATHS-got),"unexpected":sorted(got-PATHS)})]
def validate_sources(root:Path)->list[str]:
 o=[]
 for p,h in BINDINGS.items():
  try: err(git(root,"rev-parse",f"{BASE}:{p}")==h,f"source binding {p}",o)
  except subprocess.CalledProcessError:o.append(f"source binding unreadable {p}")
 return o
def validate_zip(path:Path,evidence:bytes)->list[str]:
 o=[]; err(sha(path.read_bytes())==ZIP_SHA,"ZIP digest",o)
 if o:return o
 with zipfile.ZipFile(path) as z:
  err(set(z.namelist())==ZIP_MEMBERS,"ZIP members",o); b=z.read("aggregate_boundary.json")
 err(sha(b)==EVIDENCE_SHA and b==evidence,"frozen evidence bytes",o); return o

def validate_bundle(root:Path,*,evidence_zip:Path|None=None,changed_paths:Iterable[str]|None=None,verify_source_bindings=False,verify_git_scope=False)->dict[str,Any]:
 ep=root/"docs/adr/sprint80/evidence/aggregate_boundary.run93.json"; eb=ep.read_bytes(); e=json.loads(eb)
 m=readj(root/"docs/adr/sprint80/aggregate_boundary_matrix.json"); man=readj(root/"docs/adr/sprint80/aggregate_boundary_manifest.json")
 errors=[]; err(sha(eb)==EVIDENCE_SHA,"repository evidence digest",errors); errors+=validate_evidence(e)+validate_matrix(m,e)+validate_manifest(root,man)
 if changed_paths is not None:errors+=validate_scope(changed_paths)
 if verify_git_scope:errors+=validate_scope(git(root,"diff","--name-only",f"{BASE}...HEAD").splitlines())
 if verify_source_bindings:errors+=validate_sources(root)
 hist="NOT_RUN"
 if evidence_zip: z=validate_zip(evidence_zip,eb); errors+=z; hist="PASS" if not z else "FAIL"
 blocker=m.get("predecessor_governance",{}).get("human_architecture_acceptance")!="ACCEPT"
 return {"schema_version":2,"status":"FAIL" if errors else ("PASS_WITH_GOVERNANCE_BLOCKER" if blocker else "PASS"),"technical_status":"FAIL" if errors else "PASS","governance_status":"BLOCKED" if blocker else "PASS","governance_blockers":["ADR-080C1 post-merge human architecture acceptance record is missing"] if blocker else [],"human_architecture_acceptance":"NOT_READY" if blocker else "PENDING","predecessor_human_acceptance":"PENDING" if blocker else "ACCEPT","base_head":BASE,"frozen_source_head":SOURCE,"frozen_artifact_sha256":ZIP_SHA,"aggregate_evidence_sha256":EVIDENCE_SHA,"observation_count":e.get("observation_count"),"finding_count":len(FINDINGS),"changed_path_count":len(PATHS),"historical_frozen_zip_verification":hist,"source_binding_verification":"PASS" if verify_source_bindings and not any("source binding" in x for x in errors) else ("NOT_RUN" if not verify_source_bindings else "FAIL"),"git_scope_verification":"PASS" if verify_git_scope and not any("missing" in x or "unexpected" in x for x in errors) else ("NOT_RUN" if not verify_git_scope else "FAIL"),"product_source_mutation":0,"implementation_authorized":False,"errors":errors}

def main()->int:
 p=argparse.ArgumentParser(); p.add_argument("--repo-root",type=Path,default=Path.cwd()); p.add_argument("--evidence-zip",type=Path); p.add_argument("--changed-path",action="append",default=[]); p.add_argument("--verify-source-bindings",action="store_true"); p.add_argument("--verify-git-scope",action="store_true"); a=p.parse_args()
 r=validate_bundle(a.repo_root.resolve(),evidence_zip=a.evidence_zip.resolve() if a.evidence_zip else None,changed_paths=a.changed_path or None,verify_source_bindings=a.verify_source_bindings,verify_git_scope=a.verify_git_scope); print(json.dumps(r,indent=2,sort_keys=True)); return 1 if r["technical_status"]=="FAIL" else 0
if __name__=="__main__":raise SystemExit(main())
