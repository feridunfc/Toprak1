"""Execute historical workflow policies with deterministic, source-bound Git fixtures.

These fixtures exercise the actual inline programs, not a second policy implementation.
Real repository provenance is verified separately when validating a candidate.
"""
import ast
from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
import subprocess
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]
A = "sprint84-7a-baseline-truth-closure.yml"
B = "sprint84-1-canonical-task-dispatch-binding.yml"
BASE = "b71deacf711b616443c4ca8ed9e93fdf104347d1"
REVIEW = "6f9fd4bc02c8fc93faf3f2413903f7c6510f1057"
HEAD = "ff02be25d7f1f30dc9ba8f99ed35fcfe920686ca"
AUTHORITY = "hfa-core/src/hfa/authority"
PERSISTENCE = AUTHORITY + "/redis_persistence.py"
BLOB = "9b9cbdf9dbce32ef69a3fd0f1dbf1cd2bf6cc7fe"
TREE = "080de232678d0a8670024b987e4fa9dad9563e38"
SCHEDULER = "hfa-control/src/hfa_control/scheduler.py"
FEATURE_A = "sprint/84-7a-baseline-truth-closure"
FEATURE_B = "sprint/84-1-canonical-task-dispatch-binding"

FEATURE_A_FILES = {
    ".github/workflows/sprint83-7-trusted-gateway-single-task-alpha.yml",
    ".github/workflows/sprint83-8-idempotent-single-task-submission.yml",
    ".github/workflows/sprint84-7a-baseline-truth-closure.yml",
    "docs/implementation/sprint84/SPRINT84.7A-baseline-truth-closure.md",
    "hfa-control/src/hfa_control/scheduler_reservation_dispatch.py",
    "hfa-control/src/hfa_control/task_claim.py",
    "tests/core/test_production_scheduler_composition_contract.py",
}
FEATURE_B_FILES = {
    ".github/workflows/sprint84-1-canonical-task-dispatch-binding.yml",
    "docs/implementation/sprint84/SPRINT84.1-canonical-task-dispatch-binding.md",
    "hfa-control/src/hfa_control/dag_lua.py",
    "hfa-control/src/hfa_control/dag_scheduler_bridge.py",
    "hfa-control/src/hfa_control/dag_scheduler_dispatch_controller.py",
    SCHEDULER, "hfa-control/src/hfa_control/scheduler_reservation_dispatch.py",
    "hfa-control/src/hfa_control/task_dispatch_authority.py",
    "hfa-core/src/hfa/dag/schema.py", "hfa-core/src/hfa/lua/task_dispatch_commit.lua",
    "scripts/runtime_alpha_acceptance_84_1.py",
    "tests/integration/test_task_dispatch_authority_binding_integration.py",
    "tests/integration/test_dag_task_dispatch_integration.py",
    "tests/integration/test_scheduler_dag_dispatch_writer_integration.py",
    "tests/integration/test_scheduler_reservation_dispatch_integration.py",
    "tests/integration/test_task_dispatch_identity_contract_integration.py",
    "tests/unit/test_task_dispatch_authority_binding.py",
    "tests/unit/test_scheduler_runtime_truth_guard.py",
}
# Frozen reviewed source inventory, not an input manifest trusted by a workflow.
F07_FILES = {
    'hfa-control/src/hfa_control/dag_lua.py',
    'hfa-control/src/hfa_control/scheduler.py',
    'hfa-control/src/hfa_control/service.py',
    'hfa-control/src/hfa_control/task_admit_authority.py',
    'hfa-core/pyproject.toml',
    'hfa-core/src/hfa/authority/__init__.py',
    'hfa-core/src/hfa/authority/redis_persistence.py',
    'hfa-core/src/hfa/lua/canonical_authority_commit.lua',
    'hfa-core/src/hfa/lua/task_admit.lua',
    'scripts/scheduler_dispatched_task_execution.py',
    'tests/core/test_scheduler_dispatch_identity_contract.py',
    'tests/core/test_task_recovery_core.py',
    'tests/core/test_worker_consumer_process_message.py',
    'tests/core/test_worker_consumer_task_consumer_bridge.py',
    'tests/integration/test_worker_run_terminate_canonical_composition_integration.py',
    'tests/unit/test_f02a1_root_binding.py',
    'tests/unit/test_f04_4a_task_admit_projection_proof.py',
    'tests/unit/test_f06_task_admit_effect_completeness.py',
    'tests/unit/test_g0_r_profile_isolation.py',
    'tests/unit/test_g1_f07_global_task_owner.py',
    'tests/unit/test_worker_runtime_truth_guards.py',
}
CURRENT_FILES = {
    '.github/workflows/ci-authority-gate.yml',
    'hfa-control/src/hfa_control/dag_lua.py',
    'hfa-control/src/hfa_control/run_termination.py',
    'hfa-control/src/hfa_control/scheduler.py',
    'hfa-control/src/hfa_control/service.py',
    'hfa-control/src/hfa_control/task_admit_authority.py',
    'hfa-core/pyproject.toml',
    'hfa-core/src/hfa/authority/__init__.py',
    'hfa-core/src/hfa/authority/redis_persistence.py',
    'hfa-core/src/hfa/lua/canonical_authority_commit.lua',
    'hfa-core/src/hfa/lua/task_admit.lua',
    'scripts/runtime_alpha_acceptance_83_5.py',
    'scripts/runtime_alpha_acceptance_83_6.py',
    'scripts/scheduler_dispatched_task_execution.py',
    'tests/core/test_authority_gate_redis_endpoint_contract.py',
    'tests/core/test_scheduler_dispatch_identity_contract.py',
    'tests/core/test_task_recovery_core.py',
    'tests/core/test_worker_consumer_process_message.py',
    'tests/core/test_worker_consumer_task_consumer_bridge.py',
    'tests/integration/test_legacy_terminal_duplicate_recovery_boundary.py',
    'tests/integration/test_terminal_task_run_finalization.py',
    'tests/integration/test_user_facing_run_output_e2e_83_6.py',
    'tests/integration/test_user_facing_runtime_e2e_83_5.py',
    'tests/integration/test_worker_run_terminate_canonical_composition_integration.py',
    'tests/integration/test_worker_service_recovery_proof_continuity.py',
    'tests/unit/test_f02a1_root_binding.py',
    'tests/unit/test_f04_4a_task_admit_projection_proof.py',
    'tests/unit/test_f06_task_admit_effect_completeness.py',
    'tests/unit/test_g0_r_profile_isolation.py',
    'tests/unit/test_g1_f07_global_task_owner.py',
    'tests/unit/test_run_termination_proof_continuity.py',
    'tests/unit/test_runtime_acceptance_terminal_event_setup.py',
    'tests/unit/test_worker_runtime_truth_guards.py',
}
PRESERVED_STEPS = [{'sha256': '99fbdeb7fa8846bb078d142178391991916cfba2931e7427d1cebf58216c8b6a',
  'step': 'Checkout exact PR HEAD',
  'workflow': 'sprint84-7a-baseline-truth-closure.yml'},
 {'sha256': '5ad75121c35696b40573989c7f11f241b0eb1a6694829dd8d66316b3f3b811f2',
  'step': 'Install dependencies',
  'workflow': 'sprint84-7a-baseline-truth-closure.yml'},
 {'sha256': 'bdf414f25b46c89e104d8c629d5663c7967fe0190ac72b46130fc0bd70a47b1e',
  'step': 'Focused baseline truth closure',
  'workflow': 'sprint84-7a-baseline-truth-closure.yml'},
 {'sha256': '7629419a69b4388cefe5b22ed667801505d4cf0d2e7597a2aa1dbd9f489ceb88',
  'step': 'TASK_CLAIM regression with zero exclusions',
  'workflow': 'sprint84-7a-baseline-truth-closure.yml'},
 {'sha256': 'f920db663c33a0ce0b08d7833a38334c77a7279761d32a246e6e137b409f211a',
  'step': 'Sprint 83.7 regression without baseline deselection',
  'workflow': 'sprint84-7a-baseline-truth-closure.yml'},
 {'sha256': '9eb9e7eae31055b2be330d14614bb1c9dc0ce44c022db6c2b4c6333a9775a59b',
  'step': 'Sprint 83.8 regression without baseline deselection',
  'workflow': 'sprint84-7a-baseline-truth-closure.yml'},
 {'sha256': '514b3b37236d4b98c431aa884d325d674d6d1480eaa39c3e462ee5d857eead19',
  'step': 'Static authority boundary',
  'workflow': 'sprint84-7a-baseline-truth-closure.yml'},
 {'sha256': 'cb0a253068f1ee70e7b62b7e59f63b1b186349072f5de766d9de2770da53859b',
  'step': 'Authority audit',
  'workflow': 'sprint84-7a-baseline-truth-closure.yml'},
 {'sha256': '9fd05075bd75d0bbb21435f4a141b0289873ba06950c5a6424880df436b26035',
  'step': 'Compile and diff hygiene',
  'workflow': 'sprint84-7a-baseline-truth-closure.yml'},
 {'sha256': '99fbdeb7fa8846bb078d142178391991916cfba2931e7427d1cebf58216c8b6a',
  'step': 'Checkout exact PR HEAD',
  'workflow': 'sprint84-1-canonical-task-dispatch-binding.yml'},
 {'sha256': '7c8d330a7f6ee80fc07ca7205ab6f5c5ad2d928b466ee74150b0910b7705c106',
  'step': 'Install dependencies',
  'workflow': 'sprint84-1-canonical-task-dispatch-binding.yml'},
 {'sha256': '3f3c5ab996b0bc62dc6f5ea7b943e1f33bada70636880960dbfb84e3138fbffb',
  'step': 'Dedicated unit and scheduler contracts',
  'workflow': 'sprint84-1-canonical-task-dispatch-binding.yml'},
 {'sha256': '30128918a3fbf8866d4961b5135c4f4fb5deb95c0cf07d970787f4e2069a7eb3',
  'step': 'Real Redis authority and dispatch regression',
  'workflow': 'sprint84-1-canonical-task-dispatch-binding.yml'},
 {'sha256': 'aa24092c9e2acf34e9e2d0f6b153df781445e86c5882c812ef23aab7b1450202',
  'step': 'Deterministic Sprint 84.1 acceptance',
  'workflow': 'sprint84-1-canonical-task-dispatch-binding.yml'},
 {'sha256': '8101b76c72b8aca62011d593344297653ea7070127e951e6bbefb86780c6ee56',
  'step': 'Compile and whitespace validation',
  'workflow': 'sprint84-1-canonical-task-dispatch-binding.yml'},
 {'sha256': '60dd2a1bbc4a97dd08d7ea78e5ca80e1698ffa921669fd3b9c4cfd2c8ae79cf8',
  'step': 'Upload deterministic evidence',
  'workflow': 'sprint84-1-canonical-task-dispatch-binding.yml'}]
SEMANTIC_AST_SHA256 = '32f713b4e6480810700d19f5ddb24ab9685a3ac6ecd109d839a6ef4c63a83e9c'


def blocks(workflow):
    text = (ROOT / ".github/workflows" / workflow).read_text()
    return ["      - " + part for part in re.split(r"^      - ", text, flags=re.M)[1:]]


def step(workflow, name):
    return next(part.rstrip() for part in blocks(workflow) if part.startswith("      - name: " + name + "\n"))


def inline(workflow, name):
    lines = step(workflow, name).splitlines()
    start = lines.index("          python - <<'PY'") + 1
    end = next(i for i in range(start, len(lines)) if lines[i] == "          PY")
    return "\n".join(line[10:] for line in lines[start:end])


@dataclass
class GitEvidence:
    changed: set
    head: str = HEAD
    reviewed_commit: str = REVIEW
    reviewed_blob: str = BLOB
    head_blob: str = BLOB
    reviewed_tree: str = TREE
    head_tree: str = TREE
    review_base: str = BASE
    pr_base: str = BASE
    ancestry: int = 0
    history: str = ""
    working_delta: str = ""
    untracked: str = ""
    unavailable: bool = False

    def check_output(self, args, *, text):
        assert text is True
        if self.unavailable:
            raise subprocess.CalledProcessError(128, args)
        commands = {
            ("git", "diff", "--name-only", BASE + "...HEAD"): "\n".join(sorted(self.changed)),
            ("git", "rev-parse", "HEAD"): self.head,
            ("git", "rev-parse", REVIEW + "^{commit}"): self.reviewed_commit,
            ("git", "merge-base", BASE, REVIEW): self.review_base,
            ("git", "merge-base", BASE, BASE): self.pr_base,
            ("git", "rev-parse", REVIEW + ":" + AUTHORITY): self.reviewed_tree,
            ("git", "rev-parse", "HEAD:" + AUTHORITY): self.head_tree,
            ("git", "rev-parse", REVIEW + ":" + PERSISTENCE): self.reviewed_blob,
            ("git", "rev-parse", "HEAD:" + PERSISTENCE): self.head_blob,
            ("git", "diff", "--name-only", "HEAD", "--", AUTHORITY): self.working_delta,
            ("git", "ls-files", "--others", "--exclude-standard", "--", AUTHORITY): self.untracked,
            ("git", "log", "--full-history", "--format=%H", REVIEW + "..HEAD", "--", AUTHORITY): self.history,
        }
        assert tuple(args) in commands, ("unexpected Git query", args)
        return commands[tuple(args)] + "\n"

    def run(self, args, *, capture_output):
        assert capture_output is True
        assert args == ["git", "merge-base", "--is-ancestor", REVIEW, "HEAD"]
        return SimpleNamespace(returncode=self.ancestry)


@pytest.fixture
def policy(monkeypatch):
    def execute(workflow, changed, *, branch="worker/regression", authority=False, **overrides):
        evidence = GitEvidence(set(changed), **overrides)
        monkeypatch.setattr(subprocess, "check_output", evidence.check_output)
        monkeypatch.setattr(subprocess, "run", evidence.run)
        monkeypatch.setenv("PR_BASE_SHA", BASE)
        monkeypatch.setenv("PR_HEAD_SHA", HEAD)
        monkeypatch.setenv("PR_HEAD_BRANCH", branch)
        monkeypatch.chdir(ROOT)
        name = ("Authority boundary scan" if authority else
                "Verify exact Sprint 84.7A scope" if workflow == A else "Verify Sprint 84.1 scope")
        namespace = {}
        exec(compile(inline(workflow, name), workflow + ":" + name, "exec"), namespace)
        return namespace
    return execute


@pytest.mark.parametrize("workflow,branch,files", [(A, FEATURE_A, FEATURE_A_FILES), (B, FEATURE_B, FEATURE_B_FILES)])
def test_original_feature_scope_passes_exactly(policy, workflow, branch, files):
    result = policy(workflow, files, branch=branch)
    assert result["mode"] == "FEATURE_EXACT_SCOPE" and result["allowlist"] == files
    if workflow == B:
        assert policy(B, files, branch=branch, authority=True)["authority_policy"] == "FEATURE_NO_CORE_CHANGE"


@pytest.mark.parametrize("workflow,branch,missing", [
    (A, FEATURE_A, path) for path in sorted(FEATURE_A_FILES)
] + [(B, FEATURE_B, path) for path in sorted(FEATURE_B_FILES)])
def test_original_feature_missing_file_fails(policy, workflow, branch, missing):
    files = FEATURE_A_FILES if workflow == A else FEATURE_B_FILES
    with pytest.raises(AssertionError):
        policy(workflow, files - {missing}, branch=branch)


@pytest.mark.parametrize("workflow,branch,files", [(A, FEATURE_A, FEATURE_A_FILES), (B, FEATURE_B, FEATURE_B_FILES)])
@pytest.mark.parametrize("extra", [SCHEDULER + ".extra", PERSISTENCE, AUTHORITY + "/__init__.py", "unrelated.txt"])
def test_original_feature_extra_file_fails(policy, workflow, branch, files, extra):
    with pytest.raises(AssertionError):
        policy(workflow, files | {extra}, branch=branch)


@pytest.mark.parametrize("files", [F07_FILES, CURRENT_FILES])
@pytest.mark.parametrize("workflow", [A, B])
def test_reviewed_f07_inventory_triggers_regressions(policy, workflow, files):
    assert policy(workflow, files)["mode"] != "FEATURE_EXACT_SCOPE"
    if workflow == B:
        assert policy(B, files, authority=True)["authority_policy"] == "REVIEWED_F07_SOURCE_AND_ANCESTRY"


def test_scheduler_only_trigger_and_all_declared_84_7a_paths_match(policy):
    source = (ROOT / ".github/workflows" / A).read_text()
    trigger_block = source.split("    paths:\n", 1)[1].split("\npermissions:", 1)[0]
    paths = set(re.findall(r"^      - '([^']+)'$", trigger_block, flags=re.M))
    result = policy(A, {SCHEDULER})
    assert paths == result["regression_triggers"]
    assert result["allowlist"] == FEATURE_A_FILES and len(result["allowlist"]) == 7
    for path in paths:
        assert policy(A, {path})["mode"] == "REGRESSION_TRIGGER_INTERSECTION"


@pytest.mark.parametrize("workflow", [A, B])
@pytest.mark.parametrize("branch", ["worker/g1-f07-v2-global-task-owner-20261014", "worker/anything", "unrelated-branch"])
def test_branch_name_cannot_exempt_unrelated_scope(policy, workflow, branch):
    with pytest.raises(AssertionError):
        policy(workflow, {"docs/unrelated.md"}, branch=branch)


@pytest.mark.parametrize("workflow", [A, B])
@pytest.mark.parametrize("path", ["local_out/report.json", "pkg/__pycache__/x.py", ".pytest_cache/v/cache", "pkg/x.pyc"])
def test_generated_artifact_is_rejected_even_with_valid_trigger(policy, workflow, path):
    with pytest.raises(AssertionError):
        policy(workflow, {SCHEDULER, path})


@pytest.mark.parametrize("path", [PERSISTENCE, AUTHORITY + "/canonical_transition.py"])
def test_original_dispatch_feature_has_no_authority_exception(policy, path):
    with pytest.raises(AssertionError):
        policy(B, FEATURE_B_FILES | {path}, branch=FEATURE_B, authority=True)


@pytest.mark.parametrize("path", [AUTHORITY + "/canonical_transition.py", AUTHORITY + "/new_writer.py", AUTHORITY + "/subdir/authority.py"])
def test_additional_authority_core_path_is_not_approved(policy, path):
    with pytest.raises(AssertionError):
        policy(B, CURRENT_FILES | {path}, authority=True)


@pytest.mark.parametrize("variant", [
    {"head_blob": "0" * 40}, {"reviewed_blob": "0" * 40},
    {"head_tree": "0" * 40}, {"reviewed_tree": "0" * 40},
    {"reviewed_commit": "0" * 40}, {"head": "0" * 40},
    {"review_base": "0" * 40}, {"pr_base": "0" * 40},
    {"ancestry": 1}, {"ancestry": 128}, {"unavailable": True},
    {"history": "unreviewed-authority-commit"},
    {"working_delta": PERSISTENCE}, {"untracked": AUTHORITY + "/new_writer.py"},
])
def test_missing_changed_or_invalid_authority_provenance_fails(policy, variant):
    with pytest.raises((AssertionError, subprocess.CalledProcessError)):
        policy(B, CURRENT_FILES, authority=True, **variant)


@pytest.mark.parametrize("variant", [{"head_tree": "0" * 40}, {"history": "changed-then-reverted"}])
def test_pr_base_cannot_hide_post_review_authority_changes(policy, variant):
    with pytest.raises(AssertionError):
        policy(B, {SCHEDULER}, authority=True, **variant)


def test_author_supplied_review_claim_cannot_approve_new_blob(policy, monkeypatch):
    monkeypatch.setenv("REVIEWED_AUTHORITY_BLOB", "0" * 40)
    monkeypatch.setenv("AUTHORITY_MANIFEST", '{"approved":true}')
    with pytest.raises(AssertionError):
        policy(B, CURRENT_FILES, authority=True, head_blob="0" * 40)


def test_absent_core_diff_cannot_bypass_source_and_provenance_review(policy):
    assert policy(B, {SCHEDULER}, authority=True)["authority_policy"] == "REVIEWED_F07_SOURCE_AND_ANCESTRY"
    for ancestry in (1, 128):
        with pytest.raises(AssertionError):
            policy(B, {SCHEDULER}, authority=True, ancestry=ancestry)


@pytest.mark.parametrize("branch", [FEATURE_B, "worker/regression"])
@pytest.mark.parametrize("forbidden", ["automatic_reassignment = True", "automatic_repair = True"])
def test_dispatch_anti_repair_assertions_remain_active(policy, monkeypatch, branch, forbidden):
    read = Path.read_text
    def changed_source(path, *args, **kwargs):
        text = read(path, *args, **kwargs)
        return text + "\n" + forbidden if str(path).endswith("task_dispatch_authority.py") else text
    monkeypatch.setattr(Path, "read_text", changed_source)
    with pytest.raises(AssertionError):
        policy(B, FEATURE_B_FILES if branch == FEATURE_B else CURRENT_FILES, authority=True, branch=branch)


@pytest.mark.parametrize("record", PRESERVED_STEPS)
def test_original_semantic_commands_and_zero_exclusion_steps_unchanged(record):
    assert hashlib.sha256(step(record["workflow"], record["step"]).encode()).hexdigest() == record["sha256"]


def test_dispatch_semantic_ast_is_unchanged():
    tree = ast.parse(inline(B, "Authority boundary scan"))
    start = next(i for i, n in enumerate(tree.body) if isinstance(n, ast.Assign) and n.targets[0].id == "scheduler")
    semantic = ast.Module(body=tree.body[start:-1], type_ignores=[])
    assert hashlib.sha256(ast.dump(semantic).encode()).hexdigest() == SEMANTIC_AST_SHA256


@pytest.mark.parametrize("workflow", [A, B])
def test_new_policy_contract_step_is_unconditional_and_zero_exclusion(workflow):
    block = step(workflow, "Historical CI scope contracts")
    assert "if:" not in block
    assert "set -o pipefail" in block
    assert "python -m pytest tests/core/test_ci_historical_scope_contracts.py -q --tb=short" in block
    assert "! grep -Eqi '[1-9][0-9]* (skipped|deselected|xfailed|xpassed)'" in block
    assert "--deselect" not in block and "--ignore" not in block and " -k " not in block
