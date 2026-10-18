"""Execute the CI policies; historical source acceptance is separate from regression.

The historical scripts and semantic commands are compared with the actual PR
parent, not replaced by permissive test implementations.
"""
import ast
import hashlib
import os
from pathlib import Path
import re
import subprocess
from types import SimpleNamespace

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
BASE = "f8ad17fc1b68082c1d07d68c3d2b0734c1385468"
WORKFLOWS = ROOT / ".github/workflows"
ACTION = ".github/actions/acceptance-redis/action.yml"
TOOLS = [
    "sprint81-3-task-admit-runtime-binding.yml",
    "sprint82-1-scheduler-runtime-truth-guard.yml",
    "sprint82-2-worker-runtime-truth-guards.yml",
    "sprint82-3-recovery-read-model-convergence.yml",
    "sprint83-2-terminal-task-run-finalization.yml",
    "sprint83-3-durable-status-result-read-model.yml",
    "sprint83-4-production-run-finalization-binding.yml",
]
REDIS = [
    "sprint83-7-trusted-gateway-single-task-alpha.yml",
    "sprint83-8-idempotent-single-task-submission.yml",
    "sprint84-7a-baseline-truth-closure.yml",
    "sprint84-7b-task-claim-production-composition.yml",
    "sprint84-7c2-task-terminal-production-composition.yml",
    "sprint84-7d-run-terminate-production-injection.yml",
    "sprint84-8-canonical-resource-settlement.yml",
    "sprint84-9-canonical-task-requeue-retry.yml",
]
LOCKS = [
    (REDIS[4], "Verify C1 product byte parity and forbidden-file freeze", "sprint/84-7c2-task-terminal-production-composition"),
    (REDIS[5], "Verify frozen RUN and TASK authority parity", "sprint/84-7d-run-terminate-production-injection"),
    (REDIS[6], "Verify frozen product parity and no new OperationType", "sprint/84-8-canonical-resource-settlement"),
    (REDIS[7], "Frozen authority parity", "sprint/84-9-canonical-task-requeue-retry"),
]
SCOPES = [
    (REDIS[6], "Verify accepted parent and exact 84.8 scope", LOCKS[2][2]),
    (REDIS[7], "Exact parent and scope", LOCKS[3][2]),
]


def workflow(name, *, original=False):
    raw = (subprocess.check_output(["git", "show", f"{BASE}:.github/workflows/{name}"], cwd=ROOT, text=True)
           if original else (WORKFLOWS / name).read_text())
    return yaml.safe_load(raw)


def steps(doc):
    return doc["jobs"]["verify"]["steps"] if "verify" in doc["jobs"] else next(iter(doc["jobs"].values()))["steps"]


def step(doc, name):
    return next(s for s in steps(doc) if s.get("name") == name)


def inline(program):
    return program.split("python - <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]


@pytest.mark.parametrize("name", TOOLS)
def test_install_resolves_local_tools_in_same_transaction(name):
    install = next(s["run"] for s in steps(workflow(name))
                   if "pip install" in s.get("run", ""))
    commands = install.replace("\\\n", " ").splitlines()
    package_command = next(c for c in commands if "-e hfa-control" in c)
    assert "-e hfa-core" in package_command and "-e hfa-tools" in package_command


@pytest.mark.parametrize("name", REDIS)
def test_owned_executable_precedes_semantic_tests_without_adopting_service(name):
    doc = workflow(name)
    ss = steps(doc)
    setup = next(i for i, s in enumerate(ss) if s.get("uses") == "./.github/actions/acceptance-redis")
    assert setup < min(i for i, s in enumerate(ss) if "python -m pytest" in s.get("run", ""))
    assert ACTION in doc.get("on", doc.get(True))["pull_request"]["paths"]
    assert "HFA_ACCEPTANCE_REDIS_BINARY" not in doc.get("env", {})
    assert not any(s.get("continue-on-error") for s in ss)
    assert "Retain owned acceptance Redis evidence" in [s.get("name") for s in ss]


@pytest.mark.parametrize("name,title,feature", LOCKS + SCOPES)
def test_historical_source_and_scope_programs_preserved_exactly(name, title, feature):
    old = step(workflow(name, original=True), title)
    new = step(workflow(name), title)
    assert new["run"] == old["run"]
    assert new["if"] == f"github.head_ref == '{feature}'"


@pytest.mark.parametrize("name,title,feature", LOCKS)
@pytest.mark.parametrize("corrupt", [False, True])
def test_original_frozen_source_gate_passes_historical_bytes_and_rejects_mutation(monkeypatch, name, title, feature, corrupt):
    doc = workflow(name)
    program = inline(step(doc, title)["run"])
    parent = doc["env"]["ACCEPTED_PARENT"]
    # Load genuine old bytes before mocking the gate's Git calls.
    historical = {}
    for path in set(re.findall(r"[\"'](hfa-[^\"']+\.(?:py|lua))[\"']", program)):
        historical[path] = subprocess.check_output(["git", "show", f"{parent}:{path}"], cwd=ROOT)
    assert historical
    pristine = dict(historical)
    target = ("hfa-control/src/hfa_control/task_terminal_authority.py" if name == REDIS[4]
              else "hfa-control/src/hfa_control/run_termination.py" if name == REDIS[5]
              else sorted(historical)[0])
    if corrupt:
        historical[target] += b"\n# unreviewed modification\n"
    original_read = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda p: historical.get(str(p), b"") if str(p) in historical else original_read(p))
    def output(argv, **kwargs):
        if argv[1] == "hash-object":
            raw = historical[argv[2]]
            return hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest() + "\n"
        if argv[1] == "rev-parse":
            raw = pristine[argv[2].split(":", 1)[1]]
            return hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest() + "\n"
        if argv[1] == "show":
            return pristine[argv[2].split(":", 1)[1]].decode()
        raise AssertionError(argv)
    monkeypatch.setattr(subprocess, "check_output", output)
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=0))
    monkeypatch.setenv("PR_HEAD_BRANCH", feature)
    monkeypatch.setenv("PR_BASE_SHA", parent)
    monkeypatch.setenv("ACCEPTED_PARENT", parent)
    if corrupt:
        with pytest.raises(AssertionError):
            exec(compile(program, title, "exec"), {})
    else:
        exec(compile(program, title, "exec"), {})


@pytest.mark.parametrize("name,title,feature", SCOPES)
@pytest.mark.parametrize("variant", ["exact", "missing", "extra"])
def test_original_feature_scope_remains_exact(monkeypatch, name, title, feature, variant):
    program = inline(step(workflow(name), title)["run"])
    tree = ast.parse(program)
    assignment = next(n for n in tree.body if isinstance(n, ast.Assign) and n.targets[0].id in {"allowlist", "allowed"})
    paths = ast.literal_eval(assignment.value)
    if variant == "missing":
        paths = paths - {sorted(paths)[0]}
    if variant == "extra":
        paths = paths | {"unrelated.py"}
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **k: "\n".join(sorted(paths)))
    monkeypatch.setenv("PR_BASE_SHA", "parent")
    monkeypatch.setenv("ACCEPTED_PARENT", "parent")
    if variant == "exact":
        exec(compile(program, title, "exec"), {})
    else:
        with pytest.raises(AssertionError):
            exec(compile(program, title, "exec"), {})


@pytest.mark.parametrize("name,title,feature", SCOPES)
def test_wrong_historical_parent_still_fails_before_scope_execution(name, title, feature):
    run = step(workflow(name), title)["run"]
    run = run.replace("${{ github.event.pull_request.base.sha }}", "wrong-parent")
    run = run.replace("${{ github.head_ref }}", feature)
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    result = subprocess.run(["bash", "-c", run], cwd=ROOT, capture_output=True, text=True,
        env=dict(os.environ, PR_BASE_SHA="wrong-parent", ACCEPTED_PARENT="accepted-parent",
                 PR_HEAD_SHA=head, FEATURE_BRANCH=feature))
    assert result.returncode != 0
    assert "SCOPE_OK" not in result.stdout


@pytest.mark.parametrize("name", REDIS[:6])
@pytest.mark.parametrize("generated", [False, True])
def test_shared_action_only_trigger_is_consistent_and_keeps_artifact_prohibition(monkeypatch, name, generated):
    doc = workflow(name)
    scope = next(s for s in steps(doc) if "SCOPE_OK" in s.get("run", ""))
    changed = {ACTION} | ({"local_out/forbidden.json"} if generated else set())
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **k: "\n".join(sorted(changed)))
    monkeypatch.setenv("PR_BASE_SHA", "base")
    monkeypatch.setenv("PR_HEAD_BRANCH", "recovery/current-integration")
    if generated:
        with pytest.raises(AssertionError):
            exec(compile(inline(scope["run"]), name, "exec"), {})
    else:
        exec(compile(inline(scope["run"]), name, "exec"), {})


@pytest.mark.parametrize("name,_,feature", SCOPES)
@pytest.mark.parametrize("variant", ["product", "action", "unrelated", "generated"])
def test_current_scope_executes_product_and_rejects_unrelated_or_generated(monkeypatch, name, _, feature, variant):
    doc = workflow(name)
    trigger = doc.get("on", doc.get(True))["pull_request"]["paths"][1]
    paths = {trigger}
    if variant == "action":
        paths = {ACTION}
    elif variant == "unrelated":
        paths = {"unrelated.py"}
    elif variant == "generated":
        paths.add("local_out/unreviewed.json")
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **k: "\n".join(sorted(paths)))
    monkeypatch.setenv("PR_BASE_SHA", "base")
    program = inline(step(doc, "Verify current integration regression applicability")["run"])
    if variant in {"unrelated", "generated"}:
        with pytest.raises(AssertionError):
            exec(compile(program, name, "exec"), {})
    else:
        exec(compile(program, name, "exec"), {})


@pytest.mark.parametrize("name", TOOLS + REDIS)
def test_existing_semantic_tests_and_exclusion_checks_unchanged(name):
    old = [s for s in steps(workflow(name, original=True)) if "python -m pytest" in s.get("run", "")]
    new = steps(workflow(name))
    assert old
    for s in old:
        current = next(n for n in new if n.get("name") == s.get("name"))
        assert current["run"] == s["run"]
        assert "if" not in current


@pytest.mark.parametrize("name", REDIS[:2])
def test_artifact_cli_uses_existing_owned_launcher_and_keeps_assertions(name):
    old = step(workflow(name, original=True), "Deterministic one-command product acceptance")["run"]
    new = step(workflow(name), "Deterministic one-command product acceptance")["run"]
    expected = old.replace('--redis-url "$REDIS_URL"', '--owned-fresh-domain').replace('  --reset-test-db \\\n', '')
    assert new == expected
    assert "--reset-test-db" not in new and "--redis-url" not in new


def test_source_build_has_published_digest_and_no_lifecycle_authority():
    run = yaml.safe_load((ROOT / ACTION).read_text())["runs"]["steps"][0]["run"]
    assert "669ab6689b5e7d0c479e8d526ccbadae36b69a11370742ffe23822b9df8d85ba" in run
    assert run.index("sha256sum --check --strict") < run.index("tar -xzf") < run.index("make -C")
    assert "https://download.redis.io/releases/redis-7.4.10.tar.gz" in run
    for forbidden in ["flushdb", "activate_task_owner_namespace", "redis-cli", "redis:latest"]:
        assert forbidden not in run.lower()


@pytest.mark.parametrize("mode", ["download_failure", "corrupt_source"])
def test_untrusted_source_fails_before_extraction_build_or_service(tmp_path, mode):
    run = yaml.safe_load((ROOT / ACTION).read_text())["runs"]["steps"][0]["run"]
    bindir = tmp_path / "bin"
    bindir.mkdir()
    # The only substituted boundary is network retrieval; checksum and all
    # subsequent shell fail-closed behavior are the real workflow program.
    curl = bindir / "curl"
    curl.write_text("#!/bin/bash\n" + ("exit 22\n" if mode == "download_failure" else
                    'while [ "$1" != "-o" ]; do shift; done\nprintf corrupt > "$2"\n'))
    curl.chmod(0o755)
    for command in ["tar", "make"]:
        f = bindir / command
        f.write_text('#!/bin/bash\ntouch "$UNSAFE_BOUNDARY"\nexit 99\n')
        f.chmod(0o755)
    unsafe = tmp_path / "unsafe"
    envfile = tmp_path / "env"
    result = subprocess.run(["bash", "-c", run], env=dict(os.environ,
        PATH=str(bindir) + ":" + os.environ["PATH"], RUNNER_TEMP=str(tmp_path),
        GITHUB_ENV=str(envfile), UNSAFE_BOUNDARY=str(unsafe)), capture_output=True, text=True)
    assert result.returncode != 0
    assert not unsafe.exists() and not envfile.exists()
