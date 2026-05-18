from __future__ import annotations

import importlib.util
import json
import sys
import textwrap
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "authority_audit.py"
_SPEC = importlib.util.spec_from_file_location("authority_audit", _SCRIPT)
assert _SPEC and _SPEC.loader

authority_audit = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = authority_audit
_SPEC.loader.exec_module(authority_audit)


def test_authority_audit_scans_critical_runtime_files() -> None:
    repo = Path(__file__).resolve().parents[2]
    scanned = {p.relative_to(repo).as_posix() for p in authority_audit.iter_python_files(repo, authority_audit.DEFAULT_SCAN_DIRS)}

    for critical in authority_audit.CRITICAL_RUNTIME_FILES:
        assert critical in scanned


def test_authority_audit_bans_direct_state_write(tmp_path: Path) -> None:
    repo = tmp_path
    target = repo / "hfa-control" / "src" / "hfa_control" / "bad_writer.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        textwrap.dedent(
            """
            async def bad(redis, run_id):
                state_key = f"hfa:run:{run_id}:state"
                await redis.set(state_key, "done")
            """
        ),
        encoding="utf-8",
    )

    result = authority_audit.run_audit(repo, scan_dirs=("hfa-control/src",))
    assert result.banned == 1
    assert result.status == "FAIL"


def test_authority_audit_allows_event_append(tmp_path: Path) -> None:
    repo = tmp_path
    target = repo / "hfa-control" / "src" / "hfa_control" / "event_writer.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        textwrap.dedent(
            """
            async def ok(redis):
                await redis.xadd("events", {"event_type": "TASK_SCHEDULED"})
            """
        ),
        encoding="utf-8",
    )

    result = authority_audit.run_audit(repo, scan_dirs=("hfa-control/src",))
    assert result.banned == 0
    assert result.allowed == 1


def test_authority_audit_dashboard_json_shape(tmp_path: Path, capsys) -> None:
    repo = tmp_path
    target = repo / "hfa-worker" / "src" / "hfa_worker" / "writer.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        "async def maybe(redis):\n    await redis.hset('some:key', mapping={'x': 'y'})\n",
        encoding="utf-8",
    )

    code = authority_audit.main(["--repo-root", str(repo), "--format", "dashboard", "--fail-on", "none"])
    captured = capsys.readouterr().out
    payload = json.loads(captured)

    assert code == 0
    assert "authority_status" in payload
    assert "risk_score" in payload
    assert "counts" in payload
    assert "findings" in payload


def test_authority_audit_ci_exit_code_on_banned(tmp_path: Path) -> None:
    repo = tmp_path
    target = repo / "hfa-core" / "src" / "hfa" / "bad.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        "async def bad(redis):\n    await redis.set('run_state:1', 'done')\n",
        encoding="utf-8",
    )

    assert authority_audit.main(["--repo-root", str(repo), "--fail-on", "banned"]) == 1


def test_authority_audit_critical_runtime_mutation_is_banned(tmp_path: Path) -> None:
    repo = tmp_path
    target = repo / "hfa-core" / "src" / "hfa" / "runtime" / "state_store.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        "async def bad(redis, run_id):\n    await redis.hset(f'hfa:run:{run_id}:state', mapping={'state': 'done'})\n",
        encoding="utf-8",
    )
    result = authority_audit.run_audit(repo, scan_dirs=("hfa-core/src",), include_lua=False)
    assert result.banned == 1
    assert "critical runtime" in result.findings[0].reason


def test_authority_audit_strict_fails_on_suspicious(tmp_path: Path) -> None:
    repo = tmp_path
    target = repo / "hfa-worker" / "src" / "hfa_worker" / "writer.py"
    target.parent.mkdir(parents=True)
    target.write_text("async def maybe(redis):\n    await redis.hset('other:key', mapping={'x': 'y'})\n", encoding="utf-8")
    assert authority_audit.main(["--repo-root", str(repo), "--strict"]) == 1


def test_authority_audit_heatmap_output(tmp_path: Path, capsys) -> None:
    repo = tmp_path
    target = repo / "hfa-control" / "src" / "hfa_control" / "writer.py"
    target.parent.mkdir(parents=True)
    target.write_text("async def maybe(redis):\n    await redis.hset('other:key', mapping={'x': 'y'})\n", encoding="utf-8")
    code = authority_audit.main(["--repo-root", str(repo), "--heatmap", "--fail-on", "none"])
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload
    assert payload[0]["risk_score"] > 0


def test_authority_audit_lua_scanner_reports_mutations(tmp_path: Path) -> None:
    repo = tmp_path
    target = repo / "hfa-core" / "src" / "hfa" / "lua" / "task_complete.lua"
    target.parent.mkdir(parents=True)
    target.write_text("redis.call('HSET', KEYS[1], 'state', 'done')\n", encoding="utf-8")
    result = authority_audit.run_audit(repo, scan_dirs=(), include_lua=True)
    assert result.suspicious == 1
    assert result.findings[0].category == "lua_script_mutation"


def test_authority_audit_suspicious_classification_shape(tmp_path: Path, capsys) -> None:
    repo = tmp_path
    target = repo / "hfa-core" / "src" / "hfa" / "governance" / "budget_guard.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        "async def maybe(redis):\n    await redis.set('budget:tenant:1:spent', '10')\n",
        encoding="utf-8",
    )

    code = authority_audit.main(["--repo-root", str(repo), "--format", "dashboard", "--fail-on", "none"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["findings"][0]["suspicious_class"] == "governance_local"
    assert payload["suspicious_classes"]["governance_local"] == 1
    assert payload["heatmap"][0]["class_counts"]["governance_local"] == 1


def test_authority_audit_false_positive_static_classification(tmp_path: Path) -> None:
    repo = tmp_path
    target = repo / "hfa-control" / "src" / "hfa_control" / "worker_scoring.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        "def score(worker):\n    caps = set(getattr(worker, 'capabilities', ()) or ())\n    return caps\n",
        encoding="utf-8",
    )

    result = authority_audit.run_audit(repo, scan_dirs=("hfa-control/src",), include_lua=False)

    assert result.suspicious == 1
    assert result.findings[0].suspicious_class == "false_positive_static"


def test_authority_audit_false_positive_static_has_zero_risk(tmp_path: Path, capsys) -> None:
    repo = tmp_path
    target = repo / "hfa-control" / "src" / "hfa_control" / "worker_scoring.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        "def score(worker):\n    caps = set(getattr(worker, 'capabilities', ()) or ())\n    return caps\n",
        encoding="utf-8",
    )

    result = authority_audit.run_audit(repo, scan_dirs=("hfa-control/src",), include_lua=False)

    assert result.suspicious == 1
    assert result.findings[0].suspicious_class == "false_positive_static"
    assert result.findings[0].risk_score == 0

    code = authority_audit.main(["--repo-root", str(repo), "--format", "dashboard", "--fail-on", "none"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["noise_count"] == 1
    assert payload["risk_bearing_suspicious"] == 0
    assert payload["risk_score"] == 0


def test_authority_audit_observability_only_has_low_risk(tmp_path: Path, capsys) -> None:
    repo = tmp_path
    target = repo / "hfa-core" / "src" / "hfa" / "obs" / "graph_store.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        "async def save(redis):\n    await redis.set('graph:snapshot', '1')\n",
        encoding="utf-8",
    )

    result = authority_audit.run_audit(repo, scan_dirs=("hfa-core/src",), include_lua=False)

    assert result.suspicious == 1
    assert result.findings[0].suspicious_class == "observability_only"
    assert result.findings[0].risk_score == 1

    code = authority_audit.main(["--repo-root", str(repo), "--format", "dashboard", "--fail-on", "none"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["noise_count"] == 0
    assert payload["telemetry_risk_count"] == 1
    assert payload["risk_bearing_suspicious"] == 0
    assert payload["risk_score"] == 1
