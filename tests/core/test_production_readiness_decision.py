import json
import subprocess
import sys
from pathlib import Path

from scripts.production_readiness_decision import build_artifact


def _write_rollup(path: Path, *, status: str = "PASS", components=None):
    if components is None:
        components = [
            {
                "name": "authority",
                "status": "PASS",
                "required": True,
                "present": True,
            },
            {
                "name": "deployment_smoke",
                "status": "NOT_PRESENT_LOCAL",
                "required": False,
                "present": False,
            },
        ]

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "source": "production_readiness_rollup",
                "status": status,
                "components": components,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_production_readiness_decision_ready_when_rollup_passes(tmp_path):
    rollup = tmp_path / "latest_production_readiness_rollup.json"
    _write_rollup(rollup, status="PASS")

    artifact = build_artifact(rollup)

    assert artifact.status == "PASS"
    assert artifact.decision == "READY"
    assert artifact.rollup_status == "PASS"
    assert artifact.reasons == []


def test_production_readiness_decision_not_ready_when_rollup_missing(tmp_path):
    rollup = tmp_path / "missing.json"

    artifact = build_artifact(rollup)

    assert artifact.status == "PASS"
    assert artifact.decision == "NOT_READY"
    assert artifact.rollup_status == "MISSING"
    assert artifact.reasons == ["production_readiness_rollup_missing"]


def test_production_readiness_decision_not_ready_when_rollup_malformed(tmp_path):
    rollup = tmp_path / "latest_production_readiness_rollup.json"
    rollup.write_text("{not-json", encoding="utf-8")

    artifact = build_artifact(rollup)

    assert artifact.status == "PASS"
    assert artifact.decision == "NOT_READY"
    assert artifact.rollup_status == "MALFORMED"
    assert artifact.reasons == ["production_readiness_rollup_malformed"]


def test_production_readiness_decision_not_ready_when_rollup_fails(tmp_path):
    rollup = tmp_path / "latest_production_readiness_rollup.json"
    _write_rollup(rollup, status="FAIL")

    artifact = build_artifact(rollup)

    assert artifact.decision == "NOT_READY"
    assert artifact.reasons == ["production_readiness_rollup_status:FAIL"]


def test_production_readiness_decision_not_ready_when_required_component_missing(tmp_path):
    rollup = tmp_path / "latest_production_readiness_rollup.json"
    _write_rollup(
        rollup,
        status="PASS",
        components=[
            {
                "name": "authority",
                "status": "MISSING",
                "required": True,
                "present": False,
            }
        ],
    )

    artifact = build_artifact(rollup)

    assert artifact.decision == "NOT_READY"
    assert artifact.reasons == ["required_component_missing:authority"]


def test_production_readiness_decision_cli_writes_artifact(tmp_path):
    rollup = tmp_path / "latest_production_readiness_rollup.json"
    output = tmp_path / "latest_production_readiness_decision.json"
    _write_rollup(rollup, status="PASS")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/production_readiness_decision.py",
            "--input",
            str(rollup),
            "--output",
            str(output),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["source"] == "production_readiness_decision"
    assert payload["status"] == "PASS"
    assert payload["decision"] == "READY"
    assert output.exists()
