import json
import subprocess
import sys

from scripts.semanticbridge_gate import build_artifact


def test_semanticbridge_gate_artifact_passes():
    artifact = build_artifact()

    assert artifact.status == "PASS"
    assert artifact.advisory_only_surface is True
    assert artifact.canonical_authority_writes_allowed is False

    cases = {case.name: case for case in artifact.cases}
    assert cases["missing_verdict"].allowed is False
    assert cases["low_confidence_allowed_verdict"].allowed is False
    assert cases["high_confidence_allowed_verdict"].allowed is True
    assert cases["hook_unavailable"].allowed is False
    assert cases["hook_exception"].allowed is False


def test_semanticbridge_gate_cli_writes_artifact(tmp_path):
    output = tmp_path / "latest_semanticbridge_gate.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/semanticbridge_gate.py",
            "--output",
            str(output),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["source"] == "semanticbridge_gate"
    assert payload["status"] == "PASS"
    assert payload["advisory_only_surface"] is True
    assert payload["canonical_authority_writes_allowed"] is False
    assert output.exists()
