import json
import subprocess
import sys

from scripts.feedbackwriter_governance import build_artifact


def test_feedbackwriter_governance_artifact_passes():
    artifact = build_artifact()

    assert artifact.status == "PASS"
    assert artifact.advisory_only_surface is True
    assert artifact.canonical_authority_writes_allowed is False

    cases = {case.name: case for case in artifact.cases}
    assert cases["valid_feedback"].accepted is True
    assert cases["low_confidence"].accepted is False
    assert cases["non_success_status"].accepted is False
    assert cases["missing_trace_id"].accepted is False
    assert cases["malformed_output_data"].accepted is False


def test_feedbackwriter_governance_cli_writes_artifact(tmp_path):
    output = tmp_path / "latest_feedbackwriter_governance.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/feedbackwriter_governance.py",
            "--output",
            str(output),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["source"] == "feedbackwriter_governance"
    assert payload["status"] == "PASS"
    assert payload["advisory_only_surface"] is True
    assert payload["canonical_authority_writes_allowed"] is False
    assert output.exists()
