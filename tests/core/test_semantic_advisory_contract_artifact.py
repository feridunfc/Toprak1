import json
import subprocess
import sys
from pathlib import Path

from scripts.semantic_advisory_contract import build_artifact


def test_semantic_advisory_contract_artifact_passes_repo():
    artifact = build_artifact(Path("."))

    assert artifact.status == "PASS"
    assert artifact.contract_doc_exists is True
    assert artifact.surfaces_checked == 2
    assert artifact.governance_audit_status == "PASS"
    assert artifact.governance_audit_findings_count == 0

    for marker in artifact.markers:
        assert marker.advisory_only_surface is True
        assert marker.canonical_authority_writes_allowed is False


def test_semantic_advisory_contract_cli_writes_artifact(tmp_path):
    output = tmp_path / "latest_semantic_advisory_contract.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/semantic_advisory_contract.py",
            "--output",
            str(output),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["source"] == "semantic_advisory_contract"
    assert payload["status"] == "PASS"
    assert payload["contract_doc_exists"] is True
    assert payload["surfaces_checked"] == 2
    assert payload["governance_audit_status"] == "PASS"
    assert payload["governance_audit_findings_count"] == 0
    assert output.exists()
