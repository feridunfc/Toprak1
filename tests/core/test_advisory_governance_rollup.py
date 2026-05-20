import json
import subprocess
import sys
from pathlib import Path

from scripts.advisory_governance_rollup import build_artifact


def test_advisory_governance_rollup_passes_repo():
    artifact = build_artifact(Path("."))

    assert artifact.status == "PASS"
    assert artifact.components_checked == 4

    components = {component.name: component for component in artifact.components}
    assert components["cognitive_governance_audit"].status == "PASS"
    assert components["semantic_advisory_contract"].status == "PASS"
    assert components["feedbackwriter_governance"].status == "PASS"
    assert components["semanticbridge_gate"].status == "PASS"

    assert all(
        component.canonical_authority_writes_allowed is not True
        for component in artifact.components
    )


def test_advisory_governance_rollup_cli_writes_artifact(tmp_path):
    output = tmp_path / "latest_advisory_governance_rollup.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/advisory_governance_rollup.py",
            "--output",
            str(output),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["source"] == "advisory_governance_rollup"
    assert payload["status"] == "PASS"
    assert payload["components_checked"] == 4
    assert output.exists()
