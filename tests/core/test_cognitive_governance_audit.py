import json
import subprocess
import sys
from pathlib import Path

from scripts.cognitive_governance_audit import audit_repo


def test_cognitive_governance_audit_passes_repo():
    artifact = audit_repo(Path("."))

    assert artifact.status == "PASS"
    assert artifact.findings_count == 0
    assert artifact.checked_files > 0


def test_cognitive_governance_audit_cli_writes_artifact(tmp_path):
    output = tmp_path / "latest_cognitive_governance_audit.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/cognitive_governance_audit.py",
            "--output",
            str(output),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["source"] == "cognitive_governance_audit"
    assert payload["status"] == "PASS"
    assert payload["findings_count"] == 0
    assert payload["checked_files"] > 0
    assert output.exists()
