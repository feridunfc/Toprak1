import json
from pathlib import Path
import subprocess
import sys

from scripts.recovery_auto_resume_guardrail import audit_repo


def test_recovery_auto_resume_guardrail_passes_repo():
    artifact = audit_repo(Path("."))

    assert artifact.status == "PASS"
    assert artifact.banned_findings_count == 0


def test_recovery_auto_resume_guardrail_cli_writes_artifact(tmp_path):
    output = tmp_path / "guardrail.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/recovery_auto_resume_guardrail.py",
            "--output",
            str(output),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["source"] == "recovery_auto_resume_guardrail"
    assert payload["status"] == "PASS"
    assert payload["banned_findings_count"] == 0
    assert output.exists()
