import json
import subprocess
import sys
from pathlib import Path

from scripts.production_readiness_rollup import (
    OPTIONAL_CI_COMPONENTS,
    REQUIRED_COMPONENTS,
    build_artifact,
)


def _write_artifact(root: Path, filename: str, status: str = "PASS", source: str = "test"):
    root.mkdir(parents=True, exist_ok=True)
    (root / filename).write_text(
        json.dumps({"status": status, "source": source}, sort_keys=True),
        encoding="utf-8",
    )


def test_production_readiness_rollup_passes_with_required_components(tmp_path):
    artifact_dir = tmp_path / "artifacts"

    for name, filename in REQUIRED_COMPONENTS:
        _write_artifact(artifact_dir, filename, status="PASS", source=name)

    artifact = build_artifact(artifact_dir)

    assert artifact.status == "PASS"
    assert artifact.required_components_checked == len(REQUIRED_COMPONENTS)
    assert artifact.optional_components_checked == len(OPTIONAL_CI_COMPONENTS)

    components = {component.name: component for component in artifact.components}
    assert components["authority"].status == "PASS"
    assert components["advisory_governance_rollup"].status == "PASS"
    assert components["deployment_smoke"].status == "NOT_PRESENT_LOCAL"


def test_production_readiness_rollup_fails_when_required_missing(tmp_path):
    artifact_dir = tmp_path / "artifacts"

    for name, filename in REQUIRED_COMPONENTS[1:]:
        _write_artifact(artifact_dir, filename, status="PASS", source=name)

    artifact = build_artifact(artifact_dir)

    assert artifact.status == "FAIL"
    components = {component.name: component for component in artifact.components}
    assert components["authority"].present is False
    assert components["authority"].status == "MISSING"


def test_production_readiness_rollup_fails_when_required_non_pass(tmp_path):
    artifact_dir = tmp_path / "artifacts"

    for name, filename in REQUIRED_COMPONENTS:
        _write_artifact(artifact_dir, filename, status="PASS", source=name)

    _write_artifact(artifact_dir, "latest_replay.json", status="FAIL", source="replay")

    artifact = build_artifact(artifact_dir)

    assert artifact.status == "FAIL"
    components = {component.name: component for component in artifact.components}
    assert components["replay"].status == "FAIL"


def test_production_readiness_rollup_cli_writes_artifact(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    output = tmp_path / "latest_production_readiness_rollup.json"

    for name, filename in REQUIRED_COMPONENTS:
        _write_artifact(artifact_dir, filename, status="PASS", source=name)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/production_readiness_rollup.py",
            "--artifact-dir",
            str(artifact_dir),
            "--output",
            str(output),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["source"] == "production_readiness_rollup"
    assert payload["status"] == "PASS"
    assert payload["required_components_checked"] == len(REQUIRED_COMPONENTS)
    assert output.exists()


def test_production_readiness_rollup_normalizes_ci_artifact_status_shapes(tmp_path):
    artifact_dir = tmp_path / "artifacts"

    for name, filename in REQUIRED_COMPONENTS:
        _write_artifact(artifact_dir, filename, status="PASS", source=name)

    # Authority dashboard artifacts may omit a top-level status and expose banned counts.
    (artifact_dir / "latest_authority.json").write_text(
        json.dumps({"source": "authority", "banned_count": 0}, sort_keys=True),
        encoding="utf-8",
    )

    # Replay artifacts expose replay_status.
    (artifact_dir / "latest_replay.json").write_text(
        json.dumps({"source": "replay_compare", "replay_status": "PASS"}, sort_keys=True),
        encoding="utf-8",
    )

    # Recovery requeue dry-run can safely block when no recovery candidate exists.
    (artifact_dir / "latest_recovery_requeue.json").write_text(
        json.dumps(
            {
                "source": "recovery_requeue",
                "status": "BLOCKED",
                "mode": "dry-run",
                "requeue_status": "NO_RECOVERY_CANDIDATE",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    # Fake Redis CI/staging paths may skip real-Redis drills.
    (artifact_dir / "latest_recovery_requeue_drill.json").write_text(
        json.dumps({"source": "recovery_requeue_drill", "status": "SKIPPED"}, sort_keys=True),
        encoding="utf-8",
    )

    artifact = build_artifact(artifact_dir)

    assert artifact.status == "PASS"

    components = {component.name: component for component in artifact.components}
    assert components["authority"].status == "PASS"
    assert components["replay"].status == "PASS"
    assert components["recovery_requeue"].status == "BLOCKED"
    assert components["recovery_requeue_drill"].status == "SKIPPED"
