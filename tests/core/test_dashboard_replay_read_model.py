import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / "hfa-dashboard" / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from read_models.replay import ReplayReadModel


def test_replay_read_model_uses_artifact_when_present(tmp_path):
    artifact = tmp_path / "docs" / "dashboard" / "artifacts" / "latest_replay.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text(
        json.dumps({
            "mode": "read-only",
            "source": "replay_compare",
            "replay_status": "PASS",
            "replay_clean": True,
            "deterministic_replay_ok": True,
        }),
        encoding="utf-8",
    )

    payload = ReplayReadModel(repo_root=tmp_path).snapshot()

    assert payload["mode"] == "read-only"
    assert payload["source"] == "artifact"
    assert payload["artifact_path"].replace("\\", "/") == "docs/dashboard/artifacts/latest_replay.json"
    assert payload["replay_status"] == "PASS"
    assert payload["last_result"] == "PASS"


def test_replay_read_model_reports_invalid_artifact_read_only(tmp_path):
    artifact = tmp_path / "docs" / "dashboard" / "artifacts" / "latest_replay.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("{not-json", encoding="utf-8")

    payload = ReplayReadModel(repo_root=tmp_path).snapshot()

    assert payload["mode"] == "read-only"
    assert payload["source"] == "artifact"
    assert payload["replay_status"] == "ARTIFACT_INVALID"
    assert payload["last_result"] == "artifact_invalid"
    assert "invalid replay artifact json" in payload["error"]


def test_replay_read_model_falls_back_to_readiness_without_artifact(tmp_path, monkeypatch):
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "replay_compare.py").write_text("", encoding="utf-8")
    (tmp_path / "scripts" / "full_auto_v3_2_smoke.py").write_text("", encoding="utf-8")

    from read_models.authority_audit import AuthorityAuditReader

    monkeypatch.setattr(
        AuthorityAuditReader,
        "dashboard",
        lambda self: {
            "authority_status": "PASS_WITH_RISKS",
            "counts": {"banned": 0},
            "risk_score": 943,
        },
    )

    payload = ReplayReadModel(repo_root=tmp_path).snapshot()

    assert payload["mode"] == "read-only"
    assert payload["source"] == "readiness"
    assert payload["replay_compare_present"] is True
    assert payload["smoke_runner_present"] is True
    assert payload["authority_status"] == "PASS_WITH_RISKS"
    assert payload["banned"] == 0
    assert payload["risk_score"] == 943
    assert payload["last_result"] == "not_executed_by_dashboard"
