import json

from scripts.recovery_requeue import evaluate_artifact_proof


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_artifact_proof_fails_closed_when_artifacts_missing(tmp_path):
    result = evaluate_artifact_proof(
        run_id="run-1",
        replay_artifact=tmp_path / "missing_replay.json",
        authority_artifact=tmp_path / "missing_authority.json",
        recovery_audit_artifact=tmp_path / "missing_audit.json",
    )

    assert result.proof.allowed is False
    assert "replay_artifact_missing" in result.proof.reason
    assert "authority_artifact_missing" in result.proof.reason
    assert "recovery_audit_artifact_missing" in result.proof.reason
    assert result.replay_status == "missing"
    assert result.authority_status == "missing"
    assert result.recovery_audit_status == "missing"


def test_artifact_proof_fails_closed_when_candidate_missing(tmp_path):
    replay = tmp_path / "latest_replay.json"
    authority = tmp_path / "latest_authority.json"
    audit = tmp_path / "latest_recovery_audit.json"

    _write_json(replay, {"status": "PASS"})
    _write_json(authority, {"status": "PASS", "banned_count": 0})
    _write_json(audit, {"status": "PASS", "runs": []})

    result = evaluate_artifact_proof(
        run_id="run-1",
        replay_artifact=replay,
        authority_artifact=authority,
        recovery_audit_artifact=audit,
    )

    assert result.proof.allowed is False
    assert result.proof.reason == "recovery_audit_candidate_missing"
    assert result.replay_status == "PASS"
    assert result.authority_status == "PASS"
    assert result.recovery_audit_status == "PASS"


def test_artifact_proof_fails_closed_when_authority_has_banned_findings(tmp_path):
    replay = tmp_path / "latest_replay.json"
    authority = tmp_path / "latest_authority.json"
    audit = tmp_path / "latest_recovery_audit.json"

    _write_json(replay, {"status": "PASS"})
    _write_json(authority, {"status": "PASS", "banned_count": 1})
    _write_json(
        audit,
        {
            "status": "PASS",
            "runs": [{"run_id": "run-1", "stale": True, "missing_claim": False, "expired_claim": False}],
        },
    )

    result = evaluate_artifact_proof(
        run_id="run-1",
        replay_artifact=replay,
        authority_artifact=authority,
        recovery_audit_artifact=audit,
    )

    assert result.proof.allowed is False
    assert result.proof.reason == "authority_artifact_has_banned_findings"


def test_artifact_proof_allows_recovery_when_artifacts_pass_and_candidate_exists(tmp_path):
    replay = tmp_path / "latest_replay.json"
    authority = tmp_path / "latest_authority.json"
    audit = tmp_path / "latest_recovery_audit.json"

    _write_json(replay, {"status": "PASS"})
    _write_json(authority, {"status": "PASS", "banned_count": 0})
    _write_json(
        audit,
        {
            "status": "PASS",
            "runs": [{"run_id": "run-1", "stale": True, "missing_claim": False, "expired_claim": False}],
        },
    )

    result = evaluate_artifact_proof(
        run_id="run-1",
        replay_artifact=replay,
        authority_artifact=authority,
        recovery_audit_artifact=audit,
    )

    assert result.proof.allowed is True
    assert result.proof.reason == "artifact_proof_allows_auto_resume"
    assert result.replay_status == "PASS"
    assert result.authority_status == "PASS"
    assert result.recovery_audit_status == "PASS"
