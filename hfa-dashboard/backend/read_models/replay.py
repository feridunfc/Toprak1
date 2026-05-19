"""Read-only replay integrity read model."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from read_models.authority_audit import AuthorityAuditReader


LATEST_REPLAY_ARTIFACT = Path("docs/dashboard/artifacts/latest_replay.json")


@dataclass(frozen=True)
class ReplayReadModel:
    repo_root: Path

    @classmethod
    def from_env(cls) -> "ReplayReadModel":
        return cls(repo_root=AuthorityAuditReader.from_env().repo_root)

    def _artifact_snapshot(self) -> dict[str, Any] | None:
        artifact_path = self.repo_root / LATEST_REPLAY_ARTIFACT
        if not artifact_path.exists():
            return None

        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return {
                "mode": "read-only",
                "source": "artifact",
                "artifact_path": str(LATEST_REPLAY_ARTIFACT),
                "replay_status": "ARTIFACT_INVALID",
                "last_result": "artifact_invalid",
                "error": f"invalid replay artifact json: {exc}",
                "note": "Replay artifact is present but invalid; dashboard remains read-only.",
            }

        if not isinstance(payload, dict):
            return {
                "mode": "read-only",
                "source": "artifact",
                "artifact_path": str(LATEST_REPLAY_ARTIFACT),
                "replay_status": "ARTIFACT_INVALID",
                "last_result": "artifact_invalid",
                "error": "replay artifact root is not an object",
                "note": "Replay artifact is present but invalid; dashboard remains read-only.",
            }

        payload.setdefault("mode", "read-only")
        payload["source"] = "artifact"
        payload["artifact_path"] = str(LATEST_REPLAY_ARTIFACT)
        payload.setdefault("last_result", payload.get("replay_status", "artifact_loaded"))
        payload.setdefault("note", "Dashboard replay evidence loaded from read-only artifact.")
        return payload

    def snapshot(self) -> dict[str, Any]:
        artifact = self._artifact_snapshot()
        if artifact is not None:
            return artifact

        replay_compare = self.repo_root / "scripts" / "replay_compare.py"
        smoke = self.repo_root / "scripts" / "full_auto_v3_2_smoke.py"
        authority = AuthorityAuditReader(self.repo_root).dashboard()
        return {
            "mode": "read-only",
            "source": "readiness",
            "replay_compare_present": replay_compare.exists(),
            "smoke_runner_present": smoke.exists(),
            "authority_status": authority.get("authority_status"),
            "banned": authority.get("counts", {}).get("banned"),
            "risk_score": authority.get("risk_score"),
            "last_result": "not_executed_by_dashboard",
            "note": "Dashboard-B reports replay readiness only; it does not execute replay.",
        }
