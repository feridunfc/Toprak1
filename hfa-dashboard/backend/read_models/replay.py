"""Read-only replay integrity read model."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from read_models.authority_audit import AuthorityAuditReader


@dataclass(frozen=True)
class ReplayReadModel:
    repo_root: Path

    @classmethod
    def from_env(cls) -> "ReplayReadModel":
        return cls(repo_root=AuthorityAuditReader.from_env().repo_root)

    def snapshot(self) -> dict[str, Any]:
        replay_compare = self.repo_root / "scripts" / "replay_compare.py"
        smoke = self.repo_root / "scripts" / "full_auto_v3_2_smoke.py"
        authority = AuthorityAuditReader(self.repo_root).dashboard()
        return {
            "mode": "read-only",
            "replay_compare_present": replay_compare.exists(),
            "smoke_runner_present": smoke.exists(),
            "authority_status": authority.get("authority_status"),
            "banned": authority.get("counts", {}).get("banned"),
            "risk_score": authority.get("risk_score"),
            "last_result": "not_executed_by_dashboard",
            "note": "Dashboard-B reports replay readiness only; it does not execute replay.",
        }
