"""Read-only quarantine snapshot read model."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from read_models.authority_audit import AuthorityAuditReader


@dataclass(frozen=True)
class QuarantineReadModel:
    repo_root: Path

    @classmethod
    def from_env(cls) -> "QuarantineReadModel":
        return cls(repo_root=AuthorityAuditReader.from_env().repo_root)

    def snapshot(self) -> dict[str, Any]:
        authority = AuthorityAuditReader(self.repo_root).dashboard()
        findings = authority.get("findings", [])
        banned = [f for f in findings if str(f.get("severity", "")).lower() == "banned"]
        return {
            "mode": "read-only",
            "pending_count": 0,
            "known_banned_authority_findings": len(banned),
            "items": [],
            "note": "No live quarantine store is read in Dashboard-B MVP; no write/action endpoint exists.",
        }
