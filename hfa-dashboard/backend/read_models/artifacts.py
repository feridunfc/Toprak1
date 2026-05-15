"""Read-only artifact/claim-check vault read model."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


ARTIFACT_HINTS = ("artifact", "claim", "completion", "execution")


@dataclass(frozen=True)
class ArtifactVaultReadModel:
    repo_root: Path

    @classmethod
    def from_env(cls) -> "ArtifactVaultReadModel":
        from read_models.authority_audit import AuthorityAuditReader
        return cls(repo_root=AuthorityAuditReader.from_env().repo_root)

    def snapshot(self, *, limit: int = 25) -> dict[str, Any]:
        candidates: list[dict[str, Any]] = []
        roots = [
            self.repo_root / "hfa-core" / "src" / "hfa" / "events",
            self.repo_root / "artifacts",
            self.repo_root / ".hfa" / "artifacts",
        ]
        for root in roots:
            if not root.exists():
                continue
            for path in root.rglob("*"):
                if not path.is_file():
                    continue
                lower = path.name.lower()
                if not any(hint in lower for hint in ARTIFACT_HINTS):
                    continue
                stat = path.stat()
                candidates.append({
                    "path": path.relative_to(self.repo_root).as_posix(),
                    "size_bytes": stat.st_size,
                    "modified_time": int(stat.st_mtime),
                })
        candidates.sort(key=lambda item: (item["modified_time"], item["path"]), reverse=True)
        return {
            "mode": "read-only",
            "count": len(candidates),
            "items": candidates[: max(0, min(limit, 200))],
            "note": "Dashboard-B lists local artifact-related files only; it does not read sealed payload bodies.",
        }
