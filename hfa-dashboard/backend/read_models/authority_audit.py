"""Read-only projection adapter for scripts/authority_audit.py."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class AuthorityAuditReadError(RuntimeError):
    pass


@dataclass(frozen=True)
class AuthorityAuditReader:
    repo_root: Path

    @classmethod
    def from_env(cls) -> "AuthorityAuditReader":
        return cls(repo_root=Path(os.getenv("HFA_DASHBOARD_REPO_ROOT", ".")).resolve())

    def _script_path(self) -> Path:
        script = self.repo_root / "scripts" / "authority_audit.py"
        if not script.exists():
            raise AuthorityAuditReadError(f"authority audit script not found: {script}")
        return script

    def _run_json(self, args: list[str]) -> Any:
        cmd = [sys.executable, str(self._script_path()), "--repo-root", str(self.repo_root), *args]
        completed = subprocess.run(cmd, cwd=self.repo_root, check=False, capture_output=True, text=True)
        if completed.returncode not in (0, 1):
            raise AuthorityAuditReadError(
                f"authority audit failed: returncode={completed.returncode} stderr={completed.stderr.strip()}"
            )
        try:
            return json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise AuthorityAuditReadError("authority audit did not emit valid JSON") from exc

    def dashboard(self) -> dict[str, Any]:
        payload = self._run_json(["--format", "dashboard", "--fail-on", "none"])
        if not isinstance(payload, dict):
            raise AuthorityAuditReadError("dashboard payload is not an object")
        return payload

    def heatmap(self) -> list[dict[str, Any]]:
        payload = self._run_json(["--heatmap", "--fail-on", "none"])
        if not isinstance(payload, list):
            raise AuthorityAuditReadError("heatmap payload is not a list")
        return payload

    def findings(self, *, limit: int = 25, severity: str | None = None) -> list[dict[str, Any]]:
        findings = self.dashboard().get("findings", [])
        if not isinstance(findings, list):
            raise AuthorityAuditReadError("findings payload is not a list")
        if severity:
            findings = [f for f in findings if str(f.get("severity", "")).lower() == severity.lower()]
        return findings[: max(0, min(limit, 500))]
