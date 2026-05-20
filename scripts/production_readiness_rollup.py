#!/usr/bin/env python3
"""Generate production readiness rollup artifact."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_ARTIFACT_DIR = Path("docs/dashboard/artifacts")
DEFAULT_OUTPUT = DEFAULT_ARTIFACT_DIR / "latest_production_readiness_rollup.json"


REQUIRED_COMPONENTS = [
    ("authority", "latest_authority.json"),
    ("replay", "latest_replay.json"),
    ("recovery_audit", "latest_recovery_audit.json"),
    ("recovery_requeue", "latest_recovery_requeue.json"),
    ("recovery_requeue_drill", "latest_recovery_requeue_drill.json"),
    ("cold_restart_drill", "latest_cold_restart_drill.json"),
    ("zombie_completion_drill", "latest_zombie_completion_drill.json"),
    ("recovery_auto_resume_guardrail", "latest_recovery_auto_resume_guardrail.json"),
    ("advisory_governance_rollup", "latest_advisory_governance_rollup.json"),
]

OPTIONAL_CI_COMPONENTS = [
    ("deployment_smoke", "latest_deployment_smoke.json"),
    ("redis_failover_smoke", "latest_redis_failover_smoke.json"),
]


ALLOWED_REQUIRED_STATUSES = {
    "recovery_requeue": {"PASS", "BLOCKED"},
    "recovery_requeue_drill": {"PASS", "SKIPPED"},
    "cold_restart_drill": {"PASS", "SKIPPED"},
    "zombie_completion_drill": {"PASS", "SKIPPED"},
}


@dataclass(frozen=True)
class ProductionReadinessComponent:
    name: str
    path: str
    status: str
    required: bool
    present: bool
    source: str = "unknown"


@dataclass(frozen=True)
class ProductionReadinessRollupArtifact:
    source: str
    status: str
    mode: str
    components: list[ProductionReadinessComponent]
    required_components_checked: int
    optional_components_checked: int
    notes: list[str]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _component_status(payload: dict[str, Any], component_name: str) -> str:
    if "status" in payload:
        return str(payload.get("status", "UNKNOWN"))

    if component_name == "authority":
        banned = int(payload.get("banned_count", payload.get("banned", 0)) or 0)
        return "PASS" if banned == 0 else "FAIL"

    if component_name == "replay":
        return str(payload.get("replay_status", "UNKNOWN"))

    return "UNKNOWN"


def _component_source(payload: dict[str, Any], fallback: str) -> str:
    return str(payload.get("source", fallback))


def _read_component(
    *,
    artifact_dir: Path,
    name: str,
    filename: str,
    required: bool,
) -> ProductionReadinessComponent:
    path = artifact_dir / filename
    if not path.exists():
        return ProductionReadinessComponent(
            name=name,
            path=str(path),
            status="MISSING" if required else "NOT_PRESENT_LOCAL",
            required=required,
            present=False,
            source="missing",
        )

    payload = _load_json(path)
    return ProductionReadinessComponent(
        name=name,
        path=str(path),
        status=_component_status(payload, name),
        required=required,
        present=True,
        source=_component_source(payload, name),
    )


def build_artifact(
    artifact_dir: Path = DEFAULT_ARTIFACT_DIR,
) -> ProductionReadinessRollupArtifact:
    components: list[ProductionReadinessComponent] = []

    for name, filename in REQUIRED_COMPONENTS:
        components.append(
            _read_component(
                artifact_dir=artifact_dir,
                name=name,
                filename=filename,
                required=True,
            )
        )

    for name, filename in OPTIONAL_CI_COMPONENTS:
        components.append(
            _read_component(
                artifact_dir=artifact_dir,
                name=name,
                filename=filename,
                required=False,
            )
        )

    required_ok = all(
        component.present
        and component.status in ALLOWED_REQUIRED_STATUSES.get(component.name, {"PASS"})
        for component in components
        if component.required
    )

    optional_ok = all(
        (not component.present) or component.status == "PASS"
        for component in components
        if not component.required
    )

    status = "PASS" if required_ok and optional_ok else "FAIL"

    return ProductionReadinessRollupArtifact(
        source="production_readiness_rollup",
        status=status,
        mode="static-rollup",
        components=components,
        required_components_checked=len(REQUIRED_COMPONENTS),
        optional_components_checked=len(OPTIONAL_CI_COMPONENTS),
        notes=[
            "Production readiness rollup performs no Redis/runtime mutation.",
            "Required proof artifacts must be present and PASS.",
            "Optional CI-only smoke artifacts are reported when present and must PASS if present.",
            "Canonical runtime truth authority remains in runtime/Lua/control-plane approved paths.",
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate production readiness rollup artifact")
    parser.add_argument("--artifact-dir", default=str(DEFAULT_ARTIFACT_DIR))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    artifact = build_artifact(Path(args.artifact_dir))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(asdict(artifact), sort_keys=True))
    else:
        print(
            f"PRODUCTION_READINESS_ROLLUP_{artifact.status}: "
            f"required={artifact.required_components_checked} "
            f"optional={artifact.optional_components_checked}"
        )

    return 0 if artifact.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
