#!/usr/bin/env python3
"""Generate production readiness decision artifact."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_INPUT = Path("docs/dashboard/artifacts/latest_production_readiness_rollup.json")
DEFAULT_OUTPUT = Path("docs/dashboard/artifacts/latest_production_readiness_decision.json")


@dataclass(frozen=True)
class ProductionReadinessDecisionComponent:
    name: str
    status: str
    required: bool
    present: bool


@dataclass(frozen=True)
class ProductionReadinessDecisionArtifact:
    source: str
    status: str
    decision: str
    mode: str
    rollup_status: str
    reasons: list[str]
    components: list[ProductionReadinessDecisionComponent]
    notes: list[str]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _component_summary(payload: dict[str, Any]) -> list[ProductionReadinessDecisionComponent]:
    components = payload.get("components", [])
    if not isinstance(components, list):
        return []

    summaries: list[ProductionReadinessDecisionComponent] = []
    for item in components:
        if not isinstance(item, dict):
            continue
        summaries.append(
            ProductionReadinessDecisionComponent(
                name=str(item.get("name", "unknown")),
                status=str(item.get("status", "UNKNOWN")),
                required=bool(item.get("required", False)),
                present=bool(item.get("present", False)),
            )
        )
    return summaries


def build_artifact(input_path: Path = DEFAULT_INPUT) -> ProductionReadinessDecisionArtifact:
    reasons: list[str] = []

    if not input_path.exists():
        return ProductionReadinessDecisionArtifact(
            source="production_readiness_decision",
            status="PASS",
            decision="NOT_READY",
            mode="read-only-decision",
            rollup_status="MISSING",
            reasons=["production_readiness_rollup_missing"],
            components=[],
            notes=[
                "Decision gate performs no Redis/runtime mutation.",
                "Missing or malformed rollup fails closed to NOT_READY.",
            ],
        )

    try:
        payload = _load_json(input_path)
    except Exception:
        return ProductionReadinessDecisionArtifact(
            source="production_readiness_decision",
            status="PASS",
            decision="NOT_READY",
            mode="read-only-decision",
            rollup_status="MALFORMED",
            reasons=["production_readiness_rollup_malformed"],
            components=[],
            notes=[
                "Decision gate performs no Redis/runtime mutation.",
                "Missing or malformed rollup fails closed to NOT_READY.",
            ],
        )

    rollup_status = str(payload.get("status", "UNKNOWN"))
    components = _component_summary(payload)

    if rollup_status != "PASS":
        reasons.append(f"production_readiness_rollup_status:{rollup_status}")

    for component in components:
        if component.required and not component.present:
            reasons.append(f"required_component_missing:{component.name}")
        elif component.required and component.status not in {"PASS", "BLOCKED", "SKIPPED"}:
            reasons.append(f"required_component_not_ready:{component.name}:{component.status}")

    decision = "READY" if rollup_status == "PASS" and not reasons else "NOT_READY"

    return ProductionReadinessDecisionArtifact(
        source="production_readiness_decision",
        status="PASS",
        decision=decision,
        mode="read-only-decision",
        rollup_status=rollup_status,
        reasons=reasons,
        components=components,
        notes=[
            "Decision gate performs no Redis/runtime mutation.",
            "READY requires production readiness rollup PASS.",
            "NOT_READY is fail-closed for missing, malformed, or non-PASS rollup input.",
            "Decision artifact does not enable production auto-deployment.",
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate production readiness decision artifact")
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    artifact = build_artifact(Path(args.input))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(asdict(artifact), sort_keys=True))
    else:
        print(f"PRODUCTION_READINESS_DECISION_{artifact.decision}: reasons={len(artifact.reasons)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
