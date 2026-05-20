#!/usr/bin/env python3
"""Generate SemanticBridge gate enforcement artifact."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SEMANTIC_BRIDGE_PATH = (
    REPO_ROOT / "hfa-agents" / "src" / "hfa_agents" / "integration" / "semantic_bridge.py"
)


def _load_semantic_bridge_module() -> Any:
    """Load semantic_bridge.py without importing hfa_agents package __init__.

    Authority Gate installs only lightweight dependencies. Importing the package
    pulls pydantic-backed contracts that are not needed for this static artifact.
    """
    spec = importlib.util.spec_from_file_location(
        "semantic_bridge_contract_module",
        SEMANTIC_BRIDGE_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable_to_load_semantic_bridge:{SEMANTIC_BRIDGE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_semantic_bridge = _load_semantic_bridge_module()

ADVISORY_ONLY_SURFACE = _semantic_bridge.ADVISORY_ONLY_SURFACE
CANONICAL_AUTHORITY_WRITES_ALLOWED = _semantic_bridge.CANONICAL_AUTHORITY_WRITES_ALLOWED
SemanticBridge = _semantic_bridge.SemanticBridge
evaluate_semantic_gate_decision = _semantic_bridge.evaluate_semantic_gate_decision


DEFAULT_OUTPUT = Path("docs/dashboard/artifacts/latest_semanticbridge_gate.json")


@dataclass(frozen=True)
class SemanticBridgeGateCase:
    name: str
    allowed: bool
    reason: str
    replay_visible: bool
    audit_visible: bool


@dataclass(frozen=True)
class SemanticBridgeGateArtifact:
    source: str
    status: str
    mode: str
    advisory_only_surface: bool
    canonical_authority_writes_allowed: bool
    cases: list[SemanticBridgeGateCase]
    notes: list[str]


async def _boom(*args: Any, **kwargs: Any) -> Any:
    raise RuntimeError("boom")


async def _run_hook_cases() -> list[SemanticBridgeGateCase]:
    bridge_module = _semantic_bridge

    cases: list[SemanticBridgeGateCase] = []

    original = bridge_module.evaluate_gate_semantics
    try:
        bridge_module.evaluate_gate_semantics = None
        bridge = SemanticBridge(semantic_pipeline=None)
        verdict = await bridge.evaluate_gate({"event_id": "evt-hook-missing"})
        cases.append(
            SemanticBridgeGateCase(
                name="hook_unavailable",
                allowed=bool(verdict["allowed"]),
                reason=str(verdict["reason"]),
                replay_visible=bool(verdict["replay_visible"]),
                audit_visible=bool(verdict["audit_visible"]),
            )
        )

        bridge_module.evaluate_gate_semantics = _boom
        bridge = SemanticBridge(semantic_pipeline=None)
        verdict = await bridge.evaluate_gate({"event_id": "evt-hook-exception"})
        cases.append(
            SemanticBridgeGateCase(
                name="hook_exception",
                allowed=bool(verdict["allowed"]),
                reason=str(verdict["reason"]),
                replay_visible=bool(verdict["replay_visible"]),
                audit_visible=bool(verdict["audit_visible"]),
            )
        )
    finally:
        bridge_module.evaluate_gate_semantics = original

    return cases


async def build_artifact_async() -> SemanticBridgeGateArtifact:
    direct_cases = [
        (
            "missing_verdict",
            evaluate_semantic_gate_decision(None).as_dict(),
        ),
        (
            "low_confidence_allowed_verdict",
            evaluate_semantic_gate_decision(
                {
                    "mode": "gate",
                    "allowed": True,
                    "reason": "semantic_ok",
                    "confidence": 0.10,
                }
            ).as_dict(),
        ),
        (
            "high_confidence_allowed_verdict",
            evaluate_semantic_gate_decision(
                {
                    "mode": "gate",
                    "allowed": True,
                    "reason": "semantic_ok",
                    "confidence": 0.91,
                }
            ).as_dict(),
        ),
    ]

    cases: list[SemanticBridgeGateCase] = [
        SemanticBridgeGateCase(
            name=name,
            allowed=bool(verdict["allowed"]),
            reason=str(verdict["reason"]),
            replay_visible=bool(verdict["replay_visible"]),
            audit_visible=bool(verdict["audit_visible"]),
        )
        for name, verdict in direct_cases
    ]
    cases.extend(await _run_hook_cases())

    expected = {
        "missing_verdict": False,
        "low_confidence_allowed_verdict": False,
        "high_confidence_allowed_verdict": True,
        "hook_unavailable": False,
        "hook_exception": False,
    }
    cases_ok = all(case.allowed is expected[case.name] for case in cases)

    status = "PASS" if (
        ADVISORY_ONLY_SURFACE
        and not CANONICAL_AUTHORITY_WRITES_ALLOWED
        and cases_ok
    ) else "FAIL"

    return SemanticBridgeGateArtifact(
        source="semanticbridge_gate",
        status=status,
        mode="static-contract",
        advisory_only_surface=ADVISORY_ONLY_SURFACE,
        canonical_authority_writes_allowed=CANONICAL_AUTHORITY_WRITES_ALLOWED,
        cases=cases,
        notes=[
            "SemanticBridge remains advisory/non-authoritative.",
            "Gate decisions are explicit and structured.",
            "Gate failure fails closed for advisory gate metadata.",
            "Gate decisions do not mutate canonical runtime truth.",
        ],
    )


def build_artifact() -> SemanticBridgeGateArtifact:
    return asyncio.run(build_artifact_async())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate SemanticBridge gate artifact")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    artifact = build_artifact()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(asdict(artifact), sort_keys=True))
    else:
        print(f"SEMANTICBRIDGE_GATE_{artifact.status}: cases={len(artifact.cases)}")

    return 0 if artifact.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
