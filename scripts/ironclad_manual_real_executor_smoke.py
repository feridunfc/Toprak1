from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ironclad_executor_mode import executor_mode_boundary
from scripts.ironclad_production_executor_dry_run import (
    PROVIDER_KEY_ENV,
    dry_run_enabled,
    requested_provider,
)

REQUIRED_ENV = {
    "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
    "IRONCLAD_ALLOW_REAL_LLM": "1",
    "IRONCLAD_EXECUTOR_DRY_RUN": "0",
}

DEFAULT_PROMPT = "Return exactly: IRONCLAD_REAL_EXECUTOR_SMOKE_OK"


def _env_matches(source: dict[str, str], key: str, expected: str) -> bool:
    return (source.get(key) or "").strip() == expected


def _base_gate(env: dict[str, str] | None = None) -> dict[str, Any]:
    source = env if env is not None else os.environ
    boundary = executor_mode_boundary(source)
    provider = requested_provider(source)
    provider_key_env = PROVIDER_KEY_ENV[provider]
    api_key_present = bool((source.get(provider_key_env) or "").strip())
    dry_run = dry_run_enabled(source)

    missing_requirements: list[str] = []
    for key, expected in REQUIRED_ENV.items():
        if not _env_matches(source, key, expected):
            missing_requirements.append(f"{key}={expected}")

    if not api_key_present:
        missing_requirements.append(provider_key_env)

    ready = (
        boundary["executor_mode"] == "production_llm_enabled"
        and not boundary["real_llm_blocked"]
        and not dry_run
        and api_key_present
        and not missing_requirements
    )

    blocked_reasons: list[str] = []
    if boundary["real_llm_blocked"]:
        blocked_reasons.append(boundary["blocked_reason"] or "real LLM blocked")
    if dry_run:
        blocked_reasons.append("IRONCLAD_EXECUTOR_DRY_RUN=0 is required for manual real smoke")
    if missing_requirements:
        blocked_reasons.append("missing manual real smoke requirements: " + ", ".join(missing_requirements))

    return {
        "source": "manual_real_executor_smoke_gate",
        "manual_only": True,
        "ci_safe": True,
        "status": "READY" if ready else "BLOCKED",
        "executor_mode": boundary["executor_mode"],
        "requested_executor_mode": boundary["requested_executor_mode"],
        "real_llm_blocked": bool(boundary["real_llm_blocked"]),
        "executor_dry_run": dry_run,
        "provider": provider,
        "provider_key_env": provider_key_env,
        "api_key_present": api_key_present,
        "api_key_value_exposed": False,
        "api_key_redacted": True,
        "required_env": dict(REQUIRED_ENV),
        "missing_requirements": missing_requirements,
        "manual_real_smoke_ready": ready,
        "network_call_attempted": False,
        "production_llm_call_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "blocked_reason": "; ".join(blocked_reasons) or None,
        "failing_reasons": [],
    }


async def _default_provider_call(prompt: str, env: dict[str, str]) -> dict[str, Any]:
    # This function is intentionally only reached when the manual gate is READY.
    # It imports OpenAIExecutor lazily so CI/blocking tests never instantiate it.
    from hfa.events.schema import RunRequestedEvent
    from hfa_worker.openai_executor import OpenAIExecutor

    provider = requested_provider(env)
    if provider != "openai":
        return {
            "status": "SKIPPED",
            "output_text": "",
            "provider": provider,
            "reason": "manual real smoke currently supports openai only",
        }

    executor = OpenAIExecutor(
        api_key=env["OPENAI_API_KEY"],
        model=env.get("OPENAI_MODEL", "gpt-4o-mini"),
        timeout_seconds=float(env.get("IRONCLAD_REAL_SMOKE_TIMEOUT_SECONDS", "30")),
    )
    event = RunRequestedEvent(
        run_id="manual-real-executor-smoke",
        tenant_id="manual-smoke",
        agent_type="openai",
        payload={"prompt": prompt},
    )
    result = await executor.execute(event)
    return {
        "status": result.status,
        "payload": result.payload,
        "output_text": (result.payload or {}).get("output_text", ""),
        "provider": "openai",
    }


async def build_manual_real_executor_smoke_artifact(
    env: dict[str, str] | None = None,
    *,
    prompt: str = DEFAULT_PROMPT,
    execute: bool = False,
    provider_call: Callable[[str, dict[str, str]], Awaitable[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    source = dict(env if env is not None else os.environ)
    artifact = _base_gate(source)
    artifact["prompt_value_exposed"] = False
    artifact["prompt_length"] = len(prompt)

    if not artifact["manual_real_smoke_ready"]:
        artifact["status"] = "BLOCKED"
        return artifact

    if not execute:
        artifact["status"] = "READY"
        artifact["blocked_reason"] = "manual gate ready; pass --execute to attempt real provider call"
        return artifact

    artifact["network_call_attempted"] = True
    artifact["production_llm_call_attempted"] = True

    call = provider_call or _default_provider_call
    try:
        result = await call(prompt, source)
    except Exception as exc:  # pragma: no cover - defensive manual smoke path
        artifact["status"] = "FAILED"
        artifact["provider_result_status"] = "exception"
        artifact["error_type"] = type(exc).__name__
        artifact["error_message_exposed"] = False
        artifact["failing_reasons"] = ["manual real executor smoke provider call failed"]
        return artifact

    artifact["provider_result_status"] = result.get("status")
    artifact["provider"] = result.get("provider", artifact["provider"])
    artifact["status"] = "PASS" if result.get("status") in {"done", "success", "PASS"} else "FAILED"
    artifact["output_text_present"] = bool(result.get("output_text"))
    artifact["output_text_value_exposed"] = False
    artifact["blocked_reason"] = None
    if artifact["status"] != "PASS":
        artifact["failing_reasons"] = ["manual real executor smoke did not return success"]
    return artifact


def main() -> int:
    parser = argparse.ArgumentParser(description="Manual-only real executor smoke gate.")
    parser.add_argument("--json", action="store_true", help="Print JSON artifact.")
    parser.add_argument("--execute", action="store_true", help="Attempt real provider call if all manual gates are satisfied.")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT)
    args = parser.parse_args()

    artifact = asyncio.run(
        build_manual_real_executor_smoke_artifact(
            prompt=args.prompt,
            execute=args.execute,
        )
    )

    if args.json:
        print(json.dumps(artifact, indent=2, sort_keys=True))
    else:
        print(
            f"status={artifact['status']} ready={artifact['manual_real_smoke_ready']} "
            f"network_call_attempted={artifact['network_call_attempted']} "
            f"production_llm_call_attempted={artifact['production_llm_call_attempted']}"
        )

    return 0 if artifact["status"] in {"BLOCKED", "READY", "PASS"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
