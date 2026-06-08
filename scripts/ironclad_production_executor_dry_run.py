from __future__ import annotations

import os
import sys
from typing import Any

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ironclad_executor_mode import executor_mode_boundary

DEFAULT_PROVIDER = "openai"
PROVIDER_KEY_ENV = {
    "openai": "OPENAI_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
}


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def requested_provider(env: dict[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    provider = (source.get("IRONCLAD_LLM_PROVIDER") or DEFAULT_PROVIDER).strip().lower()
    if provider not in PROVIDER_KEY_ENV:
        return DEFAULT_PROVIDER
    return provider


def dry_run_enabled(env: dict[str, str] | None = None) -> bool:
    source = env if env is not None else os.environ
    # Fail closed: dry-run remains enabled unless explicitly disabled in a future sprint.
    raw = source.get("IRONCLAD_EXECUTOR_DRY_RUN")
    if raw is None or raw.strip() == "":
        return True
    return _truthy(raw)


def build_production_executor_dry_run_artifact(env: dict[str, str] | None = None) -> dict[str, Any]:
    source = env if env is not None else os.environ
    boundary = executor_mode_boundary(source)
    provider = requested_provider(source)
    key_env_name = PROVIDER_KEY_ENV[provider]
    api_key_present = bool((source.get(key_env_name) or "").strip())
    dry_run = dry_run_enabled(source)

    requested_enabled = boundary["requested_executor_mode"] == "production_llm_enabled"
    real_allowed = requested_enabled and not boundary["real_llm_blocked"]

    blocked_reasons: list[str] = []
    if boundary["real_llm_blocked"]:
        blocked_reasons.append(boundary["blocked_reason"] or "real LLM execution blocked by executor mode boundary")
    if dry_run:
        blocked_reasons.append("dry-run boundary prevents production LLM network calls")
    if not api_key_present:
        blocked_reasons.append(f"{key_env_name} is not present")

    return {
        "source": "production_executor_dry_run_boundary",
        "status": "PASS",
        "executor_mode": boundary["executor_mode"],
        "requested_executor_mode": boundary["requested_executor_mode"],
        "real_llm_blocked": bool(boundary["real_llm_blocked"]),
        "executor_dry_run": dry_run,
        "production_executor_adapter_visible": True,
        "provider": provider,
        "provider_key_env": key_env_name,
        "api_key_present": api_key_present,
        "api_key_value_exposed": False,
        "api_key_redacted": True,
        "network_call_attempted": False,
        "production_llm_call_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "real_llm_allowed_by_mode": real_allowed,
        "ready_for_manual_real_smoke": bool(real_allowed and api_key_present and not dry_run),
        "blocked_reason": "; ".join(reason for reason in blocked_reasons if reason) or None,
        "failing_reasons": [],
    }


def main() -> int:
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Emit production executor dry-run boundary artifact.")
    parser.add_argument("--json", action="store_true", help="Print JSON artifact.")
    args = parser.parse_args()

    artifact = build_production_executor_dry_run_artifact()
    if args.json:
        print(json.dumps(artifact, indent=2, sort_keys=True))
    else:
        print(
            f"status={artifact['status']} provider={artifact['provider']} "
            f"executor_mode={artifact['executor_mode']} dry_run={artifact['executor_dry_run']} "
            f"network_call_attempted={artifact['network_call_attempted']}"
        )
    return 0 if artifact["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
