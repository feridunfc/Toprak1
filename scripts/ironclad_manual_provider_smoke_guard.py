from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ironclad_executor_mode import executor_mode_boundary
from scripts.ironclad_production_executor_dry_run import PROVIDER_KEY_ENV, requested_provider, dry_run_enabled

CONFIRMATION_VALUE = "I_UNDERSTAND_THIS_MAY_CALL_A_PAID_PROVIDER"
DEFAULT_ALLOWED_PROVIDERS = {"openai"}
DEFAULT_ALLOWED_MODELS = {"gpt-4o-mini", "gpt-4.1-mini"}
DEFAULT_MAX_TOKENS = 128
DEFAULT_MAX_COST_CENTS = 5


def _csv_set(value: str | None, default: set[str]) -> set[str]:
    if not value or not value.strip():
        return set(default)
    return {part.strip().lower() for part in value.split(",") if part.strip()}


def _int_env(source: dict[str, str], key: str, default: int) -> int:
    raw = (source.get(key) or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return -1
    return parsed


def _provider_model(source: dict[str, str]) -> tuple[str, str]:
    provider = requested_provider(source)
    if provider == "anthropic":
        model = (source.get("ANTHROPIC_MODEL") or source.get("IRONCLAD_LLM_MODEL") or "claude-3-5-haiku-latest").strip()
    else:
        model = (source.get("OPENAI_MODEL") or source.get("IRONCLAD_LLM_MODEL") or "gpt-4o-mini").strip()
    return provider, model


def build_manual_provider_smoke_guard_artifact(env: dict[str, str] | None = None) -> dict[str, Any]:
    source = env if env is not None else os.environ
    boundary = executor_mode_boundary(source)
    provider, model = _provider_model(source)

    allowed_providers = _csv_set(source.get("IRONCLAD_ALLOWED_PROVIDERS"), DEFAULT_ALLOWED_PROVIDERS)
    allowed_models = _csv_set(source.get("IRONCLAD_ALLOWED_MODELS"), DEFAULT_ALLOWED_MODELS)

    provider_allowed = provider.lower() in allowed_providers
    model_allowed = model.lower() in allowed_models

    provider_key_env = PROVIDER_KEY_ENV.get(provider, "OPENAI_API_KEY")
    api_key_present = bool((source.get(provider_key_env) or "").strip())

    dry_run = dry_run_enabled(source)
    max_tokens = _int_env(source, "IRONCLAD_REAL_SMOKE_MAX_TOKENS", DEFAULT_MAX_TOKENS)
    max_cost_cents = _int_env(source, "IRONCLAD_REAL_SMOKE_MAX_COST_CENTS", DEFAULT_MAX_COST_CENTS)

    token_guard_passed = 1 <= max_tokens <= DEFAULT_MAX_TOKENS
    cost_guard_passed = 1 <= max_cost_cents <= DEFAULT_MAX_COST_CENTS
    budget_guard_passed = token_guard_passed and cost_guard_passed

    operator_confirmed = (source.get("IRONCLAD_MANUAL_PROVIDER_SMOKE_CONFIRM") or "").strip() == CONFIRMATION_VALUE

    base_ready = (
        boundary["executor_mode"] == "production_llm_enabled"
        and not boundary["real_llm_blocked"]
        and not dry_run
        and api_key_present
    )

    guard_passed = (
        base_ready
        and provider_allowed
        and model_allowed
        and budget_guard_passed
        and operator_confirmed
    )

    blocking_reasons: list[str] = []
    if boundary["real_llm_blocked"]:
        blocking_reasons.append(boundary["blocked_reason"] or "real LLM blocked")
    if boundary["executor_mode"] != "production_llm_enabled":
        blocking_reasons.append("IRONCLAD_EXECUTOR_MODE=production_llm_enabled is required")
    if dry_run:
        blocking_reasons.append("IRONCLAD_EXECUTOR_DRY_RUN=0 is required")
    if not api_key_present:
        blocking_reasons.append(f"{provider_key_env} is not present")
    if not provider_allowed:
        blocking_reasons.append(f"provider {provider!r} is not allowlisted")
    if not model_allowed:
        blocking_reasons.append(f"model {model!r} is not allowlisted")
    if not token_guard_passed:
        blocking_reasons.append("IRONCLAD_REAL_SMOKE_MAX_TOKENS must be between 1 and 128")
    if not cost_guard_passed:
        blocking_reasons.append("IRONCLAD_REAL_SMOKE_MAX_COST_CENTS must be between 1 and 5")
    if not operator_confirmed:
        blocking_reasons.append("manual provider smoke confirmation is required")

    return {
        "source": "manual_provider_smoke_guard",
        "status": "READY" if guard_passed else "BLOCKED",
        "manual_only": True,
        "ci_safe": True,
        "executor_mode": boundary["executor_mode"],
        "requested_executor_mode": boundary["requested_executor_mode"],
        "real_llm_blocked": bool(boundary["real_llm_blocked"]),
        "executor_dry_run": dry_run,
        "provider": provider,
        "model": model,
        "provider_allowed": provider_allowed,
        "model_allowed": model_allowed,
        "allowed_providers": sorted(allowed_providers),
        "allowed_models": sorted(allowed_models),
        "provider_key_env": provider_key_env,
        "api_key_present": api_key_present,
        "api_key_value_exposed": False,
        "api_key_redacted": True,
        "max_tokens": max_tokens,
        "max_cost_cents": max_cost_cents,
        "token_guard_passed": token_guard_passed,
        "cost_guard_passed": cost_guard_passed,
        "budget_guard_passed": budget_guard_passed,
        "operator_confirmed": operator_confirmed,
        "manual_provider_smoke_ready": guard_passed,
        "network_call_attempted": False,
        "production_llm_call_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "prompt_value_exposed": False,
        "output_text_value_exposed": False,
        "blocked_reason": "; ".join(blocking_reasons) or None,
        "failing_reasons": [],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Manual real provider smoke guard.")
    parser.add_argument("--json", action="store_true", help="Print JSON artifact.")
    args = parser.parse_args()

    artifact = build_manual_provider_smoke_guard_artifact()
    if args.json:
        print(json.dumps(artifact, indent=2, sort_keys=True))
    else:
        print(
            f"status={artifact['status']} provider={artifact['provider']} model={artifact['model']} "
            f"budget_guard_passed={artifact['budget_guard_passed']} "
            f"network_call_attempted={artifact['network_call_attempted']}"
        )

    return 0 if artifact["status"] in {"BLOCKED", "READY"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
