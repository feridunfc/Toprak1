from __future__ import annotations

from scripts.ironclad_manual_provider_smoke_guard import CONFIRMATION_VALUE
from scripts.ironclad_manual_real_executor_smoke import build_manual_real_executor_smoke_artifact


def test_manual_real_executor_smoke_blocks_by_default_without_network_call():
    import asyncio

    artifact = asyncio.run(build_manual_real_executor_smoke_artifact({}))

    assert artifact["status"] == "BLOCKED"
    assert artifact["manual_only"] is True
    assert artifact["ci_safe"] is True
    assert artifact["manual_real_smoke_ready"] is False
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert artifact["api_key_value_exposed"] is False


def test_manual_real_executor_smoke_blocks_when_dry_run_enabled_even_if_real_mode_allowed():
    import asyncio

    artifact = asyncio.run(
        build_manual_real_executor_smoke_artifact(
            {
                "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
                "IRONCLAD_ALLOW_REAL_LLM": "1",
                "IRONCLAD_EXECUTOR_DRY_RUN": "1",
                "OPENAI_API_KEY": "sk-test-secret",
            }
        )
    )

    assert artifact["status"] == "BLOCKED"
    assert artifact["executor_mode"] == "production_llm_enabled"
    assert artifact["executor_dry_run"] is True
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert "sk-test-secret" not in str(artifact)


def test_manual_real_executor_smoke_blocks_without_provider_guard_ready():
    import asyncio

    artifact = asyncio.run(
        build_manual_real_executor_smoke_artifact(
            {
                "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
                "IRONCLAD_ALLOW_REAL_LLM": "1",
                "IRONCLAD_EXECUTOR_DRY_RUN": "0",
                "OPENAI_API_KEY": "sk-test-secret",
            },
            execute=True,
        )
    )

    assert artifact["status"] == "BLOCKED"
    assert artifact["manual_real_smoke_ready"] is True
    assert artifact["provider_guard_required"] is True
    assert artifact["provider_guard_ready"] is False
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert "manual provider smoke guard is not READY" in artifact["blocked_reason"]
    assert "sk-test-secret" not in str(artifact)


def test_manual_real_executor_smoke_ready_without_execute_when_provider_guard_ready():
    import asyncio

    artifact = asyncio.run(
        build_manual_real_executor_smoke_artifact(
            {
                "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
                "IRONCLAD_ALLOW_REAL_LLM": "1",
                "IRONCLAD_EXECUTOR_DRY_RUN": "0",
                "OPENAI_API_KEY": "sk-test-secret",
                "OPENAI_MODEL": "gpt-4o-mini",
                "IRONCLAD_ALLOWED_PROVIDERS": "openai",
                "IRONCLAD_ALLOWED_MODELS": "gpt-4o-mini",
                "IRONCLAD_REAL_SMOKE_MAX_TOKENS": "64",
                "IRONCLAD_REAL_SMOKE_MAX_COST_CENTS": "3",
                "IRONCLAD_MANUAL_PROVIDER_SMOKE_CONFIRM": CONFIRMATION_VALUE,
            },
            execute=False,
        )
    )

    assert artifact["status"] == "READY"
    assert artifact["manual_real_smoke_ready"] is True
    assert artifact["provider_guard_required"] is True
    assert artifact["provider_guard_status"] == "READY"
    assert artifact["provider_guard_ready"] is True
    assert artifact["provider_allowed"] is True
    assert artifact["model_allowed"] is True
    assert artifact["budget_guard_passed"] is True
    assert artifact["operator_confirmed"] is True
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert "pass --execute" in artifact["blocked_reason"]
    assert "sk-test-secret" not in str(artifact)


def test_manual_real_executor_smoke_execute_uses_injected_provider_call_only_when_ready():
    import asyncio

    calls = []

    async def fake_provider_call(prompt, env):
        calls.append({"prompt": prompt, "env": dict(env)})
        return {"status": "done", "output_text": "IRONCLAD_REAL_EXECUTOR_SMOKE_OK", "provider": "openai"}

    artifact = asyncio.run(
        build_manual_real_executor_smoke_artifact(
            {
                "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
                "IRONCLAD_ALLOW_REAL_LLM": "1",
                "IRONCLAD_EXECUTOR_DRY_RUN": "0",
                "OPENAI_API_KEY": "sk-test-secret",
                "OPENAI_MODEL": "gpt-4o-mini",
                "IRONCLAD_ALLOWED_PROVIDERS": "openai",
                "IRONCLAD_ALLOWED_MODELS": "gpt-4o-mini",
                "IRONCLAD_REAL_SMOKE_MAX_TOKENS": "64",
                "IRONCLAD_REAL_SMOKE_MAX_COST_CENTS": "3",
                "IRONCLAD_MANUAL_PROVIDER_SMOKE_CONFIRM": CONFIRMATION_VALUE,
            },
            execute=True,
            provider_call=fake_provider_call,
        )
    )

    assert artifact["status"] == "PASS"
    assert artifact["provider_guard_required"] is True
    assert artifact["provider_guard_status"] == "READY"
    assert artifact["provider_guard_ready"] is True
    assert artifact["budget_guard_passed"] is True
    assert artifact["operator_confirmed"] is True
    assert artifact["network_call_attempted"] is True
    assert artifact["production_llm_call_attempted"] is True
    assert artifact["output_text_present"] is True
    assert artifact["output_text_value_exposed"] is False
    assert len(calls) == 1
    assert calls[0]["env"]["OPENAI_API_KEY"] == "sk-test-secret"
    assert "sk-test-secret" not in str(artifact)
