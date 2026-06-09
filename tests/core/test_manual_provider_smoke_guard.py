from __future__ import annotations

from scripts.ironclad_manual_provider_smoke_guard import (
    CONFIRMATION_VALUE,
    build_manual_provider_smoke_guard_artifact,
)


def test_manual_provider_smoke_guard_blocks_by_default_without_network_call():
    artifact = build_manual_provider_smoke_guard_artifact({})

    assert artifact["status"] == "BLOCKED"
    assert artifact["manual_only"] is True
    assert artifact["ci_safe"] is True
    assert artifact["manual_provider_smoke_ready"] is False
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert artifact["api_key_value_exposed"] is False
    assert artifact["prompt_value_exposed"] is False
    assert artifact["output_text_value_exposed"] is False


def test_manual_provider_smoke_guard_ready_only_with_all_explicit_flags_and_budget():
    artifact = build_manual_provider_smoke_guard_artifact(
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
        }
    )

    assert artifact["status"] == "READY"
    assert artifact["manual_provider_smoke_ready"] is True
    assert artifact["provider_allowed"] is True
    assert artifact["model_allowed"] is True
    assert artifact["budget_guard_passed"] is True
    assert artifact["operator_confirmed"] is True
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert "sk-test-secret" not in str(artifact)


def test_manual_provider_smoke_guard_blocks_unallowlisted_model():
    artifact = build_manual_provider_smoke_guard_artifact(
        {
            "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
            "IRONCLAD_ALLOW_REAL_LLM": "1",
            "IRONCLAD_EXECUTOR_DRY_RUN": "0",
            "OPENAI_API_KEY": "sk-test-secret",
            "OPENAI_MODEL": "gpt-4o",
            "IRONCLAD_ALLOWED_PROVIDERS": "openai",
            "IRONCLAD_ALLOWED_MODELS": "gpt-4o-mini",
            "IRONCLAD_REAL_SMOKE_MAX_TOKENS": "64",
            "IRONCLAD_REAL_SMOKE_MAX_COST_CENTS": "3",
            "IRONCLAD_MANUAL_PROVIDER_SMOKE_CONFIRM": CONFIRMATION_VALUE,
        }
    )

    assert artifact["status"] == "BLOCKED"
    assert artifact["model_allowed"] is False
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False


def test_manual_provider_smoke_guard_blocks_over_budget():
    artifact = build_manual_provider_smoke_guard_artifact(
        {
            "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
            "IRONCLAD_ALLOW_REAL_LLM": "1",
            "IRONCLAD_EXECUTOR_DRY_RUN": "0",
            "OPENAI_API_KEY": "sk-test-secret",
            "OPENAI_MODEL": "gpt-4o-mini",
            "IRONCLAD_REAL_SMOKE_MAX_TOKENS": "999",
            "IRONCLAD_REAL_SMOKE_MAX_COST_CENTS": "99",
            "IRONCLAD_MANUAL_PROVIDER_SMOKE_CONFIRM": CONFIRMATION_VALUE,
        }
    )

    assert artifact["status"] == "BLOCKED"
    assert artifact["token_guard_passed"] is False
    assert artifact["cost_guard_passed"] is False
    assert artifact["budget_guard_passed"] is False
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False


def test_manual_provider_smoke_guard_blocks_without_operator_confirmation():
    artifact = build_manual_provider_smoke_guard_artifact(
        {
            "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
            "IRONCLAD_ALLOW_REAL_LLM": "1",
            "IRONCLAD_EXECUTOR_DRY_RUN": "0",
            "OPENAI_API_KEY": "sk-test-secret",
            "OPENAI_MODEL": "gpt-4o-mini",
            "IRONCLAD_REAL_SMOKE_MAX_TOKENS": "64",
            "IRONCLAD_REAL_SMOKE_MAX_COST_CENTS": "3",
        }
    )

    assert artifact["status"] == "BLOCKED"
    assert artifact["operator_confirmed"] is False
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
