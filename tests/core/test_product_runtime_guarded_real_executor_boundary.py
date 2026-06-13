from __future__ import annotations

from scripts.ironclad_manual_provider_smoke_guard import CONFIRMATION_VALUE
from scripts.ironclad_product_runtime import build_product_runtime_guarded_real_executor_boundary


class BuiltExecutor:
    pass


def test_product_runtime_real_executor_boundary_blocks_by_default_without_factory():
    calls = []

    artifact = build_product_runtime_guarded_real_executor_boundary(
        {},
        message="hello product runtime",
        executor_builder=lambda config: calls.append(config) or BuiltExecutor(),
    )

    assert artifact["status"] == "BLOCKED"
    assert artifact["guarded_real_executor_runtime_path_supported"] is True
    assert artifact["guarded_real_executor_requested"] is False
    assert artifact["provider_guard_required"] is True
    assert artifact["provider_guard_ready"] is False
    assert artifact["executor_factory_attempted"] is False
    assert artifact["real_executor_boundary_reachable"] is False
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert calls == []


def test_product_runtime_real_executor_boundary_blocks_when_requested_without_provider_guard_ready():
    calls = []

    artifact = build_product_runtime_guarded_real_executor_boundary(
        {
            "IRONCLAD_PRODUCT_RUNTIME_REAL_EXECUTOR": "1",
            "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
            "IRONCLAD_ALLOW_REAL_LLM": "1",
            "IRONCLAD_EXECUTOR_DRY_RUN": "0",
            "OPENAI_API_KEY": "sk-test-secret",
        },
        message="hello product runtime",
        executor_builder=lambda config: calls.append(config) or BuiltExecutor(),
    )

    assert artifact["status"] == "BLOCKED"
    assert artifact["guarded_real_executor_requested"] is True
    assert artifact["provider_guard_ready"] is False
    assert artifact["executor_factory_attempted"] is False
    assert artifact["real_executor_boundary_reachable"] is False
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert "manual provider smoke guard is not READY" in artifact["blocked_reason"]
    assert "sk-test-secret" not in str(artifact)
    assert calls == []


def test_product_runtime_real_executor_boundary_reachable_when_provider_guard_ready():
    calls = []

    def builder(config):
        calls.append(config)
        return BuiltExecutor()

    artifact = build_product_runtime_guarded_real_executor_boundary(
        {
            "IRONCLAD_PRODUCT_RUNTIME_REAL_EXECUTOR": "1",
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
        message="hello product runtime",
        executor_builder=builder,
    )

    assert artifact["status"] == "READY"
    assert artifact["guarded_real_executor_runtime_path_supported"] is True
    assert artifact["guarded_real_executor_requested"] is True
    assert artifact["provider_guard_required"] is True
    assert artifact["provider_guard_status"] == "READY"
    assert artifact["provider_guard_ready"] is True
    assert artifact["executor_factory_attempted"] is True
    assert artifact["real_executor_boundary_reachable"] is True
    assert artifact["executor_type"] == "BuiltExecutor"
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert artifact["prompt_value_exposed"] is False
    assert artifact["output_text_value_exposed"] is False
    assert artifact["api_key_value_exposed"] is False
    assert "sk-test-secret" not in str(artifact)
    assert len(calls) == 1
    assert calls[0]["executor_mode"] == "openai"
    assert calls[0]["openai_model"] == "gpt-4o-mini"
    assert calls[0]["openai_api_key"] == "sk-test-secret"
