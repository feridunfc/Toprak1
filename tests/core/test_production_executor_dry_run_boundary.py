from __future__ import annotations

from scripts.ironclad_production_executor_dry_run import build_production_executor_dry_run_artifact


def test_production_executor_dry_run_defaults_to_fake_without_network_call() -> None:
    artifact = build_production_executor_dry_run_artifact({})

    assert artifact["status"] == "PASS"
    assert artifact["executor_mode"] == "fake"
    assert artifact["requested_executor_mode"] == "fake"
    assert artifact["executor_dry_run"] is True
    assert artifact["provider"] == "openai"
    assert artifact["api_key_present"] is False
    assert artifact["api_key_value_exposed"] is False
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False


def test_production_executor_dry_run_blocks_enabled_mode_without_allow_flag() -> None:
    artifact = build_production_executor_dry_run_artifact(
        {
            "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
        }
    )

    assert artifact["executor_mode"] == "production_llm_blocked"
    assert artifact["requested_executor_mode"] == "production_llm_enabled"
    assert artifact["real_llm_blocked"] is True
    assert artifact["executor_dry_run"] is True
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert "IRONCLAD_ALLOW_REAL_LLM=1" in artifact["blocked_reason"]


def test_production_executor_dry_run_keeps_network_closed_even_when_real_mode_allowed() -> None:
    artifact = build_production_executor_dry_run_artifact(
        {
            "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
            "IRONCLAD_ALLOW_REAL_LLM": "1",
            "OPENAI_API_KEY": "sk-test-secret-value",
        }
    )

    assert artifact["executor_mode"] == "production_llm_enabled"
    assert artifact["real_llm_blocked"] is False
    assert artifact["api_key_present"] is True
    assert artifact["api_key_value_exposed"] is False
    assert "sk-test-secret-value" not in str(artifact)
    assert artifact["executor_dry_run"] is True
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert artifact["ready_for_manual_real_smoke"] is False
    assert "dry-run boundary prevents production LLM network calls" in artifact["blocked_reason"]


def test_production_executor_dry_run_supports_anthropic_key_presence_without_secret_exposure() -> None:
    artifact = build_production_executor_dry_run_artifact(
        {
            "IRONCLAD_LLM_PROVIDER": "anthropic",
            "ANTHROPIC_API_KEY": "anthropic-secret",
        }
    )

    assert artifact["provider"] == "anthropic"
    assert artifact["provider_key_env"] == "ANTHROPIC_API_KEY"
    assert artifact["api_key_present"] is True
    assert artifact["api_key_value_exposed"] is False
    assert "anthropic-secret" not in str(artifact)
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
