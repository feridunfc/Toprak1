from __future__ import annotations

from scripts.ironclad_executor_mode import (
    executor_mode_boundary,
    apply_executor_mode_boundary,
)


def test_executor_mode_defaults_to_fake_and_no_real_llm_attempt() -> None:
    boundary = executor_mode_boundary({})

    assert boundary["executor_mode"] == "fake"
    assert boundary["fake_executor_used"] is True
    assert boundary["production_llm_call_attempted"] is False
    assert boundary["real_llm_blocked"] is False


def test_executor_mode_blocks_real_llm_without_explicit_allow_flag() -> None:
    boundary = executor_mode_boundary({"IRONCLAD_EXECUTOR_MODE": "production_llm_enabled"})

    assert boundary["executor_mode"] == "production_llm_blocked"
    assert boundary["requested_executor_mode"] == "production_llm_enabled"
    assert boundary["fake_executor_used"] is False
    assert boundary["production_llm_call_attempted"] is False
    assert boundary["real_llm_blocked"] is True
    assert "IRONCLAD_ALLOW_REAL_LLM=1" in boundary["blocked_reason"]


def test_executor_mode_enabled_requires_explicit_allow_but_does_not_call_llm() -> None:
    boundary = executor_mode_boundary(
        {
            "IRONCLAD_EXECUTOR_MODE": "production_llm_enabled",
            "IRONCLAD_ALLOW_REAL_LLM": "1",
        }
    )

    assert boundary["executor_mode"] == "production_llm_enabled"
    assert boundary["production_llm_call_attempted"] is False
    assert boundary["real_llm_blocked"] is False


def test_apply_executor_mode_boundary_adds_visible_fields() -> None:
    payload = apply_executor_mode_boundary(
        {
            "source": "test",
            "fake_executor_used": True,
            "production_llm_call_attempted": False,
        },
        {},
    )

    assert payload["executor_mode"] == "fake"
    assert payload["requested_executor_mode"] == "fake"
    assert payload["fake_executor_used"] is True
    assert payload["production_llm_call_attempted"] is False
    assert payload["real_llm_blocked"] is False
