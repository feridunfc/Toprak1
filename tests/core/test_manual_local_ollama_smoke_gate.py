from __future__ import annotations

from types import SimpleNamespace

from scripts.ironclad_manual_local_ollama_smoke import build_local_ollama_smoke_artifact


def test_local_ollama_smoke_blocks_by_default_without_call():
    artifact = build_local_ollama_smoke_artifact({})

    assert artifact["status"] == "BLOCKED"
    assert artifact["manual_only"] is True
    assert artifact["local_model_call_attempted"] is False
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False


def test_local_ollama_smoke_ready_without_execute():
    artifact = build_local_ollama_smoke_artifact(
        {"IRONCLAD_LOCAL_OLLAMA_SMOKE": "1"},
        execute=False,
    )

    assert artifact["status"] == "READY"
    assert artifact["local_model_call_attempted"] is False
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert "pass --execute" in artifact["blocked_reason"]


def test_local_ollama_smoke_execute_uses_injected_runner():
    calls = []

    def runner(cmd):
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stdout="IRONCLAD_LOCAL_SMOKE_OK", stderr="")

    artifact = build_local_ollama_smoke_artifact(
        {"IRONCLAD_LOCAL_OLLAMA_SMOKE": "1", "IRONCLAD_OLLAMA_MODEL": "llama3.2:1b"},
        execute=True,
        runner=runner,
    )

    assert artifact["status"] == "PASS"
    assert artifact["provider"] == "ollama"
    assert artifact["model"] == "llama3.2:1b"
    assert artifact["local_model_call_attempted"] is True
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert artifact["output_text_present"] is True
    assert artifact["output_text_value_exposed"] is False
    assert calls and calls[0][:3] == ["ollama", "run", "llama3.2:1b"]


def test_local_ollama_smoke_execute_failure_is_reported_without_output_exposure():
    def runner(cmd):
        return SimpleNamespace(returncode=1, stdout="bad output", stderr="failure detail")

    artifact = build_local_ollama_smoke_artifact(
        {"IRONCLAD_LOCAL_OLLAMA_SMOKE": "1"},
        execute=True,
        runner=runner,
    )

    assert artifact["status"] == "FAILED"
    assert artifact["local_model_call_attempted"] is True
    assert artifact["network_call_attempted"] is False
    assert artifact["production_llm_call_attempted"] is False
    assert artifact["output_text_value_exposed"] is False
    assert artifact["stderr_present"] is True
