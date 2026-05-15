from __future__ import annotations

import os

import pytest

from hfa.events.completion_capture import LLM_COMPLETION_CAPTURED, capture_agent_completion
from hfa.events.execution_artifacts import seal_execution_artifact
from hfa_agents.base.agent_base import AgentBase
from hfa_agents.base.contracts import EnrichedEvent, ExecutionResult
from hfa_agents.roles.architect import ArchitectAgent
from hfa_agents.roles.coder import CoderAgent
from hfa_agents.roles.researcher import ResearcherAgent
from hfa_agents.roles.tester import TesterAgent


class FakeAppender:
    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.events = []

    async def append_event(self, *, run_id, event_type, worker_id=None, details=None):
        self.events.append({
            "run_id": run_id,
            "event_type": event_type,
            "worker_id": worker_id,
            "details": details or {},
        })
        return self.ok


class DemoAgent(AgentBase):
    role = "demo"

    async def _execute_core(self, event: EnrichedEvent) -> ExecutionResult:
        return ExecutionResult(
            status="success",
            output_data={"answer": "ok"},
            artifacts={"provider": "fake-provider", "model": "fake-model", "tokens": 7, "cost_cents": 3},
        )


@pytest.mark.asyncio
async def test_capture_event_contains_required_metadata_and_small_output_inline(tmp_path):
    appender = FakeAppender()
    result = ExecutionResult(
        status="success",
        output_data={"message": "small"},
        artifacts={"provider": "test", "model": "model-a", "tokens": 11, "cost_cents": 5, "fallback_used": False},
    )
    event = EnrichedEvent(
        event_id="evt-1",
        lineage_run_id="run-1",
        goal="write code",
        context={"prompt": "hello", "system_prompt": "system"},
    )

    receipt = await capture_agent_completion(
        event=event,
        result=result,
        role="tester",
        agent_id="agent-1",
        event_appender=appender,
        artifact_store=None,
    )

    assert receipt.event_type == LLM_COMPLETION_CAPTURED
    assert appender.events[0]["event_type"] == LLM_COMPLETION_CAPTURED
    details = appender.events[0]["details"]
    for key in [
        "provider",
        "model",
        "prompt_hash",
        "system_prompt_hash",
        "output_hash",
        "tokens",
        "cost_cents",
        "fallback_used",
    ]:
        assert key in details
    assert details["output"]["mode"] == "inline"
    assert details["output"]["inline_value"] == {"message": "small"}


@pytest.mark.asyncio
async def test_large_output_uses_claim_check_and_keeps_payload_out_of_event(tmp_path):
    class Store:
        def __init__(self):
            self.data = {}

        async def write_artifact(self, *, content: bytes, content_hash: str) -> str:
            ref = f"memory://{content_hash}"
            self.data[ref] = content
            return ref

        async def read_artifact(self, artifact_ref: str) -> bytes:
            return self.data[artifact_ref]

    store = Store()
    large = {"text": "x" * 5000}
    sealed = await seal_execution_artifact(large, artifact_store=store, inline_limit_bytes=64)

    event_payload = sealed.to_event_payload()
    assert event_payload["mode"] == "claim_check"
    assert "artifact_ref" in event_payload
    assert "inline_value" not in event_payload
    assert "x" * 1000 not in str(event_payload)


@pytest.mark.asyncio
async def test_agentbase_strict_capture_failure_fails_agent(monkeypatch):
    monkeypatch.setenv("IRON_V3_LLM_SEALING", "1")
    agent = DemoAgent("agent-1", semantic_engine=None)
    event = EnrichedEvent(
        event_id="evt-2",
        lineage_run_id="run-2",
        goal="demo",
        context={"event_appender": FakeAppender(ok=False)},
    )

    result = await agent.execute(event)

    assert result.status == "failed"
    assert result.requires_hitl is True
    assert any("completion_capture_failed" in item for item in result.reasoning_trace)


@pytest.mark.asyncio
async def test_agentbase_uses_shared_capture_helper_when_enabled(monkeypatch):
    monkeypatch.setenv("IRON_V3_LLM_SEALING", "1")
    appender = FakeAppender(ok=True)
    agent = DemoAgent("agent-2", semantic_engine=None)
    event = EnrichedEvent(
        event_id="evt-3",
        lineage_run_id="run-3",
        goal="demo",
        context={"event_appender": appender},
    )

    result = await agent.execute(event)

    assert result.status == "success"
    assert "completion_capture" in result.artifacts
    assert appender.events[0]["details"]["capture_helper"] == "hfa.events.completion_capture.capture_agent_completion"


def test_all_listed_roles_inherit_shared_agentbase_capture_path():
    for cls in [ArchitectAgent, CoderAgent, ResearcherAgent, TesterAgent]:
        assert issubclass(cls, AgentBase)
