from pathlib import Path
from types import SimpleNamespace

import pytest

import hfa_control.api.router as router_module
from hfa_control.terminal_duplicate_operator_evidence import (
    EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE,
)


@pytest.mark.asyncio
async def test_terminal_duplicate_operator_evidence_endpoint_is_thin_read_only_adapter(monkeypatch):
    calls = []

    def fake_require_operator(auth: str) -> None:
        calls.append(("auth", auth))

    monkeypatch.setattr(router_module, "_require_operator", fake_require_operator)

    class Redis:
        def __init__(self):
            self.calls = []

        async def get(self, key):
            self.calls.append(("get", key))
            return "done"

        async def hgetall(self, key):
            self.calls.append(("hgetall", key))
            return {"run_id": "run-endpoint-1"}

        async def xpending_range(self, stream, group, start, end, limit):
            self.calls.append(("xpending_range", stream, group, start, end, limit))
            return [
                {
                    "message_id": "1700000000-0",
                    "consumer": "worker-1",
                    "idle": 12,
                    "deliveries": 2,
                }
            ]

        async def xrange(self, stream, start, end):
            self.calls.append(("xrange", stream, start, end))
            return [
                (
                    "1700000000-0",
                    {
                        "task_id": "task-endpoint-1",
                        "run_id": "run-endpoint-1",
                    },
                )
            ]

    redis = Redis()
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(redis=redis)))

    result = await router_module.task_terminal_duplicate_operator_evidence(
        "task-endpoint-1",
        request,
        shard=0,
        group="worker_consumers",
        pending_limit=100,
        x_cp_auth="operator-token",
    )

    assert calls == [("auth", "operator-token")]
    assert result["task_id"] == "task-endpoint-1"
    assert result["run_id"] == "run-endpoint-1"
    assert result["ack_allowed"] is True
    assert result["cleanup_candidate"] is True
    assert result["cleanup_done"] is False
    assert result["cleanup_executed"] is False
    assert result["operator_action_required"] is False
    assert result["reason"] == EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE
    assert result["read_only"] is True
    assert result["mutation_allowed"] is False
    assert result["production_ready_claim"] is False

    called = [call[0] for call in redis.calls]
    assert called == ["get", "hgetall", "xpending_range", "xrange"]


def test_terminal_duplicate_operator_evidence_endpoint_contains_no_mutation_calls():
    source = Path("hfa-control/src/hfa_control/api/router.py").read_text(
        encoding="utf-8"
    ).lower()
    endpoint_start = source.index("terminal-duplicate-operator-evidence")
    endpoint_source = source[endpoint_start:]

    forbidden_tokens = [
        ".xack(",
        ".xclaim(",
        ".xadd(",
        ".hset(",
        ".set(",
        ".delete(",
        ".expire(",
        "requeue(",
        "repair(",
    ]

    for token in forbidden_tokens:
        assert token not in endpoint_source
