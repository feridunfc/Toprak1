from pathlib import Path
from types import SimpleNamespace

import pytest

from hfa_control.api import router as router_module
from hfa_control.api.router import (
    TerminalDuplicateCleanupRequest,
    task_terminal_duplicate_cleanup,
)
from hfa_control.terminal_duplicate_cleanup_command import (
    CLEANED,
    TerminalDuplicateCleanupCommandResult,
)


def _result() -> TerminalDuplicateCleanupCommandResult:
    return TerminalDuplicateCleanupCommandResult(
        task_id="task-1",
        run_id="run-1",
        task_state="done",
        terminal=True,
        stream="stream:7",
        group="worker_consumers",
        pending_message_id="1-0",
        message_task_id="task-1",
        message_run_id="run-1",
        task_meta_run_id="run-1",
        evidence_reason="EXPLICIT_TERMINAL_DUPLICATE_CLEANUP_CANDIDATE",
        evidence_status="cleanup_candidate",
        ack_policy="ack_explicit_task_run_terminal_evidence",
        ack_allowed=True,
        cleanup_candidate=True,
        dry_run=False,
        execute_requested=True,
        cleanup_executed=True,
        ack_executed=True,
        ack_count=1,
        status=CLEANED,
        denial_reason="",
        read_only_evidence_used=True,
        mutation_allowed=True,
        mutation_type="xack_terminal_duplicate_cleanup",
        operator_reason="operator_cleanup_terminal_duplicate_pending_message",
        operator_action_required_before=False,
        operator_action_required_after=False,
        production_ready_claim=False,
    )


@pytest.mark.asyncio
async def test_terminal_duplicate_cleanup_endpoint_is_thin_body_based_adapter(monkeypatch):
    calls = {}

    def fake_require_operator(token):
        calls["auth"] = token

    async def fake_execute(redis, **kwargs):
        calls["redis"] = redis
        calls["kwargs"] = kwargs
        return _result()

    monkeypatch.setattr(router_module, "_require_operator", fake_require_operator)
    monkeypatch.setattr(
        router_module,
        "execute_terminal_duplicate_cleanup_command",
        fake_execute,
    )

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(redis="redis")))
    body = TerminalDuplicateCleanupRequest(
        shard=7,
        group="worker_consumers",
        pending_message_id="1-0",
        dry_run=False,
        execute=True,
        reason="operator_cleanup_terminal_duplicate_pending_message",
        pending_limit=55,
    )

    response = await task_terminal_duplicate_cleanup(
        "task-1",
        body,
        request,
        x_cp_auth="operator-token",
    )

    assert calls["auth"] == "operator-token"
    assert calls["redis"] == "redis"
    assert calls["kwargs"] == {
        "task_id": "task-1",
        "stream_key": "hfa:stream:runs:7",
        "consumer_group": "worker_consumers",
        "pending_message_id": "1-0",
        "dry_run": False,
        "execute": True,
        "reason": "operator_cleanup_terminal_duplicate_pending_message",
        "pending_limit": 55,
    }

    assert response["status"] == CLEANED
    assert response["cleanup_executed"] is True
    assert response["ack_executed"] is True
    assert response["ack_count"] == 1
    assert response["production_ready_claim"] is False


def test_terminal_duplicate_cleanup_request_defaults_are_safe():
    body = TerminalDuplicateCleanupRequest()

    assert body.shard == 0
    assert body.group == "worker_consumers"
    assert body.pending_message_id == ""
    assert body.dry_run is True
    assert body.execute is False
    assert body.reason == ""
    assert body.pending_limit == 100


def test_terminal_duplicate_cleanup_endpoint_block_contains_no_direct_mutation():
    source = Path("hfa-control/src/hfa_control/api/router.py").read_text(
        encoding="utf-8"
    )
    marker = "async def task_terminal_duplicate_cleanup("
    assert marker in source
    block = source[source.index(marker):]

    forbidden_tokens = [
        ".xack(",
        ".xclaim(",
        ".xadd(",
        ".hset(",
        ".set(",
        ".delete(",
        ".expire(",
        "xpending_range(",
        "xrange(",
        "task_complete(",
        "claim_start(",
        "requeue(",
        "repair(",
    ]

    for token in forbidden_tokens:
        assert token not in block
