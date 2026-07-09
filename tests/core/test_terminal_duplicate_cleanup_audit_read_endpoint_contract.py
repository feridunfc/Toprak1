import inspect

import pytest

from hfa_control.api import router as router_module
from hfa_control.terminal_duplicate_cleanup_audit import (
    TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
)
from hfa_control.terminal_duplicate_cleanup_audit_read_model import (
    AUDIT_READ_EMPTY,
    TerminalDuplicateCleanupAuditReadResult,
)


pytestmark = pytest.mark.asyncio


class State:
    def __init__(self, redis):
        self.redis = redis


class App:
    def __init__(self, redis):
        self.state = State(redis)


class Request:
    def __init__(self, redis):
        self.app = App(redis)


async def test_cleanup_audit_endpoint_calls_read_model_with_safe_defaults(monkeypatch):
    redis = object()
    calls = []

    async def fake_read_model(
        received_redis,
        *,
        task_id,
        limit,
        scan_limit,
        include_entries,
    ):
        calls.append(
            {
                "redis": received_redis,
                "task_id": task_id,
                "limit": limit,
                "scan_limit": scan_limit,
                "include_entries": include_entries,
            }
        )
        return TerminalDuplicateCleanupAuditReadResult(
            task_id=task_id,
            audit_stream=TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
            read_status=AUDIT_READ_EMPTY,
            operator_summary="No terminal duplicate cleanup audit entries found.",
            production_ready_claim=False,
        )

    monkeypatch.setattr(router_module, "_require_operator", lambda x_cp_auth="": None)
    monkeypatch.setattr(
        router_module,
        "read_terminal_duplicate_cleanup_audit",
        fake_read_model,
    )

    response = await router_module.task_terminal_duplicate_cleanup_audit(
        "task-1",
        Request(redis),
        x_cp_auth="operator-secret",
    )

    assert calls == [
        {
            "redis": redis,
            "task_id": "task-1",
            "limit": 100,
            "scan_limit": 500,
            "include_entries": True,
        }
    ]
    assert response["task_id"] == "task-1"
    assert response["audit_stream"] == TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM
    assert response["read_status"] == AUDIT_READ_EMPTY
    assert response["production_ready_claim"] is False


async def test_cleanup_audit_endpoint_passes_operator_query_parameters(monkeypatch):
    redis = object()
    calls = []

    async def fake_read_model(
        received_redis,
        *,
        task_id,
        limit,
        scan_limit,
        include_entries,
    ):
        calls.append((received_redis, task_id, limit, scan_limit, include_entries))
        return TerminalDuplicateCleanupAuditReadResult(
            task_id=task_id,
            audit_stream=TERMINAL_DUPLICATE_CLEANUP_AUDIT_STREAM,
            limit=limit,
            scan_limit=scan_limit,
            entries=(),
            production_ready_claim=False,
        )

    monkeypatch.setattr(router_module, "_require_operator", lambda x_cp_auth="": None)
    monkeypatch.setattr(
        router_module,
        "read_terminal_duplicate_cleanup_audit",
        fake_read_model,
    )

    await router_module.task_terminal_duplicate_cleanup_audit(
        "task-2",
        Request(redis),
        limit=17,
        scan_limit=250,
        include_entries=False,
        x_cp_auth="operator-secret",
    )

    assert calls == [(redis, "task-2", 17, 250, False)]


def test_cleanup_audit_endpoint_route_is_registered_as_task_centered_get():
    source = inspect.getsource(router_module)

    assert '@router.get("/tasks/{task_id}/terminal-duplicate-cleanup-audit")' in source
    assert "async def task_terminal_duplicate_cleanup_audit(" in source


def test_cleanup_audit_endpoint_is_thin_adapter_without_direct_redis_behavior():
    source = inspect.getsource(router_module.task_terminal_duplicate_cleanup_audit)

    assert "read_terminal_duplicate_cleanup_audit" in source
    assert "request.app.state.redis" in source
    assert "asdict(result)" in source

    assert ".xrevrange(" not in source
    assert ".xrange(" not in source
    assert ".xadd(" not in source
    assert ".xack(" not in source
    assert ".xclaim(" not in source
    assert ".xpending" not in source
    assert "RedisKey.stream_shard" not in source
    assert "execute_terminal_duplicate_cleanup_command" not in source
    assert "read_terminal_duplicate_operator_evidence" not in source

    assert "dashboard_action=True" not in source
    assert "dashboard_action_emitted=True" not in source
    assert '"dashboard_action_emitted": True' not in source
    assert "'dashboard_action_emitted': True" not in source
    assert "production_ready_claim=True" not in source
