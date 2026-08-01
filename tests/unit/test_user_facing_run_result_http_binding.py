from __future__ import annotations

import json

import httpx
import pytest
from fastapi import FastAPI

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.api.models import RunStatusResultResponse
from hfa_control.api.router import router
from hfa_control.service import ControlPlaneService


RUN_ID = (
    "run-tenant1-"
    "11111111-1111-4111-8111-111111111111"
)
TASK_ID = (
    "task-"
    "22222222-2222-4222-8222-222222222222"
)


class FakeRedis:
    def __init__(self) -> None:
        self.values = {}
        self.hashes = {}
        self.sets = {}
        self.ttls = {}
        self.write_calls = 0

    async def type(self, key):
        if key in self.values:
            return "string"
        if key in self.hashes:
            return "hash"
        if key in self.sets:
            return "set"
        return "none"

    async def get(self, key):
        return self.values.get(key)

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def ttl(self, key):
        return self.ttls.get(key, -2)

    async def set(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("read path attempted SET")

    async def hset(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("read path attempted HSET")

    async def sadd(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("read path attempted SADD")

    async def delete(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("read path attempted DELETE")

    async def xadd(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("read path attempted XADD")


def service_for(redis: FakeRedis) -> ControlPlaneService:
    service = object.__new__(ControlPlaneService)
    service._redis = redis
    return service


def seed_run(
    redis: FakeRedis,
    *,
    state: str = "done",
    include_result: bool = True,
) -> None:
    redis.values[RedisKey.run_state(RUN_ID)] = state
    redis.hashes[RedisKey.run_meta(RUN_ID)] = {
        "run_id": RUN_ID,
        "tenant_id": "tenant1",
        "state": state,
        "result_event_id": "event-1",
    }
    redis.ttls[RedisKey.run_state(RUN_ID)] = 3600
    redis.ttls[RedisKey.run_meta(RUN_ID)] = 3600

    if include_result:
        redis.hashes[RedisKey.run_result(RUN_ID)] = {
            "run_id": RUN_ID,
            "status": state,
            "payload": json.dumps(
                {
                    "task_count": 1,
                    "done_count":
                        1 if state == "done" else 0,
                    "failed_count":
                        1 if state == "failed" else 0,
                    "skipped_count": 0,
                }
            ),
            "result_event_id": "event-1",
            "completed_at": "2.0",
            "error":
                ""
                if state == "done"
                else "aggregate_task_failure",
        }
        redis.ttls[RedisKey.run_result(RUN_ID)] = 3600


def seed_task(
    redis: FakeRedis,
    *,
    state: str = "done",
    output=None,
) -> None:
    redis.sets[DagRedisKey.run_tasks(RUN_ID)] = {
        TASK_ID
    }
    redis.hashes[DagRedisKey.task_meta(TASK_ID)] = {
        "task_id": TASK_ID,
        "run_id": RUN_ID,
        "tenant_id": "tenant1",
    }
    redis.values[DagRedisKey.task_state(TASK_ID)] = (
        state
    )
    if output is not None:
        redis.values[
            DagRedisKey.task_output(TASK_ID)
        ] = json.dumps(output)


def app_for(service: ControlPlaneService) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.state.cp = service
    app.state.redis = service._redis
    return app


@pytest.mark.asyncio
async def test_service_combines_run_and_task_output():
    redis = FakeRedis()
    seed_run(redis)
    seed_task(
        redis,
        output={"output_text": "SPRINT83_6_HTTP_OK"},
    )

    data = await service_for(
        redis
    ).get_run_status_result(RUN_ID)

    assert data["status"] == "COMPLETED"
    assert data["completeness"] == (
        "TERMINAL_WITH_RESULT"
    )
    assert data["task_output_status"] == "AVAILABLE"
    assert data["task_id"] == TASK_ID
    assert data["task_state"] == "done"
    assert data["task_output"] == {
        "output_text": "SPRINT83_6_HTTP_OK"
    }
    assert data["task_output_issues"] == []
    assert redis.write_calls == 0


@pytest.mark.asyncio
async def test_http_get_exposes_durable_task_output():
    redis = FakeRedis()
    seed_run(redis)
    seed_task(
        redis,
        output={
            "output_text": "SPRINT83_6_HTTP_OK",
            "nested": {"b": 2, "a": 1},
        },
    )
    app = app_for(service_for(redis))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://sprint83-6.test",
    ) as client:
        response = await client.get(
            f"/control/v1/runs/{RUN_ID}",
            headers={"X-Tenant-ID": "tenant1"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == RUN_ID
    assert body["status"] == "COMPLETED"
    assert body["task_output_status"] == "AVAILABLE"
    assert body["task_id"] == TASK_ID
    assert body["task_state"] == "done"
    assert body["task_output"] == {
        "output_text": "SPRINT83_6_HTTP_OK",
        "nested": {"b": 2, "a": 1},
    }
    assert body["task_output_issues"] == []
    assert redis.write_calls == 0


@pytest.mark.asyncio
async def test_http_nonterminal_run_is_additive():
    redis = FakeRedis()
    seed_run(
        redis,
        state="running",
        include_result=False,
    )
    app = app_for(service_for(redis))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://sprint83-6.test",
    ) as client:
        response = await client.get(
            f"/control/v1/runs/{RUN_ID}",
            headers={"X-Tenant-ID": "tenant1"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "RUNNING"
    assert body["terminal"] is False
    assert body["task_output_status"] == "NOT_TERMINAL"
    assert body["task_id"] is None
    assert body["task_state"] is None
    assert body["task_output"] is None
    assert body["task_output_issues"] == []


@pytest.mark.asyncio
async def test_http_unknown_run_remains_404():
    redis = FakeRedis()
    app = app_for(service_for(redis))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://sprint83-6.test",
    ) as client:
        response = await client.get(
            f"/control/v1/runs/{RUN_ID}",
            headers={"X-Tenant-ID": "tenant1"},
        )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


class TenantGuardCP:
    def __init__(self) -> None:
        self.calls = 0

    async def get_run_status_result(self, run_id: str):
        self.calls += 1
        raise AssertionError(
            "tenant mismatch must stop before read"
        )


@pytest.mark.asyncio
async def test_http_tenant_mismatch_stops_before_read():
    cp = TenantGuardCP()
    app = FastAPI()
    app.include_router(router)
    app.state.cp = cp
    app.state.redis = object()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://sprint83-6.test",
    ) as client:
        response = await client.get(
            f"/control/v1/runs/{RUN_ID}",
            headers={"X-Tenant-ID": "tenant2"},
        )

    assert response.status_code == 403
    assert cp.calls == 0


def test_response_model_keeps_additive_defaults():
    response = RunStatusResultResponse(
        schema_version=1,
        run_id=RUN_ID,
        status="RUNNING",
        terminal=False,
        outcome=None,
        result=None,
        error=None,
        submitted_at=None,
        started_at=None,
        finished_at=None,
        updated_at=None,
        freshness="CURRENT",
        completeness="RUNNING_WITHOUT_RESULT",
        completeness_reason=None,
        internal_state="running",
        task_counts={},
        state_ttl_seconds=3600,
        meta_ttl_seconds=3600,
        result_ttl_seconds=-2,
        issues=[],
    )
    data = response.model_dump()

    assert data["task_output_status"] == "RUN_UNKNOWN"
    assert data["task_id"] is None
    assert data["task_state"] is None
    assert data["task_output"] is None
    assert data["task_output_issues"] == []
