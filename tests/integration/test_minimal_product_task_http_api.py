from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from scripts.ironclad_api import create_app, self_test


REDIS_URL = "redis://localhost:6389/0"


@pytest.mark.asyncio
async def test_minimal_product_task_http_api_health() -> None:
    app = create_app(REDIS_URL)
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["service"] == "ironclad_minimal_product_task_api"
    assert data["runtime"] == "sprint_43"


@pytest.mark.asyncio
async def test_minimal_product_task_http_api_submit_and_result() -> None:
    app = create_app(REDIS_URL)
    transport = ASGITransport(app=app)
    message = "hello from minimal HTTP API test"

    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        task_response = await client.post(
            "/tasks",
            json={"tenant_id": "demo", "message": message},
        )

        assert task_response.status_code == 200
        submitted = task_response.json()

        assert submitted["status"] == "SUBMITTED"
        assert submitted["tenant_id"] == "demo"
        assert submitted["task_id"]
        assert submitted["run_id"]
        assert submitted["message"] == message
        assert submitted["uses_sprint_43_runtime"] is True
        assert submitted["production_lua_evalsha_path_used"] is True
        assert submitted["worker_stream_consume_loop_used"] is True
        assert submitted["direct_process_message_call_used"] is False
        assert submitted["fake_executor_used"] is True

        result_response = await client.get(f"/runs/{submitted['run_id']}")

    assert result_response.status_code == 200
    result = result_response.json()

    assert result["status"] == "COMPLETED"
    assert result["tenant_id"] == "demo"
    assert result["task_id"] == submitted["task_id"]
    assert result["run_id"] == submitted["run_id"]
    assert result["result_readable"] is True
    assert message in result["result"]["output_text"]
    assert result["uses_sprint_43_runtime"] is True
    assert result["production_lua_evalsha_path_used"] is True
    assert result["worker_stream_consume_loop_used"] is True
    assert result["direct_process_message_call_used"] is False
    assert result["fake_executor_used"] is True
    assert result["production_llm_call_attempted"] is False
    assert result["deployment_attempted"] is False
    assert result["release_tag_created"] is False
    assert result["operator_action_buttons"] is False


@pytest.mark.asyncio
async def test_minimal_product_task_http_api_unknown_run_returns_404() -> None:
    app = create_app(REDIS_URL)
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/runs/run-does-not-exist")

    assert response.status_code == 404
    detail = response.json()["detail"]
    assert detail["status"] == "NOT_FOUND_OR_NOT_PERSISTED"
    assert detail["run_id"] == "run-does-not-exist"


@pytest.mark.asyncio
async def test_minimal_product_task_http_api_self_test_artifact_shape() -> None:
    artifact = await self_test(
        redis_url=REDIS_URL,
        tenant_id="demo",
        message="hello from API self test",
    )

    assert artifact["status"] == "PASS"
    assert artifact["target_claim_supported"] is True
    assert artifact["http_api_available"] is True
    assert artifact["health_endpoint_available"] is True
    assert artifact["post_tasks_available"] is True
    assert artifact["get_run_result_available"] is True
    assert artifact["uses_sprint_43_runtime"] is True
    assert artifact["result_readable"] is True
    assert artifact["result_output_includes_submitted_message"] is True
    assert artifact["production_lua_evalsha_path_used"] is True
    assert artifact["worker_stream_consume_loop_used"] is True
    assert artifact["direct_process_message_call_used"] is False
    assert artifact["fake_executor_used"] is True
    assert artifact["production_llm_call_attempted"] is False
    assert artifact["deployment_attempted"] is False
    assert artifact["release_tag_created"] is False
    assert artifact["operator_action_buttons"] is False
    assert artifact["failing_reasons"] == []
