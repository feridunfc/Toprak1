import pytest
import asyncio
from scripts.e2e_tenant_submit_worker_stream import run_demo

@pytest.mark.asyncio
async def test_e2e_tenant_submit_worker_stream_passes_through_loop():
    result = await run_demo()
    assert result["status"] == "PASS"
    assert result["run_requested_event_consumed"] is True
    assert result["worker_consumer_process_message_used"] is True
