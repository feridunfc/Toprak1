"""Fresh terminal-history preparation for explicitly owned acceptance domains."""
from hfa.config.keys import RedisKey
from hfa_control.run_terminal_event_evidence import (
    ensure_terminal_event_index, backfill_terminal_event_evidence, validate_readiness,
)


async def prepare_terminal_history(redis):
    """Prepare only empty history, or validate the already prepared history."""
    if await redis.exists(RedisKey.stream_results()):
        validate_readiness(await redis.hgetall(RedisKey.run_terminal_event_index()))
        return
    await ensure_terminal_event_index(redis)
    await redis.xgroup_create(RedisKey.stream_results(), "acceptance-bootstrap", id="0", mkstream=True)
    result = await backfill_terminal_event_evidence(redis, completed_at_ms=50)
    assert result.status == "ready" and result.scanned_entries == 0

