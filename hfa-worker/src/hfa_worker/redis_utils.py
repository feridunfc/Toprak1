"""
hfa-control/src/hfa_control/redis_utils.py
Minimal Redis stream helpers for control-plane runtime.
"""

from __future__ import annotations


async def ensure_consumer_group(redis, stream, group, start_id="0", mkstream=True):
    try:
        await redis.xgroup_create(
            name=stream,
            groupname=group,
            id=start_id,
            mkstream=mkstream,
        )
    except Exception:
        pass


async def ack_message(redis, stream, group, msg_id):
    return await redis.xack(stream, group, msg_id)


async def xreadgroup_safe(
    redis,
    *,
    group: str,
    consumer: str,
    streams: dict[str, str],
    count: int = 10,
    block: int = 1000,
    noack: bool = False,
):
    """
    Thin wrapper around XREADGROUP for scheduler/control-plane callers.
    """
    return await redis.xreadgroup(
        groupname=group,
        consumername=consumer,
        streams=streams,
        count=count,
        block=block,
        noack=noack,
    )
