from __future__ import annotations

import logging

from redis.exceptions import ResponseError

from hfa.config.keys import RedisKey, RedisTTL

logger = logging.getLogger(__name__)

DECREMENT_SCRIPT = """
local current = redis.call('get', KEYS[1])
if not current then
    return 0
end
local new = math.max(0, tonumber(current) - 1)
redis.call('set', KEYS[1], new)
redis.call('expire', KEYS[1], ARGV[1])
return new
"""


async def _decrement_with_fallback(redis, inflight_key: str) -> int:
    try:
        # Real Redis path (atomic)
        return int(
            await redis.eval(
                DECREMENT_SCRIPT, 1, inflight_key, RedisTTL.TENANT_INFLIGHT
            )
            or 0
        )

    except ResponseError as exc:
        # fakeredis fallback
        if "unknown command 'eval'" not in str(exc).lower():
            raise

        raw = await redis.get(inflight_key)

        if raw is None:
            await redis.set(inflight_key, 0, ex=RedisTTL.TENANT_INFLIGHT)
            return 0

        if isinstance(raw, bytes):
            raw = raw.decode()

        try:
            current = int(raw)
        except Exception:
            current = 0

        new_value = max(0, current - 1)
        await redis.set(inflight_key, new_value, ex=RedisTTL.TENANT_INFLIGHT)
        return new_value


async def decrement_tenant_inflight_if_needed(redis, run_id: str) -> None:
    try:
        meta_key = RedisKey.run_meta(run_id)

        tenant_id_raw = await redis.hget(meta_key, "tenant_id")
        if tenant_id_raw is None:
            tenant_id_raw = await redis.hget(meta_key, b"tenant_id")

        if not tenant_id_raw:
            return

        tenant_id = (
            tenant_id_raw.decode()
            if isinstance(tenant_id_raw, bytes)
            else str(tenant_id_raw)
        )

        inflight_key = RedisKey.tenant_inflight(tenant_id)
        new_value = await _decrement_with_fallback(redis, inflight_key)

        logger.debug(
            "Tenant inflight decremented: tenant=%s run=%s new=%s",
            tenant_id,
            run_id,
            new_value,
        )

    except Exception as exc:
        logger.error(
            "Failed to decrement tenant inflight: run=%s error=%s",
            run_id,
            exc,
            exc_info=True,
        )
