from __future__ import annotations

from types import SimpleNamespace

import pytest

from hfa.config.keys import RedisKey
from hfa_control.shard import ShardOwnershipManager


class LeaseProjectionRedis:
    def __init__(self) -> None:
        self.owner = "group-shard-repair-79-12"
        self.owner_map: dict[int, str] = {}
        self.eval_args = None

    async def eval(self, script, number_of_keys, *args):
        self.eval_args = (script, number_of_keys, args)

        if number_of_keys == 1:
            owner_key, requested_group, ttl = args
            return 1 if requested_group == self.owner else 0

        if number_of_keys == 2:
            owner_key, owners_key, requested_group, ttl, shard = args
            if requested_group != self.owner:
                return 0
            self.owner_map[int(shard)] = requested_group
            return 1

        raise AssertionError(
            f"Unexpected shard-renew key count: {number_of_keys}"
        )


@pytest.mark.asyncio
async def test_successful_renew_atomically_repairs_owner_projection() -> None:
    redis = LeaseProjectionRedis()
    manager = ShardOwnershipManager(redis, SimpleNamespace())

    renewed = await manager.renew_shard(
        7,
        "group-shard-repair-79-12",
    )

    assert renewed is True
    assert redis.owner_map == {
        7: "group-shard-repair-79-12"
    }, (
        "A valid authoritative lease can coexist with a missing/stale "
        "hfa:cp:shard:owners entry after a partial claim write or restart. "
        "Renewal must atomically validate the owner, extend TTL, and repair "
        "the scheduler projection."
    )

    assert redis.eval_args is not None
    _script, number_of_keys, args = redis.eval_args
    assert number_of_keys == 2, (
        "Owner validation and owners-hash repair must share one Lua atomic "
        "boundary; a separate HSET recreates the partial-write window."
    )
    assert args[1] == RedisKey.cp_shard_owners()
