"""Bounded transport effect of an already accepted RUN_TERMINATE decision."""
from __future__ import annotations

import json
from pathlib import Path

import hfa.lua
from hfa.lua.loader import LuaScriptLoader


async def ack_message(redis, stream, group, msg_id, *, ctx, run_termination):
    """Revalidate own RUN proof and ACK atomically; never authorize TASK state."""
    root = Path(hfa.lua.__file__).parent
    loader = LuaScriptLoader(
        redis, root / "worker_recovery_ack.lua",
        helpers={"validate_authority_head": root / "authority_head_validate.lua"},
    )
    await loader.load()
    keys = tuple(getattr(run_termination, "canonical_head_keys", ()))
    own = tuple(getattr(run_termination, "canonical_head_witness", ()))
    result = await loader.run(
        num_keys=1 + len(keys), keys=[stream, *keys],
        args=[
            json.dumps(own, separators=(",", ":")),
            ctx.run_id, ctx.tenant_id, ctx.task_id, group, msg_id,
            getattr(run_termination, "final_state", ""),
            getattr(run_termination, "canonical_transition_id", ""),
            getattr(run_termination, "canonical_record_hash", ""),
            getattr(run_termination, "canonical_command_hash", ""),
            str(getattr(run_termination, "canonical_revision", 0)),
            getattr(run_termination, "terminal_proof_sha256", ""),
        ],
    )
    if not isinstance(result, (list, tuple)) or len(result) != 2:
        return "recovery_ack_invalid_result"
    status = result[0]
    return status.decode("utf-8") if isinstance(status, bytes) else str(status)
