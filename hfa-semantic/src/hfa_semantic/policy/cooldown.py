from __future__ import annotations
def cooldown_elapsed(now_ms: int, last_change_ms: int | None, cooldown_ms: int) -> bool:
    if last_change_ms is None:
        return True
    return (now_ms - last_change_ms) >= cooldown_ms
