"""
hfa-core/src/hfa/lua/loader.py
IRONCLAD Sprint 15 — Lua script loader with EVALSHA + NOSCRIPT recovery

Design
------
Production Redis deployments should call Lua scripts via EVALSHA, not EVAL:

  EVAL    — sends the full script body on every call (wasteful over network)
  EVALSHA — sends only the 40-char SHA1 hash; Redis executes the cached script

Workflow
--------
1. loader.load()        — SCRIPT LOAD → caches SHA on the Redis server
2. loader.run(...)      — EVALSHA with cached SHA
3. On NOSCRIPT error    — SCRIPT LOAD + retry automatically (Redis restart safe)

NOSCRIPT recovery
-----------------
Redis flushes its script cache on SCRIPT FLUSH or after restart (if
script persistence is not configured). EVALSHA then returns NOSCRIPT.
LuaScriptLoader catches this, reloads the script, and retries once.

Fakeredis compatibility
-----------------------
fakeredis does not support EVALSHA / SCRIPT LOAD. The loader detects this
and falls back to a provided callable. Callers pass `fallback=` for unit
test compatibility.

Usage
-----
    loader = LuaScriptLoader(redis, Path("hfa/lua/rate_limit.lua"))
    await loader.load()

    # In hot path:
    result = await loader.run(
        num_keys=1,
        keys=[rate_key],
        args=[limit, now, member, ttl],
        fallback=non_atomic_fn,   # fakeredis path
    )
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Awaitable, Callable, List, Optional

logger = logging.getLogger(__name__)


class LuaScriptLoader:
    """
    Loads a Lua script into Redis via SCRIPT LOAD and executes it via EVALSHA.

    Handles NOSCRIPT recovery and fakeredis fallback transparently.

    Args:
        redis:       aioredis.Redis client (real or fakeredis).
        script_path: Path to the .lua file to load.
    """

    def __init__(self, redis, script_path: Path) -> None:
        self._redis = redis
        self._path = script_path
        self._sha: Optional[str] = None
        self._source: Optional[str] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def load(self) -> None:
        """
        Read the Lua file and load it into Redis via SCRIPT LOAD.
        Caches the returned SHA for subsequent EVALSHA calls.

        Raises:
            FileNotFoundError: If the .lua file does not exist.
            RuntimeError:      If SCRIPT LOAD fails on real Redis.
        """
        source = self._path.read_text(encoding="utf-8")
        self._source = source

        try:
            sha = await self._redis.script_load(source)
            self._sha = sha.decode() if isinstance(sha, bytes) else sha
            logger.info(
                "LuaScriptLoader.load: %s → SHA %s…",
                self._path.name,
                self._sha[:8] if self._sha else "?",
            )
        except Exception as exc:
            if _is_fakeredis_error(exc):
                logger.debug(
                    "LuaScriptLoader: SCRIPT LOAD not supported (fakeredis), "
                    "will fall back to eval() on run()"
                )
            else:
                raise RuntimeError(
                    f"LuaScriptLoader.load failed for {self._path.name}: {exc}"
                ) from exc

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def run(
        self,
        *,
        num_keys: int,
        keys: List[Any],
        args: List[Any],
        fallback: Optional[Callable[[], Awaitable[Any]]] = None,
    ) -> Any:
        """
        Execute the loaded script via EVALSHA.

        On NOSCRIPT (Redis restarted and flushed its script cache):
          - Reload the script automatically.
          - Retry EVALSHA once.

        On fakeredis (EVALSHA not supported):
          - If fallback is provided, call it instead.
          - Otherwise fall through to raw eval().

        Args:
            num_keys: Number of KEYS[] arguments.
            keys:     KEYS list.
            args:     ARGV list.
            fallback: Async callable for fakeredis unit tests (no args).

        Returns:
            Whatever the Lua script returns (int, str, list, …).

        Raises:
            RuntimeError: If the script was never loaded and no fallback given.
        """
        # Fakeredis path — EVALSHA not supported
        if self._sha is None:
            if fallback is not None:
                logger.debug("LuaScriptLoader.run: no SHA, using fallback")
                return await fallback()
            if self._source:
                logger.debug("LuaScriptLoader.run: no SHA, falling back to EVAL")
                return await self._redis.eval(self._source, num_keys, *keys, *args)
            raise RuntimeError(
                f"LuaScriptLoader not loaded — call await loader.load() first "
                f"({self._path.name})"
            )

        try:
            return await self._redis.evalsha(self._sha, num_keys, *keys, *args)

        except Exception as exc:
            if _is_noscript_error(exc):
                logger.warning(
                    "LuaScriptLoader: NOSCRIPT for %s — reloading and retrying",
                    self._path.name,
                )
                await self._reload()
                # One retry after reload
                return await self._redis.evalsha(self._sha, num_keys, *keys, *args)

            if _is_fakeredis_error(exc):
                if fallback is not None:
                    return await fallback()
                logger.debug(
                    "LuaScriptLoader: EVALSHA not supported (fakeredis), "
                    "falling back to EVAL"
                )
                return await self._redis.eval(self._source, num_keys, *keys, *args)

            raise

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _reload(self) -> None:
        """Reload the script source and update the cached SHA."""
        if self._source is None:
            self._source = self._path.read_text(encoding="utf-8")
        sha = await self._redis.script_load(self._source)
        self._sha = sha.decode() if isinstance(sha, bytes) else sha
        logger.info(
            "LuaScriptLoader: reloaded %s → SHA %s…",
            self._path.name,
            self._sha[:8] if self._sha else "?",
        )

    @property
    def sha(self) -> Optional[str]:
        """Return the cached SHA (None if not yet loaded)."""
        return self._sha

    @property
    def is_loaded(self) -> bool:
        """Return True if the script has been loaded into Redis."""
        return self._sha is not None


# ---------------------------------------------------------------------------
# Error classification helpers
# ---------------------------------------------------------------------------


def _is_noscript_error(exc: Exception) -> bool:
    """Return True if the exception indicates a Redis NOSCRIPT error."""
    msg = str(exc).lower()
    return "noscript" in msg or "no matching script" in msg


def _is_fakeredis_error(exc: Exception) -> bool:
    """Return True if the exception is a fakeredis eval-not-supported error."""
    msg = str(exc).lower()
    return (
        "unknown command" in msg
        or ("eval" in msg and "not supported" in msg)
        or "script_load" in msg
        or "evalsha" in msg
        and "not" in msg
    )
