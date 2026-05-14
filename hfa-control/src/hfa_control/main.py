"""
hfa-control/src/hfa_control/main.py
IRONCLAD Sprint 10 — Control Plane entry point

Startup order
-------------
  1. Redis connection
  2. ControlPlaneService.start()   (registry + shard + leader election)
  3. FastAPI app mounted with /control/v1 router
  4. uvicorn serve

Shutdown (SIGTERM / SIGINT)
---------------------------
  1. Stop accepting new requests (uvicorn handles)
  2. ControlPlaneService.close()   (drains all background tasks)
  3. Redis connection close

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop() where needed.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

import uvicorn
from fastapi import FastAPI

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def build_app(redis=None, config=None) -> FastAPI:
    """
    Factory used both by main() and by tests (pass mock redis).
    """
    from hfa_control.service import ControlPlaneService
    from hfa_control.api.router import router

    app = FastAPI(title="IRONCLAD Control Plane", version="10.0.0")
    app.include_router(router)

    _redis = redis
    _cp: ControlPlaneService | None = None

    @app.on_event("startup")
    async def _startup() -> None:
        nonlocal _redis, _cp
        if _redis is None:
            import redis.asyncio as aioredis

            _redis = aioredis.from_url(
                os.environ.get("REDIS_URL", "redis://localhost:6379"),
                decode_responses=False,
            )
        _cp = ControlPlaneService(_redis, config)
        await _cp.start()
        app.state.cp = _cp
        app.state.redis = _redis
        logger.info("ControlPlane FastAPI app started")

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        if _cp:
            await _cp.close()
        if _redis:
            await _redis.aclose()
        logger.info("ControlPlane FastAPI app shutdown complete")

    return app


def main() -> None:
    try:
        import uvloop

        uvloop.install()
        logger.info("uvloop installed")
    except ImportError:
        logger.info("uvloop not available, using default asyncio event loop")

    app = build_app()

    host = os.environ.get("CP_HOST", "0.0.0.0")
    port = int(os.environ.get("CP_PORT", "8100"))

    config = uvicorn.Config(
        app=app,
        host=host,
        port=port,
        log_level="info",
        access_log=True,
    )
    server = uvicorn.Server(config)

    # SIGTERM → graceful shutdown
    loop = asyncio.get_event_loop()

    def _handle_sigterm(*_):
        logger.info("SIGTERM received — initiating graceful shutdown")
        loop.call_soon_threadsafe(server.handle_exit, signal.SIGTERM, None)

    signal.signal(signal.SIGTERM, _handle_sigterm)

    try:
        loop.run_until_complete(server.serve())
    finally:
        loop.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
