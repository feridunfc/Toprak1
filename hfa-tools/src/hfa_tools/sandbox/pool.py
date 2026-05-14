"""
hfa-tools/src/hfa_tools/sandbox/pool.py
IRONCLAD Sprint 3 — Pre-warmed container pool with tmpfs + resource limits.

Security contract (per IRONCLAD manifest):
  * read_only=True  (root FS) + tmpfs  ← both required, never one alone
  * network_disabled=True
  * user="nobody"
  * CPU + memory limits enforced by Docker cgroups

Guardian fixes applied:
  * _is_container_healthy: container.reload() before status read
  * get_container: re-validates health on checkout; dead containers removed + pool refilled
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator, Optional

import docker
from docker.errors import NotFound


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Container spec
# ---------------------------------------------------------------------------


@dataclass
class ContainerSpec:
    """Container specification for a language profile."""

    image: str
    command: str = "sleep infinity"
    tmpfs: dict = field(
        default_factory=lambda: {
            "/tmp": "rw,noexec,nosuid,size=64m",
            "/run": "rw,noexec,nosuid,size=8m",
            "/workspace": "rw,noexec,nosuid,size=128m",
        }
    )
    mem_limit: str = "256m"
    cpu_quota: int = 50_000  # 50 % of one core
    network_disabled: bool = True
    user: str = "nobody"
    working_dir: str = "/workspace"


# ---------------------------------------------------------------------------
# SandboxPool
# ---------------------------------------------------------------------------


class SandboxPool:
    """
    Pre-warmed Docker container pool.

    Each language has a fixed-size asyncio.Queue of warm container IDs.
    Containers are created with:
      - read_only=True root FS                 ← IRONCLAD: both required
      - tmpfs on /tmp, /run, /workspace        ← IRONCLAD: both required
      - network_disabled=True
      - CPU + memory limits
    """

    PROFILES: dict[str, ContainerSpec] = {
        "python": ContainerSpec(image="python:3.11-slim"),
        "node": ContainerSpec(image="node:18-slim"),
        "python-test": ContainerSpec(
            image="python:3.11-slim",
            mem_limit="512m",
        ),
    }

    def __init__(
        self,
        pool_size: int = 4,
        languages: list[str] | None = None,
        container_max_age: int = 3600,
        warmup_interval: int = 30,
        cleanup_interval: int = 300,
    ) -> None:
        self._languages = languages or ["python", "node"]
        self._pool_size = pool_size
        self._container_max_age = container_max_age
        self._warmup_interval = warmup_interval
        self._cleanup_interval = cleanup_interval

        loop = asyncio.get_running_loop() if asyncio._get_running_loop() else None
        self._docker = docker.from_env()

        self._pool: dict[str, asyncio.Queue] = {
            lang: asyncio.Queue(maxsize=pool_size) for lang in self._languages
        }
        self._active: set[str] = set()
        self._active_lock = asyncio.Lock()
        self._metadata: dict[str, dict] = {}
        self._metadata_lock = asyncio.Lock()

        self._running = True
        self._warmup_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None

        logger.info(
            "SandboxPool init: languages=%s pool_size=%d",
            self._languages,
            pool_size,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start background warm-up and cleanup loops."""
        self._warmup_task = asyncio.get_running_loop().create_task(self._warmup_loop())
        self._cleanup_task = asyncio.get_running_loop().create_task(self._cleanup_loop())
        logger.info("SandboxPool started")

    async def close(self) -> None:
        """Cancel loops and forcefully remove every managed container."""
        logger.info("SandboxPool shutting down…")
        self._running = False

        for task in (self._warmup_task, self._cleanup_task):
            if task:
                task.cancel()

        await asyncio.gather(
            self._warmup_task or asyncio.sleep(0),
            self._cleanup_task or asyncio.sleep(0),
            return_exceptions=True,
        )

        # Drain pool queues
        all_ids: set[str] = set()
        for q in self._pool.values():
            while not q.empty():
                try:
                    all_ids.add(q.get_nowait())
                except asyncio.QueueEmpty:
                    break

        async with self._active_lock:
            all_ids.update(self._active)

        for cid in all_ids:
            await self._remove_container(cid)

        logger.info("SandboxPool shut down")

    # ------------------------------------------------------------------
    # Public context manager
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def get_container(self, language: str) -> AsyncIterator[str]:
        """
        Checkout a healthy container; return or discard on exit.
        """
        if language not in self.PROFILES:
            raise ValueError(f"Unsupported language: {language!r}")
        if language not in self._pool:
            raise ValueError(
                f"Language {language!r} not in pool (available: {list(self._pool.keys())})"
            )

        container_id: Optional[str] = None

        try:
            # ── checkout loop: skip dead IDs until we get a live one ──
            while True:
                try:
                    candidate = await asyncio.wait_for(self._pool[language].get(), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning("Pool empty for %s — slow-path create", language)
                    candidate = await self._create_container(language)

                if await self._is_container_healthy(candidate):
                    container_id = candidate
                    break
                else:
                    logger.warning("Stale container %s removed at checkout", candidate[:12])
                    await self._remove_container(candidate)
                    asyncio.get_running_loop().create_task(self._refill_pool(language))

            # Fail-closed: wipe workspace to prevent cross-run/tenant data leaks
            wiped = await self._wipe_workspace(container_id)
            if not wiped:
                logger.warning(
                    "Workspace wipe failed for %s — removing container", container_id[:12]
                )
                await self._remove_container(container_id)
                # ✅ Guardian Fix: DO NOT await self._pool.put() here, it causes DEADLOCK if queue is full
                container_id = await self._create_container(language)
                wiped = await self._wipe_workspace(container_id)
                if not wiped:
                    await self._remove_container(container_id)
                    raise RuntimeError("Sandbox workspace wipe failed")

            async with self._active_lock:
                self._active.add(container_id)

            yield container_id

        finally:
            if container_id:
                async with self._active_lock:
                    self._active.discard(container_id)

                try:
                    if await self._is_container_healthy(container_id):
                        try:
                            self._pool[language].put_nowait(container_id)
                        except asyncio.QueueFull:
                            await self._remove_container(container_id)
                    else:
                        await self._remove_container(container_id)
                        asyncio.get_running_loop().create_task(self._refill_pool(language))
                except Exception as exc:
                    logger.error("Error returning container %s: %s", container_id[:12], exc)
                    await self._remove_container(container_id)

    # ------------------------------------------------------------------
    # Container management
    # ------------------------------------------------------------------

    async def _create_container(self, language: str) -> str:
        """Create a new sandboxed container. Blocks thread pool briefly."""
        spec = self.PROFILES[language]
        name = f"hfa-{language}-{uuid.uuid4().hex[:8]}"
        loop = asyncio.get_running_loop()

        try:
            container = await loop.run_in_executor(
                None,
                lambda: self._docker.containers.run(
                    image=spec.image,
                    command=spec.command,
                    name=name,
                    network_disabled=spec.network_disabled,
                    read_only=True,  # ✅ IRONCLAD: root FS read-only
                    cap_drop=["ALL"],
                    security_opt=["no-new-privileges"],
                    pids_limit=128,
                    ulimits=[
                        docker.types.Ulimit(name="nofile", soft=1024, hard=2048),
                        docker.types.Ulimit(name="nproc", soft=256, hard=512),
                    ],
                    tmpfs=spec.tmpfs,  # ✅ IRONCLAD: writable tmpfs required with read_only
                    user=spec.user,
                    working_dir=spec.working_dir,
                    mem_limit=spec.mem_limit,
                    cpu_period=100_000,
                    cpu_quota=spec.cpu_quota,
                    detach=True,
                    remove=False,
                    labels={
                        "hfa.managed": "true",
                        "hfa.language": language,
                        "hfa.created": str(time.time()),
                        "hfa.version": "5.3",
                    },
                ),
            )
            cid = container.id
            async with self._metadata_lock:
                self._metadata[cid] = {
                    "language": language,
                    "created_at": time.time(),
                }
            logger.info("Created %s container: %s", language, cid[:12])
            return cid

        except Exception as exc:
            logger.error("Container creation failed: %s", exc, exc_info=True)
            raise RuntimeError(f"Container creation failed: {exc}") from exc

    async def _remove_container(self, container_id: str) -> None:
        """Force-remove a container. Ignores NotFound."""
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self._docker.containers.get(container_id).remove(force=True, v=True),
            )
            logger.debug("Removed container %s", container_id[:12])
        except NotFound:
            pass
        except Exception as exc:
            logger.error("Remove container %s error: %s", container_id[:12], exc)
        finally:
            async with self._metadata_lock:
                self._metadata.pop(container_id, None)

    async def _is_container_healthy(self, container_id: str) -> bool:
        loop = asyncio.get_running_loop()
        try:

            def _check():
                c = self._docker.containers.get(container_id)
                c.reload()  # ← fresh status from daemon
                return c.status == "running"

            return await loop.run_in_executor(None, _check)
        except NotFound:
            return False
        except Exception as exc:
            logger.error("Health check error for %s: %s", container_id[:12], exc)
            return False

    async def _wipe_workspace(self, container_id: str) -> bool:
        """Wipe /workspace inside the container to prevent cross-run / cross-tenant leaks."""
        loop = asyncio.get_running_loop()
        try:
            container = await loop.run_in_executor(
                None, lambda: self._docker.containers.get(container_id)
            )
            cmd = [
                "sh",
                "-lc",
                "rm -rf /workspace/* /workspace/.[!.]* /workspace/..?* 2>/dev/null || true",
            ]
            runner = None
            try:
                from hfa_tools.sandbox.runner import CodeRunner

                runner = CodeRunner(self._docker, container_id)
                res = await runner.run_command(cmd=cmd, timeout=5, workdir="/")
                return bool(res.get("exit_code", 1) == 0)
            except Exception:
                # Fallback: docker exec without CodeRunner
                def _exec():
                    exec_cfg = container.client.api.exec_create(
                        container=container_id,
                        cmd=cmd,
                        workdir="/",
                        user="nobody",
                    )
                    out = container.client.api.exec_start(exec_cfg["Id"], stream=False)
                    ins = container.client.api.exec_inspect(exec_cfg["Id"])
                    return ins.get("ExitCode", 1) == 0

                return await loop.run_in_executor(None, _exec)
        except Exception as e:
            logger.error("Workspace wipe failed for %s: %s", container_id[:12], e, exc_info=True)
            return False

    async def _refill_pool(self, language: str) -> None:
        """Top up pool to pool_size."""
        current = self._pool[language].qsize()
        needed = self._pool_size - current
        if needed <= 0:
            return
        logger.debug("Refilling %s pool: +%d containers", language, needed)
        for _ in range(needed):
            try:
                cid = await self._create_container(language)
                await self._pool[language].put(cid)
            except Exception as exc:
                logger.error("Refill failed for %s: %s", language, exc)
                break

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------

    async def _warmup_loop(self) -> None:
        while self._running:
            try:
                for lang in self._languages:
                    await self._refill_pool(lang)
                await asyncio.sleep(self._warmup_interval)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Warmup loop error: %s", exc, exc_info=True)
                await asyncio.sleep(10)

    async def _cleanup_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._cleanup_interval)
                now = time.time()
                threshold = now - self._container_max_age
                async with self._metadata_lock:
                    stale = [
                        cid
                        for cid, meta in self._metadata.items()
                        if meta.get("created_at", now) < threshold
                    ]
                for cid in stale:
                    logger.info("Removing stale container %s", cid[:12])
                    await self._remove_container(cid)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Cleanup loop error: %s", exc, exc_info=True)
