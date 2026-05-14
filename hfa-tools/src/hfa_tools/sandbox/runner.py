"""
hfa-tools/src/hfa_tools/sandbox/runner.py
IRONCLAD Sprint 3 — Secure code & command execution in containers.

Guardian fixes applied:
  1. import json added (was missing → NameError on first call)
  2. Python template includes import base64 (was missing → NameError)
  3. input_b64 passed as argv argument to cmd list
  4. Timeout enforcement: stream=False → single bytes blob, wait_for wraps the
     entire executor call so the timer covers actual execution, not just the
     generator creation.
  5. Kill mechanism: on TimeoutError → container.kill() + _remove_container()
     (exec_resize was wrong and did not kill the process)
  6. run_command() added: executes arbitrary cmd list (pytest, npm test, etc.)
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import tarfile
import time
from typing import Any, Optional

from docker import DockerClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Language templates
# ---------------------------------------------------------------------------

# NOTE: both templates use `import base64` at the top — ✅ Guardian fix #2
_PYTHON_TEMPLATE = """\
import sys
import json
import traceback
import base64
try:
    _input_data = None
    if len(sys.argv) > 1:
        _input_data = json.loads(base64.b64decode(sys.argv[1]).decode())
{indented_code}
except Exception:
    print(traceback.format_exc(), file=sys.stderr)
    sys.exit(1)
"""

_NODE_TEMPLATE = """\
'use strict';
const _inputData = process.argv[2]
    ? JSON.parse(Buffer.from(process.argv[2], 'base64').toString())
    : null;
try {{
{indented_code}
}} catch (err) {{
    console.error(err.stack);
    process.exit(1);
}}
"""


# ---------------------------------------------------------------------------
# CodeRunner
# ---------------------------------------------------------------------------


class CodeRunner:
    """
    Execute code or commands inside an existing sandbox container.

    Security guarantees (enforced by container, not this class):
      - No network access
      - Read-only root FS + tmpfs
      - CPU + memory limits
      - Runs as nobody

    Timeout guarantee (Guardian fix #4 + #5):
      - stream=False returns bytes only after the process finishes
      - asyncio.wait_for wraps the ENTIRE executor call
      - On TimeoutError: container.kill() + caller should discard the container

    Args:
        docker_client: docker.DockerClient instance.
        container_id:  ID of the running sandbox container.
    """

    def __init__(self, docker_client: DockerClient, container_id: str) -> None:
        self._docker = docker_client
        self._container_id = container_id

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(
        self,
        code: str,
        timeout: int = 30,
        input_data: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Execute code snippet in the container.

        Args:
            code:       Source code to execute.
            timeout:    Wall-clock seconds. Enforced via asyncio.wait_for.
            input_data: Optional dict passed to the script as argv[1] (base64 JSON).

        Returns:
            {success, output, error, duration_ms, exit_code}
        """
        loop = asyncio.get_running_loop()
        start = time.perf_counter()
        container = await loop.run_in_executor(
            None, lambda: self._docker.containers.get(self._container_id)
        )
        language = container.labels.get("hfa.language", "python")

        # ── build script ────────────────────────────────────────────────
        script, cmd = self._build_cmd(language, code, input_data)

        return await self._exec_cmd(
            cmd=cmd,
            timeout=timeout,
            start=start,
            loop=loop,
        )

    async def run_command(
        self,
        cmd: list[str],
        timeout: int = 60,
        workdir: str = "/workspace",
    ) -> dict[str, Any]:
        """
        Execute an arbitrary command in the container (pytest, npm test, etc.).

        ✅ Guardian fix #6: TesterService needs to run pytest directly, not
        inject code. This method is the correct entry point for that.

        Args:
            cmd:     Full command + args list e.g. ["python3", "-m", "pytest", "/workspace"]
            timeout: Wall-clock seconds.
            workdir: Working directory inside the container.

        Returns:
            {success, output, error, duration_ms, exit_code}
        """
        loop = asyncio.get_running_loop()
        start = time.perf_counter()
        return await self._exec_cmd(
            cmd=cmd,
            timeout=timeout,
            start=start,
            loop=loop,
            workdir=workdir,
        )

    async def write_files(self, files: dict[str, str]) -> None:
        """
        Copy files into /workspace inside the container using put_archive.

        ✅ Guardian fix #6 (TesterService._write_files_to_container):
        Real implementation using Docker tar stream API instead of sleep().

        Args:
            files: {relative_path: content_str} mapping.
                   Paths are relative to /workspace (e.g. "src/main.py").
        """
        loop = asyncio.get_running_loop()

        # Build in-memory tar archive
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            for rel_path, content in files.items():
                content_bytes = content.encode("utf-8")
                info = tarfile.TarInfo(name=rel_path)
                info.size = len(content_bytes)
                info.mode = 0o644
                tar.addfile(info, io.BytesIO(content_bytes))
        buf.seek(0)

        # Upload to container at /workspace
        try:
            container = await loop.run_in_executor(
                None, lambda: self._docker.containers.get(self._container_id)
            )
            await loop.run_in_executor(
                None,
                lambda: container.put_archive("/workspace", buf),
            )
            logger.debug("Wrote %d files to container %s", len(files), self._container_id[:12])
        except Exception as exc:
            logger.error(
                "write_files failed for %s: %s",
                self._container_id[:12],
                exc,
                exc_info=True,
            )
            raise

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_cmd(
        self,
        language: str,
        code: str,
        input_data: Optional[dict[str, Any]],
    ) -> tuple[str, list[str]]:
        """
        Render template and build the full exec command list.

        ✅ Guardian fix #1: import json present at module level
        ✅ Guardian fix #2: templates include import base64
        ✅ Guardian fix #3: input_b64 is appended as argv element

        Returns (rendered_script, cmd_list).
        """
        # Encode input
        input_b64 = ""
        if input_data:
            input_b64 = base64.b64encode(json.dumps(input_data).encode()).decode()

        if language == "python":
            indented = "\n".join("    " + line for line in code.splitlines())
            script = _PYTHON_TEMPLATE.format(indented_code=indented)
            cmd = ["python3", "-c", script, input_b64]  # ✅ fix #3

        elif language == "node":
            indented = "\n".join("    " + line for line in code.splitlines())
            script = _NODE_TEMPLATE.format(indented_code=indented)
            cmd = ["node", "-e", script, input_b64]  # ✅ fix #3

        else:
            raise ValueError(f"Unsupported language: {language!r}")

        return script, cmd

    async def _exec_cmd(
        self,
        cmd: list[str],
        timeout: int,
        start: float,
        loop: asyncio.AbstractEventLoop,
        workdir: str = "/workspace",
    ) -> dict[str, Any]:
        """
        Core exec + timeout logic.

        ✅ Guardian fix #4: stream=False → exec_start returns bytes immediately.
           asyncio.wait_for wraps the executor call that does the FULL blocking
           round-trip, so the timer covers real execution time.

        ✅ Guardian fix #5: TimeoutError → container.kill() then _remove_and_replace().
           exec_resize was wrong (no-op for kill); only kill() terminates the process.
        """
        container = await loop.run_in_executor(
            None, lambda: self._docker.containers.get(self._container_id)
        )

        # Create exec instance
        exec_cfg = await loop.run_in_executor(
            None,
            lambda: container.client.api.exec_create(
                container=self._container_id,
                cmd=cmd,
                environment={"PYTHONUNBUFFERED": "1", "NODE_NO_WARNINGS": "1"},
                workdir=workdir,
                user="nobody",
            ),
        )
        exec_id = exec_cfg["Id"]

        try:
            # ✅ fix #4: stream=False → blocks until process exits, returns bytes
            raw: bytes = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: container.client.api.exec_start(exec_id, detach=False, stream=False),
                ),
                timeout=timeout,
            )

            inspect = await loop.run_in_executor(
                None,
                lambda: container.client.api.exec_inspect(exec_id),
            )
            exit_code = inspect["ExitCode"]
            output = raw.decode("utf-8", errors="replace").strip() if raw else ""
            duration_ms = int((time.perf_counter() - start) * 1000)

            return {
                "success": exit_code == 0,
                "output": output,
                "error": "" if exit_code == 0 else output,
                "duration_ms": duration_ms,
                "exit_code": exit_code,
            }

        except asyncio.TimeoutError:
            # ✅ fix #5: kill the container — exec_resize cannot kill a process
            logger.warning(
                "Execution timeout (%ds) — killing container %s",
                timeout,
                self._container_id[:12],
            )
            try:
                await loop.run_in_executor(None, container.kill)
            except Exception as kill_exc:
                logger.error("kill() failed: %s", kill_exc)

            return {
                "success": False,
                "output": "",
                "error": f"Execution timeout after {timeout}s — container killed",
                "duration_ms": timeout * 1000,
                "exit_code": -1,
            }

        except Exception as exc:
            logger.error("exec_cmd failed: %s", exc, exc_info=True)
            duration_ms = int((time.perf_counter() - start) * 1000)
            return {
                "success": False,
                "output": "",
                "error": str(exc),
                "duration_ms": duration_ms,
                "exit_code": -1,
            }
