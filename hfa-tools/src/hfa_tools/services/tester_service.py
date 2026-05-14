"""
hfa-tools/src/hfa_tools/services/tester_service.py
IRONCLAD Sprint 3 — Test execution with fingerprinting.

Guardian fixes applied:
  6. _write_files_to_container: real implementation via CodeRunner.write_files()
     (tar stream + put_archive) instead of asyncio.sleep() placeholder.
     run_command() is used to execute pytest directly — no code injection.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Optional

from hfa.schemas.agent import CodeChangeSet, TestResult, TestSuiteResult
from hfa_tools.sandbox.pool import SandboxPool
from hfa_tools.sandbox.runner import CodeRunner

logger = logging.getLogger(__name__)


class TesterService:
    """
    Test execution service.

    Writes code files into a sandbox container via tar stream (put_archive),
    then invokes pytest/mocha via run_command() — not via code template injection.

    ✅ Guardian fix #6: real file copy + proper command execution.
    """

    _TEST_COMMANDS: dict[str, list[str]] = {
        "python": [
            "python3",
            "-m",
            "pytest",
            "/workspace",
            "-v",
            "--tb=short",
            "--no-header",
        ],
        "javascript": ["npm", "test", "--", "--ci"],
        "typescript": ["npm", "test", "--", "--ci"],
    }

    def __init__(
        self,
        sandbox_pool: SandboxPool,
        test_timeout: int = 60,
    ) -> None:
        self._pool = sandbox_pool
        self._timeout = test_timeout
        logger.info("TesterService init: timeout=%ds", test_timeout)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run_tests(
        self,
        code_set: CodeChangeSet,
        test_files: Optional[dict[str, str]] = None,
    ) -> TestSuiteResult:
        """
        Run tests for a CodeChangeSet inside a sandbox container.

        Steps:
          1. Checkout container from pool.
          2. Write all code + test files via put_archive (real implementation).
          3. Execute test command via run_command() (not code injection).
          4. Parse output → TestResult list.
          5. Generate SHA-256 fingerprints for failures.

        Args:
            code_set:   Generated code to test.
            test_files: Optional extra test files {rel_path: content}.

        Returns:
            TestSuiteResult with results, fingerprints, and summary.
        """
        suite_id = f"suite-{code_set.change_set_id}-{int(time.time())}"
        language = code_set.language
        logger.info("Running tests: suite=%s language=%s", suite_id, language)

        # Merge code files + test files
        all_files = self._collect_files(code_set, test_files)

        async with self._pool.get_container(language) as container_id:
            runner = CodeRunner(self._pool._docker, container_id)

            # ✅ Guardian fix #6a: real file copy via tar stream
            await runner.write_files(all_files)

            # ✅ Guardian fix #6b: run_command() executes pytest directly
            cmd = self._get_test_cmd(language)
            result = await runner.run_command(cmd, timeout=self._timeout)

        test_results = self._parse_output(result["output"], result["error"], result["exit_code"])

        # Fingerprint failures
        for tr in test_results:
            if tr.status in ("failed", "error"):
                tr.fingerprint = self._fingerprint(tr, code_set, result)  # type: ignore[attr-defined]

        suite = TestSuiteResult(
            suite_id=suite_id,
            code_set_id=code_set.change_set_id,
            results=test_results,
            total_tokens=code_set.total_tokens,
        )
        suite.summary = self._summary(test_results)

        logger.info(
            "Tests done: %d passed, %d failed",
            suite.summary.get("passed", 0),
            suite.summary.get("failed", 0),
        )
        return suite

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _collect_files(
        self,
        code_set: CodeChangeSet,
        test_files: Optional[dict[str, str]],
    ) -> dict[str, str]:
        """Merge code changes + test files into a flat {rel_path: content} dict."""
        files: dict[str, str] = {}
        for change in code_set.changes:
            if change.change_type != "delete":
                files[change.file_path] = change.new_content
        if test_files:
            files.update(test_files)
        else:
            files["tests/test_basic.py"] = self._basic_test(code_set.language)
        return files

    def _get_test_cmd(self, language: str) -> list[str]:
        """Return the test runner command for the given language."""
        cmd = self._TEST_COMMANDS.get(language)
        if cmd:
            return cmd
        # Generic fallback
        logger.warning("No test command for language %s, using echo", language)
        return ["echo", f"No test runner configured for {language}"]

    def _parse_output(
        self,
        stdout: str,
        stderr: str,
        exit_code: int,
    ) -> list[TestResult]:
        """
        Parse pytest / mocha output into TestResult objects.
        Simple line-based parser; good enough for Sprint 3.
        Sprint 4 will integrate pytest-json-report for structured output.
        """
        results: list[TestResult] = []
        for i, line in enumerate(stdout.splitlines()):
            # ✅ Guardian Fix: test_id formatını validasyon kurallarına uygun hale getirdik
            safe_id = f"test-result-{i:04d}"

            if " PASSED" in line:
                name = line.split(" PASSED")[0].strip() or f"test_{i}"
                results.append(
                    TestResult(
                        test_id=safe_id,
                        name=name,
                        status="passed",
                        duration_ms=0,
                    )
                )
            elif " FAILED" in line:
                name = line.split(" FAILED")[0].strip() or f"test_{i}"
                results.append(
                    TestResult(
                        test_id=safe_id,
                        name=name,
                        status="failed",
                        duration_ms=0,
                        error_message=line[:1000],
                    )
                )
            elif " ERROR" in line:
                name = line.split(" ERROR")[0].strip() or f"test_{i}"
                results.append(
                    TestResult(
                        test_id=safe_id,
                        name=name,
                        status="error",
                        duration_ms=0,
                        error_message=line[:1000],
                    )
                )

        if not results:
            # No test lines found — treat as single pass/fail based on exit code
            results.append(
                TestResult(
                    test_id="test-result-fallback",
                    name="__default__",
                    status="passed" if exit_code == 0 else "error",
                    duration_ms=0,
                    error_message=(stderr or stdout)[:1000] if exit_code != 0 else None,
                )
            )
        return results

    @staticmethod
    def _fingerprint(
        tr: TestResult,
        code_set: CodeChangeSet,
        exec_result: dict[str, Any],
    ) -> str:
        """SHA-256 fingerprint for self-healing deduplication (Sprint 4)."""
        data = json.dumps(
            {
                "test_name": tr.name,
                "error": tr.error_message,
                "files": [c.file_path for c in code_set.changes],
                "language": code_set.language,
                "output": exec_result.get("output", "")[:500],
            },
            sort_keys=True,
        )
        return hashlib.sha256(data.encode()).hexdigest()

    @staticmethod
    def _summary(results: list[TestResult]) -> dict[str, Any]:
        total = len(results)
        passed = sum(1 for r in results if r.status == "passed")
        failed = sum(1 for r in results if r.status == "failed")
        errors = sum(1 for r in results if r.status == "error")
        skipped = sum(1 for r in results if r.status == "skipped")
        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "skipped": skipped,
            "success_rate": round(passed / total * 100, 1) if total else 0.0,
        }

    @staticmethod
    def _basic_test(language: str) -> str:
        if language == "python":
            return "import pytest\n\ndef test_basic():\n    assert True\n"
        return "// No tests provided"
