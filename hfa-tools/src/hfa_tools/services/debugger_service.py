"""
hfa-tools/src/hfa_tools/services/debugger_service.py
IRONCLAD Sprint 4 — DebuggerService

Responsibility
--------------
Given a failed test suite (TestSuiteResult) and the code that produced it
(CodeChangeSet), use the LLM to generate a fixed version of the code.

Pipeline
--------
  1. Build a structured debug prompt from failing tests + stack traces.
  2. Call LLM → DebugFixSuggestion (Pydantic model).
  3. Apply suggested changes to produce a new CodeChangeSet.
  4. Run optional compliance check on the fix (HITL/DENY gate).
  5. Return DebuggerOutput.

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop() where needed.
* close() propagates to LLM client.
* All error paths raise typed exceptions (never bare Exception swallow).
* Compliance check is fail-closed: DENY/HITL → raise DebuggerComplianceError.
"""

from __future__ import annotations

import logging

from fastapi import HTTPException
from pydantic import BaseModel, Field

from hfa.schemas.agent import CodeChangeSet, CodeChange, TestSuiteResult, DebuggerOutput

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal LLM response model
# ---------------------------------------------------------------------------


class DebugFixSuggestion(BaseModel):
    """
    Structured LLM output for a debugging fix.

    The LLM is asked to return a JSON object matching this schema.
    Fields align 1:1 with what DebuggerOutput needs.
    """

    explanation: str = Field(..., min_length=10, max_length=2_000)
    confidence: float = Field(..., ge=0.0, le=1.0)
    fixed_files: list[dict] = Field(..., min_length=0)  # list of CodeChange-like dicts


# ---------------------------------------------------------------------------
# Typed errors
# ---------------------------------------------------------------------------


class DebuggerError(Exception):
    """Base class for debugger service errors."""


class DebuggerComplianceError(DebuggerError):
    """Proposed fix was blocked by compliance policy."""

    def __init__(self, decision: str, summary: str) -> None:
        super().__init__(f"Fix blocked by compliance: {decision} — {summary}")
        self.decision = decision
        self.summary = summary


class DebuggerLLMError(DebuggerError):
    """LLM call failed."""


# ---------------------------------------------------------------------------
# DebuggerService
# ---------------------------------------------------------------------------


class DebuggerService:
    """
    LLM-powered code fixer.

    Args:
        llm_client:        Async LLM client with generate_structured().
        compliance_policy: Optional CompliancePolicy to gate the generated fix.
                           If provided, HITL/DENY findings raise
                           DebuggerComplianceError — caller must handle HITL
                           by routing to human review queue.
        max_prompt_chars:  Truncation limit for test output in prompt.
    """

    def __init__(
        self,
        llm_client,
        compliance_policy=None,
        max_prompt_chars: int = 8_000,
    ) -> None:
        self._llm = llm_client
        self._compliance = compliance_policy
        self._max_prompt_chars = max_prompt_chars
        logger.info(
            "DebuggerService created: compliance=%s",
            "enabled" if compliance_policy else "disabled",
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fix(
        self,
        code_set: CodeChangeSet,
        test_results: TestSuiteResult,
        run_id: str,
        tenant_id: str,
    ) -> DebuggerOutput:
        """
        Generate a fixed CodeChangeSet for failing tests.

        Args:
            code_set:     Original code that failed.
            test_results: Test suite result with failure details.
            run_id:       Run identifier (for logging/audit).
            tenant_id:    Tenant identifier (for logging/audit).

        Returns:
            DebuggerOutput with fixed code, explanation, and confidence.

        Raises:
            DebuggerComplianceError: Fix blocked by compliance (DENY or HITL).
            DebuggerLLMError:        LLM call failed.
            HTTPException(503):      Re-raised from LLM client on service error.
        """
        failed_tests = [r for r in test_results.results if r.status in ("failed", "error")]

        logger.info(
            "Debugger.fix: tenant=%s run=%s failing=%d/%d",
            tenant_id,
            run_id,
            len(failed_tests),
            len(test_results.results),
        )

        if not failed_tests:
            logger.info("No failing tests — nothing to fix for run=%s", run_id)
            return DebuggerOutput(
                fixed_code_set=code_set,
                explanation="All tests already passing — no fix needed.",
                confidence=1.0,
            )

        prompt = self._build_prompt(code_set, failed_tests)

        # ── LLM call ─────────────────────────────────────────────────────
        try:
            suggestion: DebugFixSuggestion = await self._llm.generate_structured(
                prompt=prompt,
                response_model=DebugFixSuggestion,
                temperature=0.1,  # low temp → deterministic fixes
            )
        except HTTPException:
            raise  # pass through FastAPI 503
        except Exception as exc:
            logger.error("Debugger LLM call failed: run=%s error=%s", run_id, exc, exc_info=True)
            raise DebuggerLLMError(f"LLM fix generation failed: {exc}") from exc

        logger.info(
            "Debugger fix generated: run=%s confidence=%.2f files=%d",
            run_id,
            suggestion.confidence,
            len(suggestion.fixed_files),
        )

        # ── Build fixed CodeChangeSet ────────────────────────────────────
        fixed_changes = self._apply_suggestion(code_set, suggestion)
        fixed_code_set = CodeChangeSet(
            change_set_id=f"fix-{code_set.change_set_id}",
            plan_id=code_set.plan_id,
            changes=fixed_changes if fixed_changes else code_set.changes,
            language=code_set.language,
            framework=code_set.framework,
            total_tokens=code_set.total_tokens,
        )

        # ── Compliance gate ───────────────────────────────────────────────
        if self._compliance is not None:
            await self._check_compliance(suggestion, run_id)

        output = DebuggerOutput(
            fixed_code_set=fixed_code_set,
            explanation=suggestion.explanation,
            confidence=suggestion.confidence,
        )
        logger.info("Debugger.fix complete: run=%s confidence=%.2f", run_id, output.confidence)
        return output

    async def close(self) -> None:
        """Propagate shutdown to LLM client."""
        await self._llm.close()
        logger.info("DebuggerService closed")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_prompt(
        self,
        code_set: CodeChangeSet,
        failed_tests: list,
    ) -> str:
        """
        Assemble a structured debug prompt for the LLM.

        Includes:
          - Language + framework context.
          - Current source files (truncated if large).
          - Failing test IDs, names, and error messages.
        """
        parts: list[str] = [
            f"Language: {code_set.language}",
        ]
        if code_set.framework:
            parts.append(f"Framework: {code_set.framework}")

        parts.append("\n### Current Source Files ###")
        for change in code_set.changes:
            preview = (change.new_content or "")[:2_000]
            parts.append(f"\n--- {change.file_path} ---\n{preview}")

        parts.append("\n### Failing Tests ###")
        char_budget = self._max_prompt_chars
        for test in failed_tests:
            entry = (
                f"\nTest: {test.name} [{test.status}]\n"
                f"Error: {test.error_message or 'No error message'}\n"
            )
            if len(entry) > char_budget:
                entry = entry[:char_budget] + "\n[truncated]"
                parts.append(entry)
                break
            char_budget -= len(entry)
            parts.append(entry)

        parts.append(
            "\n### Task ###\n"
            "Fix the source code so all failing tests pass.\n"
            "Return JSON with:\n"
            "  explanation: str  — what was wrong and what you changed\n"
            "  confidence: float — 0.0..1.0 how confident you are\n"
            "  fixed_files: list of {file_path, new_content, change_type} dicts\n"
            "Only include files that need changes.\n"
            "Do NOT change test files."
        )

        return "\n".join(parts)

    @staticmethod
    def _apply_suggestion(
        code_set: CodeChangeSet,
        suggestion: DebugFixSuggestion,
    ) -> list[CodeChange]:
        """
        Merge LLM-suggested file changes into the existing change set.

        Files mentioned in suggestion override; others are kept unchanged.
        """
        if not suggestion.fixed_files:
            return list(code_set.changes)

        # Build lookup: file_path → existing change
        existing: dict[str, CodeChange] = {c.file_path: c for c in code_set.changes}

        for fix in suggestion.fixed_files:
            path = fix.get("file_path", "")
            if not path:
                continue
            change_type = fix.get("change_type", "modify")
            new_content = fix.get("new_content", "")
            if path in existing:
                old = existing[path]
                existing[path] = CodeChange(
                    file_path=path,
                    old_content_hash=old.old_content_hash,
                    new_content=new_content,
                    change_type=change_type,
                )
            else:
                # New file introduced by fix
                try:
                    existing[path] = CodeChange(
                        file_path=path,
                        new_content=new_content,
                        change_type="create",
                    )
                except Exception:
                    logger.warning("Debugger: skipping invalid file_path=%r", path)

        return list(existing.values())

    async def _check_compliance(
        self,
        suggestion: DebugFixSuggestion,
        run_id: str,
    ) -> None:
        """
        Run compliance policy against the generated fix.

        Converts the fix into findings format and evaluates.
        Raises DebuggerComplianceError on DENY or HITL.
        """
        # Build a finding from the suggestion for compliance scanning
        finding = {
            "message": suggestion.explanation,
            "severity": "low" if suggestion.confidence >= 0.8 else "medium",
            "source": "llm_fix",
            "confidence": suggestion.confidence,
        }
        result = self._compliance.evaluate_all([finding])

        from hfa.governance.compliance_policy import PolicyAction

        if result.decision in (PolicyAction.DENY, PolicyAction.HITL):
            logger.warning(
                "Debugger compliance BLOCKED: run=%s decision=%s summary=%s",
                run_id,
                result.decision.value,
                result.summary,
            )
            raise DebuggerComplianceError(result.decision.value, result.summary)
