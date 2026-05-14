"""
hfa-tools/src/hfa_tools/services/coder_service.py
IRONCLAD Sprint 3 — Code generation from PlanManifest.
"""

from __future__ import annotations

import logging
import uuid
from typing import Optional

from hfa.llm.robust_client import LLMCallError, RobustLLMClient
from hfa.schemas.agent import CodeChangeSet, PlanManifest

logger = logging.getLogger(__name__)

_SYSTEM_PROMPTS: dict[str, str] = {
    "python": (
        "You are an expert Python developer. "
        "Generate clean, production-ready code with docstrings, type hints, and error handling. "
        "Follow PEP 8."
    ),
    "javascript": (
        "You are an expert JavaScript developer. "
        "Generate modern ES6+ code with JSDoc comments and error handling."
    ),
    "typescript": (
        "You are an expert TypeScript developer. "
        "Generate clean typed code with interfaces and proper error handling."
    ),
}


class CoderService:
    """
    Code generation service: converts PlanManifest → CodeChangeSet.

    Uses RobustLLMClient with structured outputs (CodeChangeSet as response_model).
    Temperature 0.2 for deterministic, lower-creativity output.
    """

    def __init__(
        self,
        llm_client: RobustLLMClient,
        default_language: str = "python",
    ) -> None:
        self._llm = llm_client
        self._language = default_language
        logger.info("CoderService init: default_language=%s", default_language)

    async def generate_code(
        self,
        plan: PlanManifest,
        language: Optional[str] = None,
        framework: Optional[str] = None,
        existing_code: Optional[CodeChangeSet] = None,
    ) -> CodeChangeSet:
        """
        Generate code from a plan.

        Args:
            plan:          PlanManifest produced by ArchitectService.
            language:      Target language (default: self._language).
            framework:     Optional framework hint (fastapi, express, …).
            existing_code: If provided, modify existing files rather than create new.

        Returns:
            CodeChangeSet with validated file changes.

        Raises:
            LLMCallError: If LLM is unreachable after retries.
            ValueError:   If change_set validation fails.
        """
        lang = language or self._language
        logger.info("Generating %s code for plan %s", lang, plan.plan_id)

        system_prompt = _SYSTEM_PROMPTS.get(lang, f"You are an expert {lang} developer.")
        if framework:
            system_prompt += f" Use the {framework} framework."

        user_prompt = (
            self._modification_prompt(plan, lang, framework, existing_code)
            if existing_code
            else self._generation_prompt(plan, lang, framework)
        )

        try:
            change_set: CodeChangeSet = await self._llm.generate_structured(
                prompt=user_prompt,
                response_model=CodeChangeSet,
                system_prompt=system_prompt,
                temperature=0.2,
                retry_on_validation=True,
            )
        except LLMCallError:
            logger.error("Code generation LLM call failed for plan %s", plan.plan_id)
            raise

        # Enforce IDs
        if not change_set.change_set_id or len(change_set.change_set_id) < 3:
            change_set.change_set_id = f"cs-{plan.plan_id}-{uuid.uuid4().hex[:8]}"
        change_set.plan_id = plan.plan_id

        self._validate(change_set, lang)

        logger.info(
            "Code generated: %d files, %d tokens",
            len(change_set.changes),
            change_set.total_tokens,
        )
        return change_set

    # ------------------------------------------------------------------
    # Prompt builders
    # ------------------------------------------------------------------

    @staticmethod
    def _generation_prompt(
        plan: PlanManifest,
        language: str,
        framework: Optional[str],
    ) -> str:
        steps = "\n".join(
            f"Step {i + 1}: {s.get('description', str(s))}" for i, s in enumerate(plan.steps)
        )
        reqs = "\n".join(f"- {r}" for r in plan.requirements)
        fw = f"\nFramework: {framework}" if framework else ""
        return (
            f"Plan: {plan.title}\n"
            f"Description: {plan.description}\n\n"
            f"Requirements:\n{reqs}\n\n"
            f"Steps:\n{steps}\n\n"
            f"Language: {language}{fw}\n\n"
            "Generate a complete implementation. "
            "Create all necessary files with imports, configuration, and docs.\n"
            "Output: CodeChangeSet with file_path, new_content, change_type."
        )

    @staticmethod
    def _modification_prompt(
        plan: PlanManifest,
        language: str,
        framework: Optional[str],
        existing: CodeChangeSet,
    ) -> str:
        files = "\n".join(f"- {c.file_path}" for c in existing.changes)
        reqs = "\n".join(f"- {r}" for r in plan.requirements)
        fw = f"\nFramework: {framework}" if framework else ""
        return (
            f"Plan: {plan.title}\n"
            f"Description: {plan.description}\n\n"
            f"Requirements:\n{reqs}\n\n"
            f"Existing files:\n{files}\n\n"
            f"Language: {language}{fw}\n\n"
            "Modify existing code to implement the plan. "
            "Preserve existing functionality.\n"
            "Output: CodeChangeSet with only changed/new files."
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate(change_set: CodeChangeSet, language: str) -> None:
        if not change_set.changes:
            raise ValueError("CoderService: LLM returned empty change set")

        ext_map = {
            "python": ".py",
            "javascript": ".js",
            "typescript": ".ts",
            "go": ".go",
            "rust": ".rs",
            "java": ".java",
        }
        expected = ext_map.get(language)
        if expected:
            for c in change_set.changes:
                if not c.file_path.endswith(expected):
                    logger.warning(
                        "File %s extension mismatch for language %s",
                        c.file_path,
                        language,
                    )
        for c in change_set.changes:
            if c.change_type == "modify" and not c.old_content_hash:
                logger.warning("Modification without old_content_hash: %s", c.file_path)

    async def close(self) -> None:
        await self._llm.close()
        logger.info("CoderService closed")
