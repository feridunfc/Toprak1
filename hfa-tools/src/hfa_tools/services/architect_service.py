"""
hfa-tools/src/hfa_tools/services/architect_service.py
IRONCLAD — Architect service that creates/modifies plans
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException

from hfa.llm.robust_client import RobustLLMClient, LLMCallError
from hfa.schemas.agent import ArchitectRequest, PlanManifest
from hfa.core.config import (
    settings,
)  # NOTE: keep as-is if your config module is here; update if needed

logger = logging.getLogger(__name__)


class ArchitectService:
    """Architect agent service: turns requirements into validated PlanManifest."""

    def __init__(self, llm_client: Optional[RobustLLMClient] = None):
        self.llm_client = llm_client or RobustLLMClient(
            provider=settings.LLM_PROVIDER,
            default_model=settings.LLM_MODEL,
        )
        logger.info("ArchitectService initialized")

    async def create_plan(self, request: ArchitectRequest) -> PlanManifest:
        logger.info(f"Creating plan for tenant={request.tenant_id}, run={request.run_id}")

        plan_id = request.plan_id or f"plan-{request.tenant_id}-{uuid.uuid4().hex[:8]}"

        system_prompt = (
            "You are an expert software architect.\n"
            "Create a detailed, actionable plan from the requirement.\n"
            "The plan must include: title, description, requirements, steps, estimated_tokens.\n"
            "Be specific and practical."
        )

        user_prompt = (
            f"Requirement: {request.requirement}\n\n"
            f"Context: {request.context if request.context else 'No additional context'}\n\n"
            f"Generate a comprehensive plan with ID: {plan_id}"
        )

        try:
            manifest = await self.llm_client.generate_structured(
                prompt=user_prompt,
                response_model=PlanManifest,
                system_prompt=system_prompt,
                temperature=0.2,
                retry_on_validation=True,
            )

            # fail-closed: enforce plan_id we generated
            manifest.plan_id = plan_id

            # timestamp set by service (since created_at is Optional in schema)
            manifest.created_at = datetime.now(timezone.utc).isoformat()

            logger.info(f"Plan created successfully: {plan_id}")
            return manifest

        except LLMCallError as e:
            logger.error(f"LLM call failed: {e}", exc_info=True)
            raise HTTPException(status_code=503, detail="Architect service unavailable")
        except Exception as e:
            logger.critical(f"Unexpected error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Internal server error")

    async def modify_plan(self, existing_plan: PlanManifest, new_requirement: str) -> PlanManifest:
        logger.info(f"Modifying plan: {existing_plan.plan_id}")

        system_prompt = (
            "You are modifying an existing plan. "
            "Preserve the original plan ID and structure, but incorporate the new requirement. "
            "Update steps and estimations accordingly."
        )

        user_prompt = (
            "Existing Plan:\n"
            f"Title: {existing_plan.title}\n"
            f"Description: {existing_plan.description}\n"
            f"Current Requirements: {existing_plan.requirements}\n"
            f"Current Steps: {existing_plan.steps}\n\n"
            f"New Requirement: {new_requirement}\n\n"
            f"Generate updated plan maintaining original ID: {existing_plan.plan_id}"
        )

        manifest = await self.llm_client.generate_structured(
            prompt=user_prompt,
            response_model=PlanManifest,
            system_prompt=system_prompt,
            temperature=0.1,
        )

        manifest.plan_id = existing_plan.plan_id
        manifest.created_at = datetime.now(timezone.utc).isoformat()

        logger.info(f"Plan modified: {manifest.plan_id}")
        return manifest

    async def validate_plan(self, plan: PlanManifest) -> bool:
        if not plan.plan_id or len(plan.plan_id) < 3:
            raise ValueError("Invalid plan_id")
        if not plan.title or len(plan.title) < 3:
            raise ValueError("Title too short")
        if not plan.description or len(plan.description) < 10:
            raise ValueError("Description too short")
        if not plan.requirements:
            raise ValueError("At least one requirement needed")
        if not plan.steps:
            raise ValueError("At least one step needed")
        if plan.estimated_tokens < 1:
            raise ValueError("Invalid token estimation")
        return True

    async def close(self):
        await self.llm_client.close()
        logger.info("ArchitectService closed")
