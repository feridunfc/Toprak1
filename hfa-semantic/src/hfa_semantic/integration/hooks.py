from __future__ import annotations

from typing import Optional

from hfa_semantic.api.models import SemanticQueryRequest
from hfa_semantic.query.merge_engine import MergeEngine
from hfa_semantic.reasoning.evaluator import IncrementalEvaluator
from hfa_semantic.runtime.engine import SemanticRuntimeEngine


async def semantic_preprocess_hook(raw_event: dict, engine: SemanticRuntimeEngine) -> tuple[bool, Optional[dict]]:
    return await engine.process_event(raw_event)


async def semantic_query_hook(request: SemanticQueryRequest, merge_engine: MergeEngine):
    return await merge_engine.execute_query(request)


async def semantic_reasoning_hook(enriched_context: dict, evaluator: IncrementalEvaluator):
    return await evaluator.evaluate_event(
        enriched_context["raw"],
        enriched_context["partition_key"],
        enriched_context["raw"]["timestamp_ms"],
    )
