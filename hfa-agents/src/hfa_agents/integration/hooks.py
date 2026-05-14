from __future__ import annotations

from typing import Any

from hfa_agents.base.contracts import EnrichedEvent


async def agent_preprocess_hook(raw_event: dict, semantic_engine: Any) -> EnrichedEvent:
    enriched = EnrichedEvent(
        event_id=raw_event["event_id"],
        lineage_run_id=raw_event.get("lineage_run_id"),
        goal=raw_event.get("goal", ""),
        context=raw_event.get("payload", {}),
    )
    should_process, ctx = await semantic_engine.process_event(raw_event)
    if should_process and ctx:
        enriched.semantic_matches = ctx.get("semantic_matches", [])
        enriched.context.update({"semantic_context": ctx})
    return enriched
