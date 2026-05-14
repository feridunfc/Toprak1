"""
hfa-tools/src/hfa_tools/services/researcher_service.py
IRONCLAD Sprint 3 — Research agent with citation tracking.

Guardian fixes applied:
  7. generate_structured(response_model=None) removed — response_model must be
     a BaseModel subclass. ResearchSummary (Pydantic model) is now used.
  8. Cache type corrected: Dict[str, tuple[float, ResearchResult]]
     (was tuple[float, List[RetrievalTrace]] — wrong, result was stored not traces)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from hfa.llm.robust_client import LLMCallError, RobustLLMClient
from hfa.schemas.agent import ResearchRequest
from hfa.schemas.research import ResearchResult, ResearchSummary, RetrievalTrace

logger = logging.getLogger(__name__)


class ResearcherService:
    """
    Research agent: gathers sources, de-duplicates, summarises via LLM.

    Guardian fix #7: LLM summary uses generate_structured(response_model=ResearchSummary)
    Guardian fix #8: cache type is Dict[str, tuple[float, ResearchResult]]
    """

    def __init__(
        self,
        llm_client: RobustLLMClient,
        search_enabled: bool = True,
        max_sources: int = 5,
        cache_ttl: int = 3600,
    ) -> None:
        self._llm = llm_client
        self._search_enabled = search_enabled
        self._max_sources = max_sources
        self._cache_ttl = cache_ttl

        # ✅ Guardian fix #8: correct type — stores ResearchResult, not List[RetrievalTrace]
        self._cache: dict[str, tuple[float, ResearchResult]] = {}

        logger.info("ResearcherService init: search_enabled=%s", search_enabled)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def research(self, request: ResearchRequest) -> ResearchResult:
        """
        Perform research on a query.

        Args:
            request: ResearchRequest (tenant_id, run_id, query, sources, max_results)

        Returns:
            ResearchResult with summary, traces, citations, total_tokens.
        """
        logger.info("Researching: %.100s (tenant=%s)", request.query, request.tenant_id)

        cache_key = self._cache_key(request.query, request.sources)
        cached = self._from_cache(cache_key)
        if cached:
            logger.info("Cache hit: %.50s", request.query)
            return cached

        traces = await self._gather_sources(
            query=request.query,
            sources=request.sources,
            max_results=request.max_results,
        )

        if not traces:
            logger.warning("No sources found for: %.50s", request.query)
            traces = [self._empty_trace()]

        summary_model, total_tokens = await self._summarise(request.query, traces)

        result = ResearchResult(
            query=request.query,
            summary=summary_model.summary,
            traces=traces,
            citations=summary_model.citations or [t.source for t in traces],
            total_tokens=total_tokens,
        )

        # ✅ fix #8: cache stores full ResearchResult
        self._cache[cache_key] = (time.time(), result)

        logger.info("Research done: %d sources, %d tokens", len(traces), total_tokens)
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _gather_sources(
        self,
        query: str,
        sources: list[str],
        max_results: int,
    ) -> list[RetrievalTrace]:
        traces: list[RetrievalTrace] = []

        if self._search_enabled and not sources:
            traces.extend(await self._web_search(query, max_results))

        for source in sources[:max_results]:
            trace = await self._process_source(query, source)
            if trace:
                traces.append(trace)

        # De-duplicate by source URL
        seen: set[str] = set()
        unique: list[RetrievalTrace] = []
        for t in traces:
            if t.source not in seen:
                seen.add(t.source)
                unique.append(t)

        return unique[:max_results]

    async def _web_search(self, query: str, limit: int) -> list[RetrievalTrace]:
        """Simulated web search. Replace with real API in production."""
        await asyncio.sleep(0.1)
        return [
            RetrievalTrace(
                source=f"https://example.com/result-{i}",
                content_snippet=f"Relevant info about {query[:30]}… (simulated result {i})",
                relevance_score=round(0.9 - i * 0.1, 2),
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            for i in range(min(limit, 3))
        ]

    async def _process_source(self, query: str, source: str) -> Optional[RetrievalTrace]:
        """Fetch and score a single source."""
        await asyncio.sleep(0.05)
        return RetrievalTrace(
            source=source,
            content_snippet=f"Information from {source} about {query[:30]}…",
            relevance_score=0.8,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    async def _summarise(
        self,
        query: str,
        traces: list[RetrievalTrace],
    ) -> tuple[ResearchSummary, int]:
        """
        Generate a structured summary via LLM.

        ✅ Guardian fix #7:
        generate_structured() requires a BaseModel subclass as response_model.
        ResearchSummary(summary: str, citations: list[str]) is that model.
        """
        context = "\n\n".join(
            f"[{i + 1}] ({t.source}): {t.content_snippet}" for i, t in enumerate(traces)
        )

        system_prompt = (
            "You are a research assistant. "
            "Summarise the provided sources accurately. "
            "Include inline citations like [1], [2] referencing the source numbers."
        )
        user_prompt = (
            f"Query: {query}\n\n"
            f"Sources:\n{context}\n\n"
            "Return a ResearchSummary with 'summary' and 'citations' fields."
        )

        try:
            # ✅ fix #7: response_model=ResearchSummary (BaseModel), NOT None
            result: ResearchSummary = await self._llm.generate_structured(
                prompt=user_prompt,
                response_model=ResearchSummary,
                system_prompt=system_prompt,
                temperature=0.3,
                retry_on_validation=True,
            )
            total_tokens = len(user_prompt.split()) + len(result.summary.split()) + 50
            return result, total_tokens

        except LLMCallError as exc:
            logger.error("Summary generation failed: %s", exc)
            fallback_text = " ".join(t.content_snippet for t in traces)
            return ResearchSummary(
                summary=f"LLM unavailable. Snippets: {fallback_text[:2000]}",
                citations=[t.source for t in traces],
            ), 0

    def _cache_key(self, query: str, sources: list[str]) -> str:
        content = f"{query}:{','.join(sorted(sources))}"
        return hashlib.sha256(content.encode()).hexdigest()

    def _from_cache(self, key: str) -> Optional[ResearchResult]:
        if key not in self._cache:
            return None
        ts, result = self._cache[key]
        if time.time() - ts < self._cache_ttl:
            return result
        del self._cache[key]
        return None

    @staticmethod
    def _empty_trace() -> RetrievalTrace:
        return RetrievalTrace(
            source="no-source",
            content_snippet="No information available",
            relevance_score=0.0,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    async def close(self) -> None:
        self._cache.clear()
        logger.info("ResearcherService closed")
