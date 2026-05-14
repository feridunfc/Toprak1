"""
hfa-core/src/hfa/schemas/research.py
IRONCLAD Sprint 3 — Research schemas (split from agent.py for clarity).
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class RetrievalTrace(BaseModel):
    """Single retrieved source with relevance score."""

    source: str = Field(..., min_length=1)
    content_snippet: str = Field(..., max_length=500)
    relevance_score: float = Field(..., ge=0.0, le=1.0)
    timestamp: str  # ISO-8601


class ResearchSummary(BaseModel):
    """
    Structured LLM output for research summaries.
    Used by ResearcherService.generate_structured() — response_model must be BaseModel.
    """

    summary: str = Field(..., min_length=10, max_length=5000)
    citations: list[str] = Field(default_factory=list)


class ResearchResult(BaseModel):
    """Full output of the researcher agent."""

    query: str
    summary: str = Field(..., min_length=10, max_length=5000)
    traces: list[RetrievalTrace]
    citations: list[str]
    total_tokens: int = Field(..., ge=0)
