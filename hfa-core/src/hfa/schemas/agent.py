"""
hfa-core/src/hfa/schemas/agent.py
IRONCLAD — Proper discriminated union with Pydantic V2 TypeAdapter
"""

from enum import Enum
from typing import Annotated, Optional, Union
import logging
import uuid

from pydantic import BaseModel, Field, TypeAdapter
from typing import Literal

logger = logging.getLogger(__name__)


class AgentType(str, Enum):
    # (İstersen Enum kalsın ama discriminator için Literal kullanacağız)
    ARCHITECT = "architect"
    SUPERVISOR = "supervisor"
    RESEARCHER = "researcher"
    DEBUGGER = "debugger"
    PROMPT_ENGINEER = "prompt_engineer"


_ID_PATTERN = r"^[a-zA-Z0-9][a-zA-Z0-9_-]{2,98}[a-zA-Z0-9]$"
_TENANT_PATTERN = r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{2,98}[a-zA-Z0-9]$"


class BaseAgentRequest(BaseModel):
    """Base model for all agent requests with discriminator."""

    agent_type: str

    tenant_id: str = Field(
        ...,
        min_length=3,
        max_length=100,
        pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{2,98}[a-zA-Z0-9]$",
    )
    run_id: str = Field(..., pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_-]{2,98}[a-zA-Z0-9]$")
    trace_id: str = Field(
        default_factory=lambda: f"trace-{uuid.uuid4().hex[:16]}",
        pattern=r"^trace-[a-f0-9]{16}$",
    )


# ----------------------------------------------------------------------
# ARCHITECT SCHEMAS
# ----------------------------------------------------------------------


class ArchitectRequest(BaseAgentRequest):
    """Request for architect agent to create/modify plans."""

    agent_type: Literal["architect"] = "architect"
    plan_id: Optional[str] = Field(None, pattern=_ID_PATTERN)
    requirement: str = Field(..., min_length=10, max_length=10_000)
    context: dict = Field(default_factory=dict)


class PlanManifest(BaseModel):
    """Output of architect agent."""

    plan_id: str = Field(..., pattern=_ID_PATTERN)
    version: str = "1.0.0"
    title: str = Field(..., min_length=3, max_length=200)
    description: str = Field(..., min_length=10, max_length=2000)
    requirements: list[str] = Field(..., min_length=1)
    steps: list[dict] = Field(..., min_length=1)
    estimated_tokens: int = Field(..., ge=1, le=1_000_000)
    created_at: Optional[str] = None  # set by service


# ----------------------------------------------------------------------
# SUPERVISOR SCHEMAS
# ----------------------------------------------------------------------


class SupervisorRequest(BaseAgentRequest):
    """Request for supervisor to inject policy and route."""

    agent_type: Literal["supervisor"] = "supervisor"
    policy_hints: dict = Field(default_factory=dict)
    input_data: dict


class SupervisorDecision(BaseModel):
    """Output of supervisor: where to route and with what policy."""

    target_agent: str
    budget_limit_usd: float = Field(ge=0.0, le=1_000_000.0)
    compliance_action: str = Field(..., pattern="^(allow|hitl|deny)$")
    reasoning: str = Field(..., min_length=10)


# ----------------------------------------------------------------------
# RESEARCHER SCHEMAS
# ----------------------------------------------------------------------


class ResearchRequest(BaseAgentRequest):
    """Request for researcher to gather information."""

    agent_type: Literal["researcher"] = "researcher"
    query: str = Field(..., min_length=5, max_length=1000)
    sources: list[str] = Field(default_factory=list)
    max_results: int = Field(default=5, ge=1, le=20)


class RetrievalTrace(BaseModel):
    """Single trace item from research."""

    source: str = Field(..., min_length=1)
    content_snippet: str = Field(..., max_length=500)
    relevance_score: float = Field(..., ge=0.0, le=1.0)
    timestamp: str  # ISO format


class ResearchResult(BaseModel):
    """Output of researcher."""

    query: str
    summary: str = Field(..., min_length=10, max_length=5000)
    traces: list[RetrievalTrace]
    citations: list[str]
    total_tokens: int = Field(..., ge=0)


# ----------------------------------------------------------------------
# CODER SCHEMAS
# ----------------------------------------------------------------------


class CodeChange(BaseModel):
    """Single file change."""

    file_path: str = Field(..., pattern=r"^[a-zA-Z0-9_/.-]+\.(py|js|ts|go|rs|java)$")
    old_content_hash: Optional[str] = Field(None, pattern=r"^[a-f0-9]{64}$")
    new_content: str
    change_type: str = Field(..., pattern="^(create|modify|delete)$")


class CodeChangeSet(BaseModel):
    """Collection of code changes."""

    change_set_id: str = Field(..., pattern=_ID_PATTERN)
    plan_id: str = Field(..., pattern=_ID_PATTERN)
    changes: list[CodeChange] = Field(..., min_length=1)
    language: str = Field(..., pattern="^(python|javascript|typescript|go|rust|java)$")
    framework: Optional[str] = None
    total_tokens: int = Field(..., ge=0)


# ----------------------------------------------------------------------
# TESTER SCHEMAS
# ----------------------------------------------------------------------


class TestResult(BaseModel):
    """Result of running a test."""

    test_id: str = Field(..., pattern=_ID_PATTERN)
    name: str = Field(..., min_length=1)
    status: str = Field(..., pattern="^(passed|failed|error|skipped)$")
    duration_ms: int = Field(..., ge=0)
    error_message: Optional[str] = Field(None, max_length=1000)
    fingerprint: Optional[str] = Field(None, pattern=r"^[a-f0-9]{64}$")
    coverage_percent: Optional[float] = Field(None, ge=0.0, le=100.0)


class TestSuiteResult(BaseModel):
    """Output of tester."""

    suite_id: str = Field(..., pattern=_ID_PATTERN)
    code_set_id: str = Field(..., pattern=_ID_PATTERN)
    results: list[TestResult] = Field(..., min_length=1)
    summary: dict = Field(default_factory=dict)
    total_tokens: int = Field(..., ge=0)


# ----------------------------------------------------------------------
# DEBUGGER SCHEMAS
# ----------------------------------------------------------------------


class DebuggerRequest(BaseAgentRequest):
    """Request for debugger to fix code."""

    agent_type: Literal["debugger"] = "debugger"
    error_message: str = Field(..., min_length=1, max_length=5000)
    code_set: CodeChangeSet
    test_results: TestSuiteResult


class DebuggerOutput(BaseModel):
    """Output of debugger with fixes."""

    fixed_code_set: CodeChangeSet
    explanation: str = Field(..., min_length=10, max_length=2000)
    confidence: float = Field(..., ge=0.0, le=1.0)


# ----------------------------------------------------------------------
# PROMPT ENGINEER SCHEMAS
# ----------------------------------------------------------------------


class PromptEngineerRequest(BaseAgentRequest):
    """Request to optimize prompts."""

    agent_type: Literal["prompt_engineer"] = "prompt_engineer"
    original_prompt: str = Field(..., min_length=1, max_length=10_000)
    target_model: str = Field(..., min_length=1)
    metrics: Optional[dict] = None


class OptimizedPrompt(BaseModel):
    """Output of prompt engineer."""

    optimized_prompt: str = Field(..., min_length=1, max_length=10_000)
    expected_improvement: str = Field(..., min_length=10)
    token_reduction_percent: float = Field(..., ge=0.0, le=100.0)


# ----------------------------------------------------------------------
# DISCRIMINATED UNION (Pydantic V2 proper way)
# ----------------------------------------------------------------------

AgentRequest = Annotated[
    Union[
        ArchitectRequest,
        SupervisorRequest,
        ResearchRequest,
        DebuggerRequest,
        PromptEngineerRequest,
    ],
    Field(discriminator="agent_type"),
]

_agent_request_adapter = TypeAdapter(AgentRequest)


def parse_agent_request(data: dict) -> AgentRequest:
    """Parse discriminated union using Pydantic V2 TypeAdapter."""
    try:
        return _agent_request_adapter.validate_python(data)
    except Exception as e:
        logger.error(f"Failed to parse agent request: {e}", exc_info=True)
        # fail-closed, sanitized
        raise ValueError("Invalid agent request format")


# ----------------------------------------------------------------------
# PYDANTIC REBUILD (For Discriminated Union Type Resolution)
# ----------------------------------------------------------------------
ArchitectRequest.model_rebuild()
SupervisorRequest.model_rebuild()
ResearchRequest.model_rebuild()
DebuggerRequest.model_rebuild()
PromptEngineerRequest.model_rebuild()
