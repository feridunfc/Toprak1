import pytest

from hfa_agents.workflow.orchestrator import AgentOrchestrator
from hfa_agents.roles.supervisor import SupervisorAgent  # noqa: F401
from hfa_agents.roles.researcher import ResearcherAgent  # noqa: F401
from hfa_agents.roles.architect import ArchitectAgent  # noqa: F401
from hfa_agents.roles.coder import CoderAgent  # noqa: F401
from hfa_agents.roles.tester import TesterAgent  # noqa: F401
from hfa_agents.roles.compliance import ComplianceAgent  # noqa: F401


class _DummyMerge:
    async def execute_query(self, req):
        return []


class _DummySemantic:
    def __init__(self):
        self.merge_engine = _DummyMerge()

    async def process_event(self, raw):
        return True, {"semantic_matches": []}


@pytest.mark.asyncio
async def test_orchestrator_smoke():
    orchestrator = AgentOrchestrator(_DummySemantic())
    result = await orchestrator.run_workflow({"event_id": "e1", "goal": "build a tiny api"})
    assert result.status == "success"
