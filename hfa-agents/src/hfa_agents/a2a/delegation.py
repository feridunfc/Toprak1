from __future__ import annotations

from hfa_agents.a2a.protocol import A2AMessage


def build_delegation(from_agent: str, to_agent: str, intent: str, payload: dict) -> A2AMessage:
    return A2AMessage(from_agent=from_agent, to_agent=to_agent, intent=intent, payload=payload)
