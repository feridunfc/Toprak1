from __future__ import annotations

from typing import Dict, Type

from hfa_agents.base.agent_base import AgentBase


class AgentRegistry:
    _agents: Dict[str, Type[AgentBase]] = {}

    @classmethod
    def register(cls, name: str):
        def decorator(agent_class: Type[AgentBase]):
            cls._agents[name] = agent_class
            return agent_class
        return decorator

    @classmethod
    def get(cls, name: str) -> Type[AgentBase]:
        if name not in cls._agents:
            raise KeyError(f"Agent '{name}' is not registered")
        return cls._agents[name]

    @classmethod
    def list_agents(cls) -> list[str]:
        return sorted(cls._agents.keys())
