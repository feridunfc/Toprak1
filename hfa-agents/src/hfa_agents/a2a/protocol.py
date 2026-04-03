from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Any, Dict


class A2AMessage(BaseModel):
    from_agent: str
    to_agent: str
    intent: str
    payload: Dict[str, Any] = Field(default_factory=dict)
