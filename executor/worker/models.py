from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class StepPayload:
    id: str
    key: str
    name: str
    type: str
    config: Dict[str, Any]
    depends_on: List[str]


@dataclass
class ExecuteRequest:
    run_id: str
    step: StepPayload


@dataclass
class ExecuteResponse:
    status: str
    error: Optional[str] = None
