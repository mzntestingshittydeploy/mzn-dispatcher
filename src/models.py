from typing import List, Dict, Any, Optional

from pydantic import BaseModel


class SolverStatus(BaseModel):
    active: int
    failed: int
    succeeded: int


class Solver(BaseModel):
    image: str
    cpu_request: int
    mem_request: int
    status: Optional[SolverStatus]


class ComputationRequest(BaseModel):
    user_id: str
    timeout_seconds: int
    model_url: str
    data_url: Optional[str]
    solvers: List[Solver]
    solver_options: Optional[str]


class ComputationStatus(BaseModel):
    computation_id: str
    solvers: List[Solver]


class ComputationResult(BaseModel):
    output: Optional[str]


class FinishComputationMessage(BaseModel):
    user_id: str
    computation_id: str
