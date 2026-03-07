from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field

from app.db.models import JobStatus, JobType, UserRole


class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    role: UserRole = UserRole.researcher


class UserRead(BaseModel):
    id: UUID
    email: EmailStr
    role: UserRole
    created_at: datetime


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class IngestJobRequest(BaseModel):
    file_name: str | None = None
    symbol_contract: str | None = None
    scan_watch_dir: bool = True
    rebuild: bool = False


class LargeOrdersBackfillJobRequest(BaseModel):
    symbols: list[str] = Field(min_length=1)
    threshold: float = Field(default=20.0, gt=0.0)


class JobRead(BaseModel):
    id: UUID
    job_type: JobType
    status: JobStatus
    payload: dict[str, Any]
    result: dict[str, Any]
    error: dict[str, Any]
    rq_job_id: str | None
    created_at: datetime
    updated_at: datetime


class BacktestJobRequest(BaseModel):
    mode: str = Field(default="run", pattern="^(run|sweep)$")
    name: str = "Scaffold Backtest"
    strategy_id: str = "scaffold"
    params: dict[str, Any] = Field(default_factory=dict)


class BacktestRunRead(BaseModel):
    id: UUID
    name: str
    strategy_id: str
    params: dict[str, Any]
    metrics: dict[str, Any]
    status: str
    created_at: datetime
