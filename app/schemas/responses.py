"""Pydantic models for response bodies."""

from __future__ import annotations

import uuid
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Generic
# ---------------------------------------------------------------------------

class StatusResponse(BaseModel):
    status: str


class ErrorResponse(BaseModel):
    """Standard error response returned by all error handlers."""

    status: str = Field(default="failure", examples=["failure"])
    message: str = Field(..., examples=["Descriptive error message."])


# ---------------------------------------------------------------------------
# Processing request
# ---------------------------------------------------------------------------

class RequestCreatedResponse(BaseModel):
    status: str
    request_id: uuid.UUID


# ---------------------------------------------------------------------------
# Request status
# ---------------------------------------------------------------------------

class PageStatusItem(BaseModel):
    state: str
    quality: Optional[float] = None


class RequestStatusResponse(BaseModel):
    status: str
    request_status: Dict[str, PageStatusItem]


# ---------------------------------------------------------------------------
# Processing task (worker)
# ---------------------------------------------------------------------------

class ProcessingTaskResponse(BaseModel):
    status: str
    page_id: uuid.UUID
    page_url: str
    engine_id: int


# ---------------------------------------------------------------------------
# Engines
# ---------------------------------------------------------------------------

class ModelInfo(BaseModel):
    id: int
    name: str


class EngineInfo(BaseModel):
    id: int
    description: Optional[str] = None
    engine_version: str
    models: List[ModelInfo]
    cost_per_page: float = 0.0


class EngineListResponse(BaseModel):
    status: str
    engines: Dict[str, EngineInfo]


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

class UsageStatisticsResponse(BaseModel):
    status: str
    processed_pages: int
    credit_balance: Optional[float] = None
    pending_cost: Optional[float] = None
    # Optional date filters echoed back
    from_date: Optional[str] = None
    to_date: Optional[str] = None


class PageStatisticsResponse(BaseModel):
    status: str
    state_stats: Dict[str, int]


# ---------------------------------------------------------------------------
# Engine version
# ---------------------------------------------------------------------------

class LatestEngineVersionResponse(BaseModel):
    status: str
    filename: str


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

class ApiKeyItem(BaseModel):
    id: int
    api_string: str
    owner: str
    permission: str
    suspension: bool
    credit_balance: float = 0.0
    pending_cost: float = 0.0


class CreateUserResponse(BaseModel):
    status: str
    api_key: str
    owner: str
    permission: str


class UserListResponse(BaseModel):
    status: str
    users: List[ApiKeyItem]


class SuspendUserResponse(BaseModel):
    status: str
    user_id: int
    suspended: bool


class UserUsageItem(BaseModel):
    api_key_id: int
    owner: str
    api_string: str
    processed_pages: int
    credit_balance: float = 0.0
    pending_cost: float = 0.0


class AllUsersUsageStatisticsResponse(BaseModel):
    status: str
    users: List[UserUsageItem]
    from_date: Optional[str] = None
    to_date: Optional[str] = None


class EngineUsageItem(BaseModel):
    engine_id: int
    engine_name: str
    processed_pages: int


class EngineUsageStatisticsResponse(BaseModel):
    status: str
    engines: List[EngineUsageItem]
    from_date: Optional[str] = None
    to_date: Optional[str] = None


# ---------------------------------------------------------------------------
# Credits
# ---------------------------------------------------------------------------

class AddCreditsResponse(BaseModel):
    status: str
    user_id: int
    new_balance: float
    amount: float
    note: Optional[str] = None


class CreditTransactionItem(BaseModel):
    id: int
    amount: float
    timestamp: str
    admin_owner: Optional[str] = None
    note: Optional[str] = None


class CreditHistoryResponse(BaseModel):
    status: str
    user_id: int
    transactions: List[CreditTransactionItem]


class SetEngineCostResponse(BaseModel):
    status: str
    engine_id: int
    cost_per_page: float
