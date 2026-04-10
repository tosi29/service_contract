from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from enum import StrEnum
from typing import Any


class PlanCode(StrEnum):
    TRIAL = "TRIAL"
    STARTER = "STARTER"
    PRO = "PRO"


class ModelScope(StrEnum):
    MINI_ONLY = "MINI_ONLY"
    ALL = "ALL"


class ActorType(StrEnum):
    USER = "USER"
    ADMIN = "ADMIN"
    SYSTEM = "SYSTEM"
    BATCH = "BATCH"


class EventType(StrEnum):
    APPLICATION_SUBMITTED = "APPLICATION_SUBMITTED"
    APPLICATION_APPROVED = "APPLICATION_APPROVED"
    APPLICATION_REJECTED = "APPLICATION_REJECTED"
    APPLICATION_AUTO_APPROVED = "APPLICATION_AUTO_APPROVED"
    API_KEY_ISSUED = "API_KEY_ISSUED"
    BUDGET_NUMBER_CHANGED = "BUDGET_NUMBER_CHANGED"
    PLAN_CHANGED = "PLAN_CHANGED"
    CANCELLATION_REQUESTED = "CANCELLATION_REQUESTED"
    CONTRACT_TERMINATED = "CONTRACT_TERMINATED"


class ContractLifecycleState(StrEnum):
    APPLYING = "APPLYING"
    ACTIVE = "ACTIVE"
    TERMINATED = "TERMINATED"


@dataclass(frozen=True)
class Plan:
    plan_code: PlanCode
    plan_name: str
    monthly_base_fee_yen: int
    token_fee_per_million_yen: int
    daily_token_limit: int
    available_model_scope: ModelScope
    approval_required: bool
    is_active: bool


@dataclass(frozen=True)
class ContractEvent:
    event_id: str
    contract_id: str
    event_type: EventType
    occurred_at: datetime
    recorded_at: datetime
    actor_type: ActorType
    actor_id: str | None
    payload: dict[str, Any]
    correlation_id: str | None


@dataclass
class ContractState:
    contract_id: str
    contract_lifecycle_state: ContractLifecycleState
    current_plan_code: PlanCode
    budget_number: str
    budget_owner_name: str
    api_key_name: str
    team_name: str
    primary_contact_name: str
    secondary_contact_name: str
    department_name: str
    api_key_id: str | None
    service_started_at: datetime | None
    service_ended_at: datetime | None
    last_event_id: str
    version_no: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class UsageDaily:
    usage_date: date
    contract_id: str
    model_name: str
    used_tokens: int
    created_at: datetime


@dataclass(frozen=True)
class Invoice:
    invoice_id: str
    budget_number: str
    title: str
    description: str
    target_year_month: str
    total_amount_yen: int
    created_at: datetime
