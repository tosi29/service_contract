from __future__ import annotations

from datetime import date, datetime
from typing import Protocol

from service_contract.domain.models import ContractEvent, ContractState, Invoice, Plan, UsageDaily


class PlanMasterRepositoryPort(Protocol):
    def get_plan(self, plan_code: str) -> Plan | None: ...

    def seed_defaults(self) -> None: ...


class ContractEventRepositoryPort(Protocol):
    def append(self, event: ContractEvent) -> None: ...

    def list_by_contract(self, contract_id: str) -> list[ContractEvent]: ...


class ContractStateRepositoryPort(Protocol):
    def get(self, contract_id: str) -> ContractState | None: ...

    def upsert(self, state: ContractState) -> None: ...

    def list_all(self) -> list[ContractState]: ...


class UsageRepositoryPort(Protocol):
    def upsert(self, usage: UsageDaily) -> None: ...

    def sum_tokens_for_month(self, contract_id: str, year_month: str) -> int: ...


class InvoiceRepositoryPort(Protocol):
    def create(self, invoice: Invoice) -> None: ...

    def exists(self, budget_number: str, target_year_month: str) -> bool: ...

    def list_by_month(self, target_year_month: str) -> list[Invoice]: ...


class ApiKeyIssuerPort(Protocol):
    def issue(self, contract_id: str, api_key_name: str) -> str: ...


class ClockPort(Protocol):
    def now(self) -> datetime: ...

    def today(self) -> date: ...


class IdGeneratorPort(Protocol):
    def new_id(self) -> str: ...
