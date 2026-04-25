from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Any

from service_contract.application.ports import (
    ApiKeyIssuerPort,
    ClockPort,
    ContractEventRepositoryPort,
    ContractStateRepositoryPort,
    IdGeneratorPort,
    InvoiceRepositoryPort,
    PlanMasterRepositoryPort,
    UsageRepositoryPort,
)
from service_contract.domain.models import (
    ActorType,
    ContractEvent,
    ContractLifecycleState,
    EventType,
    Invoice,
    UsageDaily,
)
from service_contract.domain.projector import DomainRuleError, apply_event


class ApplicationError(Exception):
    pass


class ContractService:
    def __init__(
        self,
        plan_repo: PlanMasterRepositoryPort,
        event_repo: ContractEventRepositoryPort,
        state_repo: ContractStateRepositoryPort,
        usage_repo: UsageRepositoryPort,
        invoice_repo: InvoiceRepositoryPort,
        api_key_issuer: ApiKeyIssuerPort,
        clock: ClockPort,
        id_generator: IdGeneratorPort,
    ) -> None:
        self._plan_repo = plan_repo
        self._event_repo = event_repo
        self._state_repo = state_repo
        self._usage_repo = usage_repo
        self._invoice_repo = invoice_repo
        self._api_key_issuer = api_key_issuer
        self._clock = clock
        self._id_gen = id_generator

    def apply_new(
        self,
        requested_plan_code: str,
        budget_number: str,
        budget_owner_name: str,
        api_key_name: str,
        team_name: str,
        primary_contact_name: str,
        secondary_contact_name: str,
        department_name: str,
        actor_id: str | None,
    ) -> str:
        plan = self._plan_repo.get_plan(requested_plan_code)
        if not plan or not plan.is_active:
            raise ApplicationError(f"invalid or inactive plan: {requested_plan_code}")

        contract_id = self._id_gen.new_id()
        submitted = self._new_event(
            contract_id=contract_id,
            event_type=EventType.APPLICATION_SUBMITTED,
            actor_type=ActorType.USER,
            actor_id=actor_id,
            payload={
                "requested_plan_code": requested_plan_code,
                "budget_number": budget_number,
                "budget_owner_name": budget_owner_name,
                "api_key_name": api_key_name,
                "team_name": team_name,
                "primary_contact_name": primary_contact_name,
                "secondary_contact_name": secondary_contact_name,
                "department_name": department_name,
            },
        )
        self._append_and_project(submitted)

        if not plan.approval_required:
            auto_approved = self._new_event(
                contract_id=contract_id,
                event_type=EventType.APPLICATION_AUTO_APPROVED,
                actor_type=ActorType.SYSTEM,
                actor_id="system",
                payload={"reason": "trial auto approved"},
            )
            self._append_and_project(auto_approved)

        return contract_id

    def approve_application(self, contract_id: str, actor_id: str | None) -> str:
        state = self._must_get_state(contract_id)
        if state.contract_lifecycle_state != ContractLifecycleState.APPLYING:
            raise ApplicationError("only applying contracts can be approved")
        event = self._new_event(
            contract_id=contract_id,
            event_type=EventType.APPLICATION_APPROVED,
            actor_type=ActorType.ADMIN,
            actor_id=actor_id,
            payload={},
        )
        self._append_and_project(event)
        return event.event_id

    def reject_application(self, contract_id: str, reason: str, actor_id: str | None) -> str:
        state = self._must_get_state(contract_id)
        if state.contract_lifecycle_state != ContractLifecycleState.APPLYING:
            raise ApplicationError("only applying contracts can be rejected")
        event = self._new_event(
            contract_id=contract_id,
            event_type=EventType.APPLICATION_REJECTED,
            actor_type=ActorType.ADMIN,
            actor_id=actor_id,
            payload={"reason": reason},
        )
        self._append_and_project(event)
        return event.event_id

    def issue_api_key(self, contract_id: str, actor_id: str | None) -> str:
        state = self._must_get_state(contract_id)
        plan = self._plan_repo.get_plan(state.current_plan_code)
        if not plan:
            raise ApplicationError("plan not found")

        if plan.approval_required and not self._is_approved(contract_id):
            raise ApplicationError("approval event is required before api key issuance")

        api_key_id = self._api_key_issuer.issue(contract_id=contract_id, api_key_name=state.api_key_name)
        event = self._new_event(
            contract_id=contract_id,
            event_type=EventType.API_KEY_ISSUED,
            actor_type=ActorType.ADMIN,
            actor_id=actor_id,
            payload={
                "api_key_id": api_key_id,
                "issued_at": self._clock.now().isoformat(),
            },
        )
        self._append_and_project(event)
        return api_key_id

    def change_budget_number(
        self,
        contract_id: str,
        to_budget_number: str,
        to_budget_owner_name: str,
        actor_id: str | None,
    ) -> str:
        state = self._must_get_state(contract_id)
        event = self._new_event(
            contract_id=contract_id,
            event_type=EventType.BUDGET_NUMBER_CHANGED,
            actor_type=ActorType.USER,
            actor_id=actor_id,
            payload={
                "from_budget_number": state.budget_number,
                "to_budget_number": to_budget_number,
                "from_budget_owner_name": state.budget_owner_name,
                "to_budget_owner_name": to_budget_owner_name,
                "effective_at": self._clock.now().isoformat(),
            },
        )
        self._append_and_project(event)
        return event.event_id

    def change_plan(self, contract_id: str, to_plan_code: str, actor_id: str | None) -> str:
        state = self._must_get_state(contract_id)
        plan = self._plan_repo.get_plan(to_plan_code)
        if not plan or not plan.is_active:
            raise ApplicationError(f"invalid or inactive plan: {to_plan_code}")
        event = self._new_event(
            contract_id=contract_id,
            event_type=EventType.PLAN_CHANGED,
            actor_type=ActorType.USER,
            actor_id=actor_id,
            payload={
                "from_plan_code": str(state.current_plan_code),
                "to_plan_code": to_plan_code,
                "effective_at": self._clock.now().isoformat(),
            },
        )
        self._append_and_project(event)
        return event.event_id

    def request_cancellation(self, contract_id: str, reason: str, actor_id: str | None) -> str:
        self._must_get_state(contract_id)
        event = self._new_event(
            contract_id=contract_id,
            event_type=EventType.CANCELLATION_REQUESTED,
            actor_type=ActorType.USER,
            actor_id=actor_id,
            payload={"reason": reason},
        )
        self._append_and_project(event)
        return event.event_id

    def terminate_contract(self, contract_id: str, reason: str, actor_id: str | None) -> str:
        self._must_get_state(contract_id)
        event = self._new_event(
            contract_id=contract_id,
            event_type=EventType.CONTRACT_TERMINATED,
            actor_type=ActorType.ADMIN,
            actor_id=actor_id,
            payload={
                "terminated_at": self._clock.now().isoformat(),
                "reason": reason,
            },
        )
        self._append_and_project(event)
        return event.event_id

    def record_usage_daily(
        self,
        contract_id: str,
        usage_date: date,
        model_name: str,
        used_tokens: int,
    ) -> None:
        self._must_get_state(contract_id)
        if used_tokens < 0:
            raise ApplicationError("used_tokens must be >= 0")
        self._usage_repo.upsert(
            UsageDaily(
                usage_date=usage_date,
                contract_id=contract_id,
                model_name=model_name,
                used_tokens=used_tokens,
                created_at=self._clock.now(),
            )
        )

    def create_monthly_invoices(self, target_year_month: str) -> list[Invoice]:
        first_day, last_day = _month_range(target_year_month)
        grouped: dict[str, dict[str, Any]] = {}
        created_at = self._clock.now()
        invoices: list[Invoice] = []

        for state in self._state_repo.list_all():
            if not _was_active_in_month(state.service_started_at, state.service_ended_at, first_day, last_day):
                continue
            plan = self._plan_repo.get_plan(state.current_plan_code)
            if not plan:
                raise ApplicationError(f"plan not found: {state.current_plan_code}")

            token_sum = self._usage_repo.sum_tokens_for_month(state.contract_id, target_year_month)
            token_fee = int((token_sum / 1_000_000) * plan.token_fee_per_million_yen)
            total = plan.monthly_base_fee_yen + token_fee

            bucket = grouped.setdefault(
                state.budget_number,
                {"total": 0, "lines": []},
            )
            bucket["total"] += total
            bucket["lines"].append(f"{state.current_plan_code}プラン {state.team_name} {target_year_month}月分")

        for budget_number, bucket in grouped.items():
            if self._invoice_repo.exists(budget_number, target_year_month):
                continue
            invoice = Invoice(
                invoice_id=self._id_gen.new_id(),
                budget_number=budget_number,
                title=f"{target_year_month} 利用料",
                description="; ".join(bucket["lines"]),
                target_year_month=target_year_month,
                total_amount_yen=int(bucket["total"]),
                created_at=created_at,
            )
            self._invoice_repo.create(invoice)
            invoices.append(invoice)

        return invoices

    def get_contract(self, contract_id: str) -> dict[str, Any]:
        state = self._must_get_state(contract_id)
        return asdict(state)

    def list_contracts(self) -> list[dict[str, Any]]:
        return [asdict(s) for s in self._state_repo.list_all()]

    def list_events(self, contract_id: str) -> list[dict[str, Any]]:
        return [asdict(e) for e in self._event_repo.list_by_contract(contract_id)]

    def list_invoices(self, target_year_month: str) -> list[dict[str, Any]]:
        return [asdict(i) for i in self._invoice_repo.list_by_month(target_year_month)]

    def seed_plan_master(self) -> None:
        self._plan_repo.seed_defaults()

    def _is_approved(self, contract_id: str) -> bool:
        events = self._event_repo.list_by_contract(contract_id)
        approved_types = {
            EventType.APPLICATION_APPROVED,
            EventType.APPLICATION_AUTO_APPROVED,
        }
        return any(e.event_type in approved_types for e in events)

    def _append_and_project(self, event: ContractEvent) -> None:
        current = self._state_repo.get(event.contract_id)
        try:
            next_state = apply_event(current, event)
        except DomainRuleError as err:
            raise ApplicationError(str(err)) from err

        self._event_repo.append(event)
        self._state_repo.upsert(next_state)

    def _must_get_state(self, contract_id: str):
        state = self._state_repo.get(contract_id)
        if not state:
            raise ApplicationError(f"contract not found: {contract_id}")
        return state

    def _new_event(
        self,
        contract_id: str,
        event_type: EventType,
        actor_type: ActorType,
        actor_id: str | None,
        payload: dict[str, Any],
    ) -> ContractEvent:
        now = self._clock.now()
        return ContractEvent(
            event_id=self._id_gen.new_id(),
            contract_id=contract_id,
            event_type=event_type,
            occurred_at=now,
            recorded_at=now,
            actor_type=actor_type,
            actor_id=actor_id,
            payload=payload,
            correlation_id=None,
        )


class SystemClock(ClockPort):
    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def today(self) -> date:
        return self.now().date()


class UuidGenerator(IdGeneratorPort):
    def new_id(self) -> str:
        import uuid

        return str(uuid.uuid4())


def _month_range(year_month: str) -> tuple[date, date]:
    year, month = [int(x) for x in year_month.split("-")]
    first_day = date(year, month, 1)
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last_day = date.fromordinal(next_month.toordinal() - 1)
    return first_day, last_day


def _was_active_in_month(
    service_started_at: datetime | None,
    service_ended_at: datetime | None,
    month_first: date,
    month_last: date,
) -> bool:
    if service_started_at is None:
        return False
    started = service_started_at.date()
    ended = service_ended_at.date() if service_ended_at else None
    if started > month_last:
        return False
    if ended is not None and ended < month_first:
        return False
    return True
