from __future__ import annotations

import json
from datetime import datetime, timezone

from service_contract.application.ports import (
    ContractEventRepositoryPort,
    ContractStateRepositoryPort,
    InvoiceRepositoryPort,
    PlanMasterRepositoryPort,
    UsageRepositoryPort,
)
from service_contract.domain.models import (
    ActorType,
    ContractEvent,
    ContractLifecycleState,
    ContractState,
    EventType,
    Invoice,
    ModelScope,
    Plan,
    PlanCode,
    UsageDaily,
)


class DuckDbPlanRepository(PlanMasterRepositoryPort):
    def __init__(self, conn) -> None:
        self._conn = conn

    def get_plan(self, plan_code: str) -> Plan | None:
        row = self._conn.execute(
            """
            SELECT plan_code, plan_name, monthly_base_fee_yen, token_fee_per_million_yen,
                   daily_token_limit, available_model_scope, approval_required, is_active
            FROM plan_master
            WHERE plan_code = ?
            """,
            [str(plan_code)],
        ).fetchone()
        if not row:
            return None
        return Plan(
            plan_code=PlanCode(row[0]),
            plan_name=row[1],
            monthly_base_fee_yen=row[2],
            token_fee_per_million_yen=row[3],
            daily_token_limit=row[4],
            available_model_scope=ModelScope(row[5]),
            approval_required=row[6],
            is_active=row[7],
        )

    def seed_defaults(self) -> None:
        now = datetime.now(timezone.utc)
        plans = [
            ("TRIAL", "Trial", 0, 100, 10000, "MINI_ONLY", False, True, now, now),
            ("STARTER", "Starter", 20000, 1000, 500000, "ALL", True, True, now, now),
            ("PRO", "Pro", 30000, 1000, 1000000, "ALL", True, True, now, now),
        ]
        for plan in plans:
            self._conn.execute(
                """
                INSERT INTO plan_master
                  (plan_code, plan_name, monthly_base_fee_yen, token_fee_per_million_yen,
                   daily_token_limit, available_model_scope, approval_required, is_active,
                   created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(plan_code) DO UPDATE SET
                    plan_name=excluded.plan_name,
                    monthly_base_fee_yen=excluded.monthly_base_fee_yen,
                    token_fee_per_million_yen=excluded.token_fee_per_million_yen,
                    daily_token_limit=excluded.daily_token_limit,
                    available_model_scope=excluded.available_model_scope,
                    approval_required=excluded.approval_required,
                    is_active=excluded.is_active,
                    updated_at=excluded.updated_at
                """,
                plan,
            )


class DuckDbEventRepository(ContractEventRepositoryPort):
    def __init__(self, conn) -> None:
        self._conn = conn

    def append(self, event: ContractEvent) -> None:
        self._conn.execute(
            """
            INSERT INTO contract_event
              (event_id, contract_id, event_type, occurred_at, recorded_at,
               actor_type, actor_id, payload_json, correlation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event.event_id,
                event.contract_id,
                str(event.event_type),
                event.occurred_at,
                event.recorded_at,
                str(event.actor_type),
                event.actor_id,
                json.dumps(event.payload, ensure_ascii=True),
                event.correlation_id,
            ],
        )

    def list_by_contract(self, contract_id: str) -> list[ContractEvent]:
        rows = self._conn.execute(
            """
            SELECT event_id, contract_id, event_type, occurred_at, recorded_at,
                   actor_type, actor_id, payload_json, correlation_id
            FROM contract_event
            WHERE contract_id = ?
            ORDER BY recorded_at, event_id
            """,
            [contract_id],
        ).fetchall()
        return [
            ContractEvent(
                event_id=row[0],
                contract_id=row[1],
                event_type=EventType(row[2]),
                occurred_at=row[3],
                recorded_at=row[4],
                actor_type=ActorType(row[5]),
                actor_id=row[6],
                payload=json.loads(row[7]),
                correlation_id=row[8],
            )
            for row in rows
        ]


class DuckDbStateRepository(ContractStateRepositoryPort):
    def __init__(self, conn) -> None:
        self._conn = conn

    def get(self, contract_id: str) -> ContractState | None:
        row = self._conn.execute(
            """
            SELECT contract_id, contract_lifecycle_state, current_plan_code,
                   budget_number, budget_owner_name, api_key_name, team_name,
                   primary_contact_name, secondary_contact_name, department_name,
                   api_key_id, service_started_at, service_ended_at,
                   last_event_id, version_no, created_at, updated_at
            FROM contract_state
            WHERE contract_id = ?
            """,
            [contract_id],
        ).fetchone()
        if not row:
            return None
        return _row_to_state(row)

    def upsert(self, state: ContractState) -> None:
        self._conn.execute(
            """
            INSERT INTO contract_state
              (contract_id, contract_lifecycle_state, current_plan_code,
               budget_number, budget_owner_name, api_key_name, team_name,
               primary_contact_name, secondary_contact_name, department_name,
               api_key_id, service_started_at, service_ended_at,
               last_event_id, version_no, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(contract_id) DO UPDATE SET
              contract_lifecycle_state=excluded.contract_lifecycle_state,
              current_plan_code=excluded.current_plan_code,
              budget_number=excluded.budget_number,
              budget_owner_name=excluded.budget_owner_name,
              api_key_name=excluded.api_key_name,
              team_name=excluded.team_name,
              primary_contact_name=excluded.primary_contact_name,
              secondary_contact_name=excluded.secondary_contact_name,
              department_name=excluded.department_name,
              api_key_id=excluded.api_key_id,
              service_started_at=excluded.service_started_at,
              service_ended_at=excluded.service_ended_at,
              last_event_id=excluded.last_event_id,
              version_no=excluded.version_no,
              updated_at=excluded.updated_at
            """,
            [
                state.contract_id,
                str(state.contract_lifecycle_state),
                str(state.current_plan_code),
                state.budget_number,
                state.budget_owner_name,
                state.api_key_name,
                state.team_name,
                state.primary_contact_name,
                state.secondary_contact_name,
                state.department_name,
                state.api_key_id,
                state.service_started_at,
                state.service_ended_at,
                state.last_event_id,
                state.version_no,
                state.created_at,
                state.updated_at,
            ],
        )

    def list_all(self) -> list[ContractState]:
        rows = self._conn.execute(
            """
            SELECT contract_id, contract_lifecycle_state, current_plan_code,
                   budget_number, budget_owner_name, api_key_name, team_name,
                   primary_contact_name, secondary_contact_name, department_name,
                   api_key_id, service_started_at, service_ended_at,
                   last_event_id, version_no, created_at, updated_at
            FROM contract_state
            ORDER BY created_at, contract_id
            """
        ).fetchall()
        return [_row_to_state(row) for row in rows]


class DuckDbUsageRepository(UsageRepositoryPort):
    def __init__(self, conn) -> None:
        self._conn = conn

    def upsert(self, usage: UsageDaily) -> None:
        self._conn.execute(
            """
            INSERT INTO usage_daily
              (usage_date, contract_id, model_name, used_tokens, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(contract_id, usage_date, model_name)
            DO UPDATE SET
              used_tokens=excluded.used_tokens,
              created_at=excluded.created_at
            """,
            [
                usage.usage_date,
                usage.contract_id,
                usage.model_name,
                usage.used_tokens,
                usage.created_at,
            ],
        )

    def sum_tokens_for_month(self, contract_id: str, year_month: str) -> int:
        row = self._conn.execute(
            """
            SELECT COALESCE(SUM(used_tokens), 0)
            FROM usage_daily
            WHERE contract_id = ?
              AND strftime(usage_date, '%Y-%m') = ?
            """,
            [contract_id, year_month],
        ).fetchone()
        return int(row[0]) if row else 0


class DuckDbInvoiceRepository(InvoiceRepositoryPort):
    def __init__(self, conn) -> None:
        self._conn = conn

    def create(self, invoice: Invoice) -> None:
        self._conn.execute(
            """
            INSERT INTO invoice
              (invoice_id, budget_number, title, description,
               target_year_month, total_amount_yen, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                invoice.invoice_id,
                invoice.budget_number,
                invoice.title,
                invoice.description,
                invoice.target_year_month,
                invoice.total_amount_yen,
                invoice.created_at,
            ],
        )

    def exists(self, budget_number: str, target_year_month: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1
            FROM invoice
            WHERE budget_number = ?
              AND target_year_month = ?
            LIMIT 1
            """,
            [budget_number, target_year_month],
        ).fetchone()
        return row is not None

    def list_by_month(self, target_year_month: str) -> list[Invoice]:
        rows = self._conn.execute(
            """
            SELECT invoice_id, budget_number, title, description,
                   target_year_month, total_amount_yen, created_at
            FROM invoice
            WHERE target_year_month = ?
            ORDER BY created_at, invoice_id
            """,
            [target_year_month],
        ).fetchall()
        return [
            Invoice(
                invoice_id=row[0],
                budget_number=row[1],
                title=row[2],
                description=row[3],
                target_year_month=row[4],
                total_amount_yen=row[5],
                created_at=row[6],
            )
            for row in rows
        ]


def _row_to_state(row) -> ContractState:
    return ContractState(
        contract_id=row[0],
        contract_lifecycle_state=ContractLifecycleState(row[1]),
        current_plan_code=PlanCode(row[2]),
        budget_number=row[3],
        budget_owner_name=row[4],
        api_key_name=row[5],
        team_name=row[6],
        primary_contact_name=row[7],
        secondary_contact_name=row[8],
        department_name=row[9],
        api_key_id=row[10],
        service_started_at=row[11],
        service_ended_at=row[12],
        last_event_id=row[13],
        version_no=row[14],
        created_at=row[15],
        updated_at=row[16],
    )
