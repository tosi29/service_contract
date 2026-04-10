from __future__ import annotations

from datetime import date

from service_contract.adapters.duckdb.db import DuckDbConnectionProvider, init_db_schema
from service_contract.adapters.duckdb.repositories import (
    DuckDbEventRepository,
    DuckDbInvoiceRepository,
    DuckDbPlanRepository,
    DuckDbStateRepository,
    DuckDbUsageRepository,
)
from service_contract.adapters.internal.api_key_issuer import InternalApiKeyIssuer
from service_contract.application.service import ContractService, SystemClock, UuidGenerator
from service_contract.domain.models import ContractLifecycleState


def _service(db_path: str) -> tuple[ContractService, object]:
    conn = DuckDbConnectionProvider(db_path).connect()
    init_db_schema(conn)
    service = ContractService(
        plan_repo=DuckDbPlanRepository(conn),
        event_repo=DuckDbEventRepository(conn),
        state_repo=DuckDbStateRepository(conn),
        usage_repo=DuckDbUsageRepository(conn),
        invoice_repo=DuckDbInvoiceRepository(conn),
        api_key_issuer=InternalApiKeyIssuer(),
        clock=SystemClock(),
        id_generator=UuidGenerator(),
    )
    service.seed_plan_master()
    return service, conn


def test_trial_auto_approved_then_issue_api_key(tmp_path):
    db_path = str(tmp_path / "svc.duckdb")
    service, conn = _service(db_path)

    contract_id = service.apply_new(
        requested_plan_code="TRIAL",
        budget_number="B-100",
        budget_owner_name="Owner",
        api_key_name="trial-key",
        team_name="Team A",
        primary_contact_name="P",
        secondary_contact_name="S",
        department_name="Dept",
        actor_id="user-1",
    )

    api_key_id = service.issue_api_key(contract_id, actor_id="admin-1")
    state = service.get_contract(contract_id)

    assert api_key_id.startswith("key_")
    assert state["contract_lifecycle_state"] == ContractLifecycleState.ACTIVE
    assert state["api_key_id"] == api_key_id
    conn.close()


def test_monthly_invoice_grouped_by_budget_number(tmp_path):
    db_path = str(tmp_path / "svc.duckdb")
    service, conn = _service(db_path)

    c1 = service.apply_new(
        requested_plan_code="STARTER",
        budget_number="B-200",
        budget_owner_name="Owner",
        api_key_name="k1",
        team_name="Team1",
        primary_contact_name="P1",
        secondary_contact_name="S1",
        department_name="D1",
        actor_id="u1",
    )
    c2 = service.apply_new(
        requested_plan_code="PRO",
        budget_number="B-200",
        budget_owner_name="Owner",
        api_key_name="k2",
        team_name="Team2",
        primary_contact_name="P2",
        secondary_contact_name="S2",
        department_name="D2",
        actor_id="u2",
    )

    service.approve_application(c1, "admin")
    service.approve_application(c2, "admin")
    service.issue_api_key(c1, "admin")
    service.issue_api_key(c2, "admin")

    today = SystemClock().today()
    target_year_month = today.strftime("%Y-%m")
    usage_day = date(today.year, today.month, 10 if today.day >= 10 else today.day)
    service.record_usage_daily(c1, usage_day, "mini", 500_000)
    service.record_usage_daily(c2, usage_day, "gpt-4.1", 1_000_000)

    invoices = service.create_monthly_invoices(target_year_month)

    assert len(invoices) == 1
    assert invoices[0].budget_number == "B-200"
    assert invoices[0].total_amount_yen == 51500
    conn.close()
