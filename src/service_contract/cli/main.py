from __future__ import annotations

import json
import os
from datetime import date, datetime
from typing import Any

import typer

from service_contract.adapters.duckdb.db import DuckDbConnectionProvider, init_db_schema
from service_contract.adapters.duckdb.repositories import (
    DuckDbEventRepository,
    DuckDbInvoiceRepository,
    DuckDbPlanRepository,
    DuckDbStateRepository,
    DuckDbUsageRepository,
)
from service_contract.adapters.internal.api_key_issuer import InternalApiKeyIssuer
from service_contract.application.service import ApplicationError, ContractService, SystemClock, UuidGenerator

app = typer.Typer(no_args_is_help=True)


def _build_service(db_path: str) -> tuple[ContractService, Any]:
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
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
    return service, conn


def _required_str(value: str | None, label: str, non_interactive: bool) -> str:
    if value is not None and value != "":
        return value
    if non_interactive:
        raise typer.BadParameter(f"{label} is required in --non-interactive mode")
    return typer.prompt(label)


def _required_int(value: int | None, label: str, non_interactive: bool) -> int:
    if value is not None:
        return value
    if non_interactive:
        raise typer.BadParameter(f"{label} is required in --non-interactive mode")
    return typer.prompt(label, type=int)


def _required_date(value: str | None, label: str, non_interactive: bool) -> date:
    raw = _required_str(value, label, non_interactive)
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _output(data: Any, as_json: bool) -> None:
    if as_json:
        typer.echo(json.dumps(data, ensure_ascii=False, indent=2, default=str))
        return
    if isinstance(data, list):
        for row in data:
            typer.echo("-" * 40)
            for k, v in row.items():
                typer.echo(f"{k}: {v}")
        if not data:
            typer.echo("(no data)")
        return
    if isinstance(data, dict):
        for k, v in data.items():
            typer.echo(f"{k}: {v}")
        return
    typer.echo(str(data))


def _run(action):
    try:
        return action()
    except ApplicationError as err:
        raise typer.Exit(code=_echo_err(str(err)))


def _echo_err(msg: str) -> int:
    typer.echo(f"ERROR: {msg}")
    return 1


@app.command("init-db")
def init_db(
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    _, conn = _build_service(db_path)
    conn.close()
    typer.echo(f"initialized: {db_path}")


@app.command("seed-plan-master")
def seed_plan_master(
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    service.seed_plan_master()
    conn.close()
    typer.echo("plan_master seeded")


@app.command("apply-new")
def apply_new(
    requested_plan_code: str | None = typer.Option(None, "--requested-plan-code"),
    budget_number: str | None = typer.Option(None, "--budget-number"),
    budget_owner_name: str | None = typer.Option(None, "--budget-owner-name"),
    api_key_name: str | None = typer.Option(None, "--api-key-name"),
    team_name: str | None = typer.Option(None, "--team-name"),
    primary_contact_name: str | None = typer.Option(None, "--primary-contact-name"),
    secondary_contact_name: str | None = typer.Option(None, "--secondary-contact-name"),
    department_name: str | None = typer.Option(None, "--department-name"),
    actor_id: str | None = typer.Option(None, "--actor-id"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    contract_id = _run(
        lambda: service.apply_new(
            requested_plan_code=_required_str(requested_plan_code, "requested_plan_code", non_interactive).upper(),
            budget_number=_required_str(budget_number, "budget_number", non_interactive),
            budget_owner_name=_required_str(budget_owner_name, "budget_owner_name", non_interactive),
            api_key_name=_required_str(api_key_name, "api_key_name", non_interactive),
            team_name=_required_str(team_name, "team_name", non_interactive),
            primary_contact_name=_required_str(primary_contact_name, "primary_contact_name", non_interactive),
            secondary_contact_name=_required_str(secondary_contact_name, "secondary_contact_name", non_interactive),
            department_name=_required_str(department_name, "department_name", non_interactive),
            actor_id=actor_id,
        )
    )
    conn.close()
    typer.echo(contract_id)


@app.command("approve-application")
def approve_application(
    contract_id: str | None = typer.Option(None, "--contract-id"),
    actor_id: str | None = typer.Option(None, "--actor-id"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    event_id = _run(
        lambda: service.approve_application(
            contract_id=_required_str(contract_id, "contract_id", non_interactive),
            actor_id=actor_id,
        )
    )
    conn.close()
    typer.echo(event_id)


@app.command("reject-application")
def reject_application(
    contract_id: str | None = typer.Option(None, "--contract-id"),
    reason: str | None = typer.Option(None, "--reason"),
    actor_id: str | None = typer.Option(None, "--actor-id"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    event_id = _run(
        lambda: service.reject_application(
            contract_id=_required_str(contract_id, "contract_id", non_interactive),
            reason=_required_str(reason, "reason", non_interactive),
            actor_id=actor_id,
        )
    )
    conn.close()
    typer.echo(event_id)


@app.command("issue-api-key")
def issue_api_key(
    contract_id: str | None = typer.Option(None, "--contract-id"),
    actor_id: str | None = typer.Option(None, "--actor-id"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    api_key_id = _run(
        lambda: service.issue_api_key(
            contract_id=_required_str(contract_id, "contract_id", non_interactive),
            actor_id=actor_id,
        )
    )
    conn.close()
    typer.echo(api_key_id)


@app.command("change-budget-number")
def change_budget_number(
    contract_id: str | None = typer.Option(None, "--contract-id"),
    to_budget_number: str | None = typer.Option(None, "--to-budget-number"),
    to_budget_owner_name: str | None = typer.Option(None, "--to-budget-owner-name"),
    actor_id: str | None = typer.Option(None, "--actor-id"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    event_id = _run(
        lambda: service.change_budget_number(
            contract_id=_required_str(contract_id, "contract_id", non_interactive),
            to_budget_number=_required_str(to_budget_number, "to_budget_number", non_interactive),
            to_budget_owner_name=_required_str(to_budget_owner_name, "to_budget_owner_name", non_interactive),
            actor_id=actor_id,
        )
    )
    conn.close()
    typer.echo(event_id)


@app.command("change-plan")
def change_plan(
    contract_id: str | None = typer.Option(None, "--contract-id"),
    to_plan_code: str | None = typer.Option(None, "--to-plan-code"),
    actor_id: str | None = typer.Option(None, "--actor-id"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    event_id = _run(
        lambda: service.change_plan(
            contract_id=_required_str(contract_id, "contract_id", non_interactive),
            to_plan_code=_required_str(to_plan_code, "to_plan_code", non_interactive).upper(),
            actor_id=actor_id,
        )
    )
    conn.close()
    typer.echo(event_id)


@app.command("request-cancellation")
def request_cancellation(
    contract_id: str | None = typer.Option(None, "--contract-id"),
    reason: str | None = typer.Option(None, "--reason"),
    actor_id: str | None = typer.Option(None, "--actor-id"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    event_id = _run(
        lambda: service.request_cancellation(
            contract_id=_required_str(contract_id, "contract_id", non_interactive),
            reason=_required_str(reason, "reason", non_interactive),
            actor_id=actor_id,
        )
    )
    conn.close()
    typer.echo(event_id)


@app.command("terminate-contract")
def terminate_contract(
    contract_id: str | None = typer.Option(None, "--contract-id"),
    reason: str | None = typer.Option(None, "--reason"),
    actor_id: str | None = typer.Option(None, "--actor-id"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    event_id = _run(
        lambda: service.terminate_contract(
            contract_id=_required_str(contract_id, "contract_id", non_interactive),
            reason=_required_str(reason, "reason", non_interactive),
            actor_id=actor_id,
        )
    )
    conn.close()
    typer.echo(event_id)


@app.command("record-usage-daily")
def record_usage_daily(
    contract_id: str | None = typer.Option(None, "--contract-id"),
    usage_date: str | None = typer.Option(None, "--usage-date"),
    model_name: str | None = typer.Option(None, "--model-name"),
    used_tokens: int | None = typer.Option(None, "--used-tokens"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    _run(
        lambda: service.record_usage_daily(
            contract_id=_required_str(contract_id, "contract_id", non_interactive),
            usage_date=_required_date(usage_date, "usage_date(YYYY-MM-DD)", non_interactive),
            model_name=_required_str(model_name, "model_name", non_interactive),
            used_tokens=_required_int(used_tokens, "used_tokens", non_interactive),
        )
    )
    conn.close()
    typer.echo("ok")


@app.command("create-monthly-invoices")
def create_monthly_invoices(
    target_year_month: str | None = typer.Option(None, "--target-year-month"),
    as_json: bool = typer.Option(False, "--json"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    invoices = _run(
        lambda: service.create_monthly_invoices(
            target_year_month=_required_str(target_year_month, "target_year_month(YYYY-MM)", non_interactive)
        )
    )
    conn.close()
    _output([i.__dict__ for i in invoices], as_json)


@app.command("show-contract")
def show_contract(
    contract_id: str | None = typer.Option(None, "--contract-id"),
    as_json: bool = typer.Option(False, "--json"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    data = _run(lambda: service.get_contract(_required_str(contract_id, "contract_id", non_interactive)))
    conn.close()
    _output(data, as_json)


@app.command("list-contracts")
def list_contracts(
    as_json: bool = typer.Option(False, "--json"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    data = _run(service.list_contracts)
    conn.close()
    _output(data, as_json)


@app.command("show-events")
def show_events(
    contract_id: str | None = typer.Option(None, "--contract-id"),
    as_json: bool = typer.Option(False, "--json"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    data = _run(lambda: service.list_events(_required_str(contract_id, "contract_id", non_interactive)))
    conn.close()
    _output(data, as_json)


@app.command("list-invoices")
def list_invoices(
    target_year_month: str | None = typer.Option(None, "--target-year-month"),
    as_json: bool = typer.Option(False, "--json"),
    non_interactive: bool = typer.Option(False, "--non-interactive"),
    db_path: str = typer.Option("./data/service_contract.duckdb", "--db-path"),
) -> None:
    service, conn = _build_service(db_path)
    data = _run(
        lambda: service.list_invoices(
            _required_str(target_year_month, "target_year_month(YYYY-MM)", non_interactive)
        )
    )
    conn.close()
    _output(data, as_json)


if __name__ == "__main__":
    app()
