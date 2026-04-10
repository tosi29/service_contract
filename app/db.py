from __future__ import annotations

import os
from pathlib import Path

import duckdb


DB_PATH_ENV = "SERVICE_CONTRACT_DB_PATH"
DEFAULT_DB_PATH = "data/service_contract.duckdb"

ALLOWED_EVENT_TYPES = (
    "APPLICATION_SUBMITTED",
    "APPLICATION_APPROVED",
    "APPLICATION_REJECTED",
    "APPLICATION_AUTO_APPROVED",
    "API_KEY_ISSUED",
    "BUDGET_NUMBER_CHANGED",
    "PLAN_CHANGED",
    "CANCELLATION_REQUESTED",
    "CONTRACT_TERMINATED",
)


def get_db_path() -> str:
    return os.getenv(DB_PATH_ENV, DEFAULT_DB_PATH)


def connect(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    path = db_path or get_db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(path)


def init_db(db_path: str | None = None) -> None:
    conn = connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS plan_master (
                plan_code TEXT PRIMARY KEY,
                plan_name TEXT NOT NULL,
                monthly_base_fee_yen INTEGER NOT NULL,
                token_fee_per_million_yen INTEGER NOT NULL,
                daily_token_limit INTEGER NOT NULL,
                available_model_scope TEXT NOT NULL,
                approval_required BOOLEAN NOT NULL,
                is_active BOOLEAN NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contract_event (
                event_id TEXT PRIMARY KEY,
                contract_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                occurred_at TIMESTAMP NOT NULL,
                recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                actor_type TEXT NOT NULL,
                actor_id TEXT,
                payload_json TEXT NOT NULL,
                correlation_id TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contract_state (
                contract_id TEXT PRIMARY KEY,
                contract_lifecycle_state TEXT NOT NULL,
                current_plan_code TEXT NOT NULL REFERENCES plan_master(plan_code),
                budget_number TEXT NOT NULL,
                budget_owner_name TEXT NOT NULL,
                api_key_name TEXT NOT NULL,
                team_name TEXT NOT NULL,
                primary_contact_name TEXT NOT NULL,
                secondary_contact_name TEXT NOT NULL,
                department_name TEXT NOT NULL,
                api_key_id TEXT,
                service_started_at TIMESTAMP,
                service_ended_at TIMESTAMP,
                last_event_id TEXT NOT NULL,
                version_no BIGINT NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS usage_daily (
                contract_id TEXT NOT NULL REFERENCES contract_state(contract_id),
                usage_date DATE NOT NULL,
                model_name TEXT NOT NULL,
                used_tokens BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (contract_id, usage_date, model_name)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS invoice (
                invoice_id TEXT PRIMARY KEY,
                contract_id TEXT NOT NULL REFERENCES contract_state(contract_id),
                budget_number TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                target_year_month TEXT NOT NULL,
                total_amount_yen BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (budget_number, target_year_month)
            );
            """
        )
        conn.executemany(
            """
            INSERT OR IGNORE INTO plan_master (
                plan_code, plan_name, monthly_base_fee_yen, token_fee_per_million_yen,
                daily_token_limit, available_model_scope, approval_required, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("TRIAL", "Trial", 0, 100, 10000, "MINI_ONLY", False, True),
                ("STARTER", "Starter", 20000, 1000, 500000, "ALL", True, True),
                ("PRO", "Pro", 30000, 1000, 1000000, "ALL", True, True),
            ],
        )
    finally:
        conn.close()
