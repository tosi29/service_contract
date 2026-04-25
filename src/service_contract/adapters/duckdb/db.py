from __future__ import annotations

import duckdb


class DuckDbConnectionProvider:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)


def init_db_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS plan_master (
            plan_code TEXT PRIMARY KEY,
            plan_name TEXT NOT NULL,
            monthly_base_fee_yen BIGINT NOT NULL,
            token_fee_per_million_yen BIGINT NOT NULL,
            daily_token_limit BIGINT NOT NULL,
            available_model_scope TEXT NOT NULL,
            approval_required BOOLEAN NOT NULL,
            is_active BOOLEAN NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS contract_event (
            event_id TEXT PRIMARY KEY,
            contract_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            occurred_at TIMESTAMP NOT NULL,
            recorded_at TIMESTAMP NOT NULL,
            actor_type TEXT NOT NULL,
            actor_id TEXT,
            payload_json TEXT NOT NULL,
            correlation_id TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS contract_state (
            contract_id TEXT PRIMARY KEY,
            contract_lifecycle_state TEXT NOT NULL,
            current_plan_code TEXT NOT NULL,
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
            version_no BIGINT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS usage_daily (
            usage_date DATE NOT NULL,
            contract_id TEXT NOT NULL,
            model_name TEXT NOT NULL,
            used_tokens BIGINT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY (contract_id, usage_date, model_name)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice (
            invoice_id TEXT PRIMARY KEY,
            budget_number TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            target_year_month TEXT NOT NULL,
            total_amount_yen BIGINT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
