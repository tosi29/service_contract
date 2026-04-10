from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from app.db import init_db
from app.services import create_invoices, import_usage_dataframe, register_event


def _conn(tmp_path: Path):
    db_path = tmp_path / "test.duckdb"
    init_db(str(db_path))
    return duckdb.connect(str(db_path))


def _submit_payload(plan_code: str = "STARTER") -> str:
    return json.dumps(
        {
            "requested_plan_code": plan_code,
            "budget_number": "BGT-0001",
            "budget_owner_name": "Owner",
            "api_key_name": "key-a",
            "team_name": "Team A",
            "primary_contact_name": "P",
            "secondary_contact_name": "S",
            "department_name": "Dept",
        }
    )


def test_event_projection_apply_to_active_and_terminated(tmp_path: Path):
    conn = _conn(tmp_path)
    try:
        register_event(
            conn,
            contract_id="C-001",
            event_type="APPLICATION_SUBMITTED",
            occurred_at="2026-04-01T00:00:00+00:00",
            actor_type="USER",
            actor_id="u1",
            payload_json=_submit_payload("STARTER"),
        )
        register_event(
            conn,
            contract_id="C-001",
            event_type="APPLICATION_APPROVED",
            occurred_at="2026-04-01T01:00:00+00:00",
            actor_type="ADMIN",
            actor_id="a1",
            payload_json="{}",
        )
        register_event(
            conn,
            contract_id="C-001",
            event_type="API_KEY_ISSUED",
            occurred_at="2026-04-01T02:00:00+00:00",
            actor_type="SYSTEM",
            actor_id=None,
            payload_json='{"api_key_id":"AK-1"}',
        )
        state = conn.execute(
            "SELECT contract_lifecycle_state, api_key_id FROM contract_state WHERE contract_id = 'C-001'"
        ).fetchone()
        assert state == ("ACTIVE", "AK-1")

        register_event(
            conn,
            contract_id="C-001",
            event_type="CONTRACT_TERMINATED",
            occurred_at="2026-04-10T00:00:00+00:00",
            actor_type="USER",
            actor_id="u1",
            payload_json="{}",
        )
        state2 = conn.execute(
            "SELECT contract_lifecycle_state FROM contract_state WHERE contract_id = 'C-001'"
        ).fetchone()[0]
        assert state2 == "TERMINATED"
    finally:
        conn.close()


def test_usage_daily_composite_pk(tmp_path: Path):
    conn = _conn(tmp_path)
    try:
        register_event(
            conn,
            contract_id="C-002",
            event_type="APPLICATION_SUBMITTED",
            occurred_at="2026-04-01T00:00:00+00:00",
            actor_type="USER",
            actor_id="u1",
            payload_json=_submit_payload("TRIAL"),
        )
        register_event(
            conn,
            contract_id="C-002",
            event_type="APPLICATION_AUTO_APPROVED",
            occurred_at="2026-04-01T00:01:00+00:00",
            actor_type="SYSTEM",
            actor_id=None,
            payload_json="{}",
        )
        register_event(
            conn,
            contract_id="C-002",
            event_type="API_KEY_ISSUED",
            occurred_at="2026-04-01T00:02:00+00:00",
            actor_type="SYSTEM",
            actor_id=None,
            payload_json='{"api_key_id":"AK-2"}',
        )

        df = pd.DataFrame(
            [
                {"contract_id": "C-002", "usage_date": "2026-04-02", "model_name": "mini", "used_tokens": 10},
            ]
        )
        import_usage_dataframe(conn, df)
        with pytest.raises(Exception):
            import_usage_dataframe(conn, df)
    finally:
        conn.close()


def test_billing_create_only_skip_duplicates(tmp_path: Path):
    conn = _conn(tmp_path)
    try:
        register_event(
            conn,
            contract_id="C-003",
            event_type="APPLICATION_SUBMITTED",
            occurred_at="2026-03-01T00:00:00+00:00",
            actor_type="USER",
            actor_id="u1",
            payload_json=_submit_payload("PRO"),
        )
        register_event(
            conn,
            contract_id="C-003",
            event_type="APPLICATION_APPROVED",
            occurred_at="2026-03-01T00:05:00+00:00",
            actor_type="ADMIN",
            actor_id="a1",
            payload_json="{}",
        )
        register_event(
            conn,
            contract_id="C-003",
            event_type="API_KEY_ISSUED",
            occurred_at="2026-03-01T00:10:00+00:00",
            actor_type="SYSTEM",
            actor_id=None,
            payload_json='{"api_key_id":"AK-3"}',
        )
        import_usage_dataframe(
            conn,
            pd.DataFrame(
                [
                    {"contract_id": "C-003", "usage_date": "2026-03-15", "model_name": "gpt-4.1", "used_tokens": 1_500_000}
                ]
            ),
        )

        first = create_invoices(conn, "2026-03")
        second = create_invoices(conn, "2026-03")

        assert first.created_count == 1
        assert first.skipped_count == 0
        assert second.created_count == 0
        assert second.skipped_count == 1
    finally:
        conn.close()
