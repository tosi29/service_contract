from __future__ import annotations

import io
import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from uuid import uuid4

import pandas as pd

from app.db import ALLOWED_EVENT_TYPES


@dataclass
class BillingResult:
    created_count: int
    skipped_count: int


def _validate_event(event_type: str, payload: dict) -> None:
    if event_type not in ALLOWED_EVENT_TYPES:
        raise ValueError(f"unsupported event_type: {event_type}")
    if not isinstance(payload, dict):
        raise ValueError("payload_json must be object")


def _parse_ts(value: str) -> datetime:
    if not value:
        raise ValueError("occurred_at is required")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def register_event(
    conn,
    *,
    contract_id: str,
    event_type: str,
    occurred_at: str,
    actor_type: str,
    actor_id: str | None,
    payload_json: str,
    correlation_id: str | None = None,
) -> str:
    payload = json.loads(payload_json)
    _validate_event(event_type, payload)
    occurred = _parse_ts(occurred_at)
    event_id = str(uuid4())

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            """
            INSERT INTO contract_event (
                event_id, contract_id, event_type, occurred_at, actor_type, actor_id, payload_json, correlation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [event_id, contract_id, event_type, occurred, actor_type, actor_id, payload_json, correlation_id],
        )
        _apply_event(conn, event_id, contract_id, event_type, occurred, payload)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return event_id


def _require_state(conn, contract_id: str):
    row = conn.execute(
        "SELECT * FROM contract_state WHERE contract_id = ?",
        [contract_id],
    ).fetchone()
    if not row:
        raise ValueError(f"contract_state not found: {contract_id}")
    return row


def _has_approval_event(conn, contract_id: str) -> bool:
    result = conn.execute(
        """
        SELECT COUNT(*) FROM contract_event
        WHERE contract_id = ?
          AND event_type IN ('APPLICATION_APPROVED', 'APPLICATION_AUTO_APPROVED')
        """,
        [contract_id],
    ).fetchone()[0]
    return result > 0


def _apply_event(conn, event_id: str, contract_id: str, event_type: str, occurred: datetime, payload: dict) -> None:
    if event_type == "APPLICATION_SUBMITTED":
        required = (
            "requested_plan_code",
            "budget_number",
            "budget_owner_name",
            "api_key_name",
            "team_name",
            "primary_contact_name",
            "secondary_contact_name",
            "department_name",
        )
        for key in required:
            if key not in payload or payload[key] in (None, ""):
                raise ValueError(f"payload missing required field: {key}")
        exists = conn.execute(
            "SELECT COUNT(*) FROM contract_state WHERE contract_id = ?",
            [contract_id],
        ).fetchone()[0]
        if exists:
            raise ValueError(f"contract_state already exists: {contract_id}")
        conn.execute(
            """
            INSERT INTO contract_state (
                contract_id, contract_lifecycle_state, current_plan_code, budget_number, budget_owner_name,
                api_key_name, team_name, primary_contact_name, secondary_contact_name, department_name,
                last_event_id
            ) VALUES (?, 'APPLYING', ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                contract_id,
                payload["requested_plan_code"],
                payload["budget_number"],
                payload["budget_owner_name"],
                payload["api_key_name"],
                payload["team_name"],
                payload["primary_contact_name"],
                payload["secondary_contact_name"],
                payload["department_name"],
                event_id,
            ],
        )
        return

    state = _require_state(conn, contract_id)
    lifecycle = state[1]

    if event_type in ("APPLICATION_APPROVED", "APPLICATION_REJECTED", "APPLICATION_AUTO_APPROVED", "CANCELLATION_REQUESTED"):
        conn.execute(
            "UPDATE contract_state SET last_event_id = ?, version_no = version_no + 1, updated_at = CURRENT_TIMESTAMP WHERE contract_id = ?",
            [event_id, contract_id],
        )
        return

    if event_type == "API_KEY_ISSUED":
        if lifecycle != "APPLYING":
            raise ValueError("API_KEY_ISSUED requires APPLYING state")
        plan_code = state[2]
        approval_required = conn.execute(
            "SELECT approval_required FROM plan_master WHERE plan_code = ?",
            [plan_code],
        ).fetchone()
        if approval_required and approval_required[0] and not _has_approval_event(conn, contract_id):
            raise ValueError("approval event is required before API_KEY_ISSUED")
        api_key_id = payload.get("api_key_id")
        issued_at = payload.get("issued_at")
        started_at = _parse_ts(issued_at) if issued_at else occurred
        conn.execute(
            """
            UPDATE contract_state
            SET contract_lifecycle_state = 'ACTIVE',
                api_key_id = ?,
                service_started_at = ?,
                last_event_id = ?,
                version_no = version_no + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE contract_id = ?
            """,
            [api_key_id, started_at, event_id, contract_id],
        )
        return

    if event_type == "PLAN_CHANGED":
        to_plan_code = payload.get("to_plan_code")
        if not to_plan_code:
            raise ValueError("to_plan_code is required")
        conn.execute(
            """
            UPDATE contract_state
            SET current_plan_code = ?, last_event_id = ?, version_no = version_no + 1, updated_at = CURRENT_TIMESTAMP
            WHERE contract_id = ?
            """,
            [to_plan_code, event_id, contract_id],
        )
        return

    if event_type == "BUDGET_NUMBER_CHANGED":
        budget_number = payload.get("to_budget_number")
        budget_owner_name = payload.get("to_budget_owner_name")
        if not budget_number or not budget_owner_name:
            raise ValueError("to_budget_number and to_budget_owner_name are required")
        conn.execute(
            """
            UPDATE contract_state
            SET budget_number = ?, budget_owner_name = ?, last_event_id = ?,
                version_no = version_no + 1, updated_at = CURRENT_TIMESTAMP
            WHERE contract_id = ?
            """,
            [budget_number, budget_owner_name, event_id, contract_id],
        )
        return

    if event_type == "CONTRACT_TERMINATED":
        if lifecycle != "ACTIVE":
            raise ValueError("CONTRACT_TERMINATED requires ACTIVE state")
        terminated_at = payload.get("terminated_at")
        ended_at = _parse_ts(terminated_at) if terminated_at else occurred
        conn.execute(
            """
            UPDATE contract_state
            SET contract_lifecycle_state = 'TERMINATED',
                service_ended_at = ?,
                last_event_id = ?,
                version_no = version_no + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE contract_id = ?
            """,
            [ended_at, event_id, contract_id],
        )
        return

    raise ValueError(f"unsupported event_type: {event_type}")


def import_usage_dataframe(conn, df: pd.DataFrame) -> int:
    expected = {"contract_id", "usage_date", "model_name", "used_tokens"}
    if not expected.issubset(df.columns):
        raise ValueError("required columns: contract_id, usage_date, model_name, used_tokens")

    frame = df[["contract_id", "usage_date", "model_name", "used_tokens"]].copy()
    frame["usage_date"] = pd.to_datetime(frame["usage_date"]).dt.date
    frame["used_tokens"] = pd.to_numeric(frame["used_tokens"], errors="raise").astype("int64")

    conn.register("usage_import_df", frame)
    try:
        conn.execute(
            """
            INSERT INTO usage_daily (contract_id, usage_date, model_name, used_tokens)
            SELECT contract_id, usage_date, model_name, used_tokens
            FROM usage_import_df
            """
        )
    finally:
        conn.unregister("usage_import_df")
    return len(frame)


def create_invoices(conn, target_year_month: str) -> BillingResult:
    month_start = datetime.strptime(target_year_month + "-01", "%Y-%m-%d").date()
    if month_start.month == 12:
        next_month = date(month_start.year + 1, 1, 1)
    else:
        next_month = date(month_start.year, month_start.month + 1, 1)
    month_end = next_month.fromordinal(next_month.toordinal() - 1)

    rows = conn.execute(
        """
        SELECT
            cs.contract_id,
            cs.budget_number,
            cs.team_name,
            pm.plan_name,
            pm.monthly_base_fee_yen,
            pm.token_fee_per_million_yen,
            COALESCE(SUM(ud.used_tokens), 0) AS used_tokens
        FROM contract_state cs
        JOIN plan_master pm ON pm.plan_code = cs.current_plan_code
        LEFT JOIN usage_daily ud
          ON ud.contract_id = cs.contract_id
         AND ud.usage_date BETWEEN ? AND ?
        WHERE cs.service_started_at IS NOT NULL
          AND CAST(cs.service_started_at AS DATE) <= ?
          AND (cs.service_ended_at IS NULL OR CAST(cs.service_ended_at AS DATE) >= ?)
        GROUP BY
            cs.contract_id,
            cs.budget_number,
            cs.team_name,
            pm.plan_name,
            pm.monthly_base_fee_yen,
            pm.token_fee_per_million_yen
        ORDER BY cs.contract_id
        """,
        [month_start, month_end, month_end, month_start],
    ).fetchall()

    created = 0
    skipped = 0
    for row in rows:
        contract_id, budget_number, team_name, plan_name, base_fee, token_fee_per_million, used_tokens = row
        exists = conn.execute(
            "SELECT COUNT(*) FROM invoice WHERE budget_number = ? AND target_year_month = ?",
            [budget_number, target_year_month],
        ).fetchone()[0]
        if exists:
            skipped += 1
            continue
        token_fee = int(math.floor((int(used_tokens) * int(token_fee_per_million)) / 1_000_000))
        total = int(base_fee) + token_fee
        conn.execute(
            """
            INSERT INTO invoice (
                invoice_id, contract_id, budget_number, title, description, target_year_month, total_amount_yen
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid4()),
                contract_id,
                budget_number,
                f"{target_year_month}月分請求",
                f"{plan_name}プラン {team_name} {target_year_month}月分",
                target_year_month,
                total,
            ],
        )
        created += 1
    return BillingResult(created_count=created, skipped_count=skipped)


def dataframe_to_file_bytes(df: pd.DataFrame, fmt: str) -> bytes:
    if fmt == "csv":
        return df.to_csv(index=False).encode("utf-8-sig")
    if fmt == "xlsx":
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="data")
        return buffer.getvalue()
    raise ValueError(f"unsupported format: {fmt}")

