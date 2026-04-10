from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from app.db import init_db, connect
from app.services import create_invoices, dataframe_to_file_bytes, import_usage_dataframe, register_event


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def create_app() -> FastAPI:
    app = FastAPI(title="Service Contract")

    @app.on_event("startup")
    def startup() -> None:
        init_db()

    @app.get("/")
    def root() -> RedirectResponse:
        return RedirectResponse("/contracts", status_code=303)

    @app.get("/contracts")
    def contracts(request: Request, message: str | None = None):
        conn = connect()
        try:
            contracts_rows = conn.execute(
                """
                SELECT
                    contract_id, contract_lifecycle_state, current_plan_code, budget_number,
                    team_name, api_key_name, service_started_at, service_ended_at, updated_at
                FROM contract_state
                ORDER BY updated_at DESC
                """
            ).fetchall()
        finally:
            conn.close()
        return templates.TemplateResponse(
            request=request,
            name="contracts.html",
            context={"contracts": contracts_rows, "message": message},
        )

    @app.get("/events/new")
    def event_form(request: Request, message: str | None = None):
        return templates.TemplateResponse(request=request, name="event_form.html", context={"message": message})

    @app.post("/events/new")
    async def post_event(
        contract_id: str = Form(...),
        event_type: str = Form(...),
        occurred_at: str = Form(...),
        actor_type: str = Form("SYSTEM"),
        actor_id: str | None = Form(default=None),
        payload_json: str = Form(...),
    ):
        conn = connect()
        try:
            register_event(
                conn,
                contract_id=contract_id,
                event_type=event_type,
                occurred_at=occurred_at,
                actor_type=actor_type,
                actor_id=actor_id,
                payload_json=payload_json,
            )
        except Exception as exc:
            conn.close()
            return RedirectResponse(f"/events/new?message=error:{exc}", status_code=303)
        conn.close()
        return RedirectResponse("/contracts?message=event_created", status_code=303)

    @app.get("/usage/import")
    def usage_import_form(request: Request, message: str | None = None):
        return templates.TemplateResponse(request=request, name="usage_import.html", context={"message": message})

    @app.post("/usage/import")
    async def usage_import(file: UploadFile = File(...)):
        suffix = Path(file.filename or "").suffix.lower()
        content = await file.read()
        conn = connect()
        try:
            if suffix == ".csv":
                df = pd.read_csv(io.BytesIO(content))
            elif suffix in (".xlsx", ".xlsm"):
                df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
            else:
                return RedirectResponse("/usage/import?message=unsupported_file", status_code=303)
            count = import_usage_dataframe(conn, df)
        except Exception as exc:
            conn.close()
            return RedirectResponse(f"/usage/import?message=error:{exc}", status_code=303)
        conn.close()
        return RedirectResponse(f"/usage/import?message=imported:{count}", status_code=303)

    @app.get("/invoices")
    def invoice_list(request: Request, message: str | None = None):
        conn = connect()
        try:
            rows = conn.execute(
                """
                SELECT invoice_id, contract_id, budget_number, title, description, target_year_month, total_amount_yen, created_at
                FROM invoice
                ORDER BY created_at DESC
                """
            ).fetchall()
        finally:
            conn.close()
        return templates.TemplateResponse(request=request, name="invoices.html", context={"invoices": rows, "message": message})

    @app.post("/billing/{yyyy_mm}/create")
    def create_billing(yyyy_mm: str):
        conn = connect()
        try:
            result = create_invoices(conn, yyyy_mm)
        except Exception as exc:
            conn.close()
            return RedirectResponse(f"/invoices?message=error:{exc}", status_code=303)
        conn.close()
        return RedirectResponse(
            f"/invoices?message=created:{result.created_count},skipped:{result.skipped_count}",
            status_code=303,
        )

    @app.get("/export/{dataset}.{fmt}")
    def export_dataset(dataset: str, fmt: str):
        if fmt not in ("csv", "xlsx"):
            return Response(status_code=400, content="unsupported format")
        query_map = {
            "contracts": "SELECT * FROM contract_state ORDER BY updated_at DESC",
            "usage": "SELECT * FROM usage_daily ORDER BY usage_date DESC, contract_id",
            "invoices": "SELECT * FROM invoice ORDER BY created_at DESC",
        }
        if dataset not in query_map:
            return Response(status_code=400, content="unsupported dataset")

        conn = connect()
        try:
            df = conn.execute(query_map[dataset]).df()
        finally:
            conn.close()

        content = dataframe_to_file_bytes(df, fmt)
        media_type = "text/csv" if fmt == "csv" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        headers = {"Content-Disposition": f'attachment; filename="{dataset}.{fmt}"'}
        return Response(content=content, media_type=media_type, headers=headers)

    return app


app = create_app()
