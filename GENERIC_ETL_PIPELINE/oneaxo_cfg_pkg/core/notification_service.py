from __future__ import annotations

import copy
import json
from collections import defaultdict
from html import escape

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.api_auth_client import ApiClient

CONTACT_API = "contact_api_config"


def _common_nomerch_tables(descripcion: str) -> list[dict]:
    return [
        {
            "style": "width:100.0%; border-collapse:collapse; border:1px solid white; border-spacing:0",
            "nameToReplace": "${tables}",
            "header": {
                "style": "background:green; color:white; padding:1.5pt",
                "columns": [
                    {"style": "width:20%", "value": "NUMERO DE RESULTADO"},
                    {"style": "width:70%", "value": "DESCRIPCION"},
                ],
            },
            "body": {"rows": [[{"style": " ", "value": "Sin avisos"}]]},
        },
        {
            "style": "width:100.0%; border-collapse:collapse; border:1px solid white; border-spacing:0",
            "nameToReplace": "${table_2}",
            "header": {
                "style": "background:orange; color:white; padding:1.5pt",
                "columns": [
                    {"style": "width:20%", "value": "NUMERO DE AVISO"},
                    {"style": "width:70%", "value": "DESCRIPCION"},
                ],
            },
            "body": {"rows": [[{"style": " ", "value": "Sin avisos"}]]},
        },
        {
            "style": "width:100.0%; border-collapse:collapse; border:1px solid white; border-spacing:0",
            "nameToReplace": "${table_3}",
            "header": {
                "style": "background:firebrick; color:white; padding:1.5pt",
                "columns": [
                    {"style": "width:20%", "value": "NUMERO DE ERROR"},
                    {"style": "width:70%", "value": "DESCRIPCION"},
                ],
            },
            "body": {
                "rows": [
                    [
                        {
                            "style": "border: 1px solid black",
                            "value": '<span style="font-size: 10.5pt; color: #cc6262;">1</span>',
                        },
                        {
                            "style": "border: 1px solid black",
                            "value": escape(descripcion),
                        },
                    ]
                ]
            },
        },
    ]


def _set_nomerch_header(notification: dict) -> None:
    notification["subject"] = (
        "DEV-Error en integracion 123 | El Proceso tiene errores en su ejecucion"
    )

    notification["variables"] = [
        {
            "name": "${description}",
            "value": "<strong>Esta es una notificacion de error</strong>",
        },
        {
            "name": "${extra_info}",
            "value": (
                '<p><span style="font-size:10.5pt; color:#666699">'
                "Numero de Integracion: 123"
                "</span></p>"
                '<p><span style="font-size:10.5pt; color:#666699">'
                "Module: SUPPLY"
                "</span></p>"
            ),
        },
    ]


def _build_complex_notification_payload(
    base_template: dict,
    items: list,
    integration_id: str,
    run_id: str | None,
    issue_code: str,
) -> dict:
    payload = {"notification": copy.deepcopy(base_template)}
    notification = payload["notification"]

    # ============================================================
    # FORMATO ESPECIAL NOMERCH - PEDIMENTOS INVÁLIDOS CON LÍNEA
    # ============================================================
    if integration_id == "R092B_NOMERCH" and issue_code == "invalid_customs_order":
        source_file = items[0].get("source_file") if items else ""
        filename = source_file.split("/")[-1] if source_file else "archivo"

        pedimentos = []

        for item in items:
            pedimento = (item.get("message") or "").strip()
            linea = (item.get("record_identifier") or "").strip()

            if not pedimento:
                continue

            if linea:
                pedimentos.append(f"{linea}: {pedimento}")
            else:
                pedimentos.append(pedimento)

        descripcion = (
            f"El excel adjunto {filename} contiene los siguientes pedimentos inválidos: "
            + ", ".join(pedimentos)
        )

        _set_nomerch_header(notification)
        notification["tables"] = _common_nomerch_tables(descripcion)

        return payload

    # ============================================================
    # FORMATO ESPECIAL NOMERCH - TRANSPORTISTA NO ENCONTRADO
    # ============================================================
    if integration_id == "R092B_NOMERCH" and issue_code == "carrier_not_found":
        transportistas = sorted({
            (item.get("message") or "").strip()
            for item in items
            if item.get("message")
        })

        descripcion = (
            f"El transportista {', '.join(transportistas)} no fue encontrado "
            "en el catálogo de traducción. Favor de validar el dato o solicitar el alta, "
            "se cancela el envío."
        )

        _set_nomerch_header(notification)
        notification["tables"] = _common_nomerch_tables(descripcion)

        return payload

    # ============================================================
    # FORMATO DEFAULT
    # ============================================================
    first_file = items[0]["source_file"] if items else "N/A"

    notification["subject"] = (
        f"PROD-NOTIFICACIÓN ERROR INTEGRACIÓN: {integration_id} - Archivo: {first_file}"
    )

    extra_info_html = (
        f"<p><span>Número de Integración: {escape(integration_id)}</span></p>"
        f"<p><span>DAG Run ID: {escape(run_id or 'N/A')}</span></p>"
        f"<p><span>Código de Error: {escape(issue_code)}</span></p>"
    )

    found_extra_info = False
    for var in notification.get("variables", []):
        if var.get("name") == "${extra_info}":
            var["value"] = extra_info_html
            found_extra_info = True
            break

    if not found_extra_info:
        notification.setdefault("variables", []).append({
            "name": "${extra_info}",
            "value": extra_info_html,
        })

    error_rows = []
    for i, item in enumerate(items):
        row = [
            {
                "style": "border: 1px solid black",
                "value": f'<span style="color: #cc6262;">{i + 1}</span>',
            },
            {
                "style": "border: 1px solid black",
                "value": escape(item["message"] or ""),
            },
            {
                "style": "border: 1px solid black",
                "value": escape(item["source_file"] or ""),
            },
            {
                "style": "border: 1px solid black",
                "value": escape(item["record_identifier"] or ""),
            },
        ]
        error_rows.append(row)

    notification["tables"] = [
        {
            "nameToReplace": "${tables}",
            "header": {
                "style": "background:green; color:white;",
                "columns": [
                    {"value": "NUMERO DE RESULTADO"},
                    {"value": "DESCRIPCION"},
                ],
            },
            "body": {"rows": [[{"value": "Sin resultados"}]]},
        },
        {
            "nameToReplace": "${table_2}",
            "header": {
                "style": "background:orange; color:white;",
                "columns": [
                    {"value": "NUMERO DE AVISO"},
                    {"value": "DESCRIPCION"},
                ],
            },
            "body": {"rows": [[{"value": "Sin avisos"}]]},
        },
        {
            "nameToReplace": "${table_3}",
            "header": {
                "style": "background:firebrick; color:white;",
                "columns": [
                    {"value": "#"},
                    {"value": "DESCRIPCIÓN"},
                    {"value": "ARCHIVO ORIGEN"},
                    {"value": "REGISTRO"},
                ],
            },
            "body": {"rows": error_rows},
        },
    ]

    return payload


def persist_validation_issues(
    postgres_conn_id: str,
    dag_id: str,
    run_id: str | None,
    task_id: str | None,
    issues: list[dict],
) -> None:
    if not issues:
        return

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    rows = [
        (
            dag_id,
            run_id,
            task_id,
            issue["integration_id"],
            issue["source_file"],
            issue["issue_code"],
            issue["severity"],
            issue["field_name"],
            issue["message"],
            issue["record_identifier"],
        )
        for issue in issues
    ]

    hook.insert_rows(
        table="ctrlplane.tbl_integration_validation_issue_scm",
        rows=rows,
        target_fields=[
            "dag_id",
            "run_id",
            "task_id",
            "integration_id",
            "source_file",
            "issue_code",
            "severity",
            "field_name",
            "message",
            "record_identifier",
        ],
    )


def send_pending_validation_notifications(
    postgres_conn_id: str,
    dag_id: str,
    run_id: str | None,
) -> int:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    sql = """
        SELECT
            i.id,
            i.integration_id,
            i.source_file,
            i.issue_code,
            i.severity,
            i.field_name,
            i.message,
            i.record_identifier,

            n.channel_type,
            n.http_conn_id,
            n.http_endpoint,
            n.http_method,
            n.to_emails,
            n.headers_json,
            n.payload_template,
            n.subject_template
        FROM ctrlplane.tbl_integration_validation_issue_scm i
        JOIN ctrlplane.tbl_integration_notification_scm n
          ON n.integration_id = i.integration_id
         AND n.issue_code = i.issue_code
         AND n.active = true
        WHERE i.dag_id = %s
          AND i.run_id = %s
          AND i.notified_at IS NULL
        ORDER BY i.integration_id, i.issue_code, i.source_file, i.id
    """

    rows = hook.get_records(sql, parameters=(dag_id, run_id))

    if not rows:
        return 0

    grouped = defaultdict(list)

    for row in rows:
        key = (
            row[1],
            row[3],
            row[8],
            row[9],
            row[10],
            row[11],
            tuple(row[12] or []),
            json.dumps(row[13] or {}, sort_keys=True),
            json.dumps(row[14] or {}, sort_keys=True),
            row[15],
        )

        grouped[key].append(
            {
                "id": row[0],
                "integration_id": row[1],
                "source_file": row[2],
                "issue_code": row[3],
                "severity": row[4],
                "field_name": row[5],
                "message": row[6],
                "record_identifier": row[7],
            }
        )

    sent_groups = 0

    for key, items in grouped.items():
        (
            integration_id,
            issue_code,
            channel_type,
            http_conn_id,
            http_endpoint,
            http_method,
            to_emails_tuple,
            headers_json_raw,
            payload_template_raw,
            subject_template,
        ) = key

        if channel_type != "http_api":
            continue

        headers_json = json.loads(headers_json_raw)
        payload_template = json.loads(payload_template_raw)

        final_payload = _build_complex_notification_payload(
            base_template=payload_template,
            items=items,
            integration_id=integration_id,
            run_id=run_id,
            issue_code=issue_code,
        )

        api_creds = Variable.get(CONTACT_API, deserialize_json=True)
        client = ApiClient(**api_creds)

        if not client:
            raise ValueError(
                "El cliente contact no fue inicializado en el diccionario de servicios."
            )

        response = client.post(
            endpoint=http_endpoint,
            payload=final_payload,
        )

        if response.status_code >= 400:
            raise ValueError(
                f"Falló notificación HTTP. status={response.status_code}, body={response.text}"
            )

        for item in items:
            hook.run(
                """
                UPDATE ctrlplane.tbl_integration_validation_issue_scm
                SET notified_at = now()
                WHERE id = %s
                """,
                parameters=(item["id"],),
            )

        sent_groups += 1

    return sent_groups