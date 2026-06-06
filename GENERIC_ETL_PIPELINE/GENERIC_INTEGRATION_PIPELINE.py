# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
from datetime import datetime

from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.db_logger import (
    safe_log_integration_event,
    safe_log_processed_order_event,
)
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.exceptions import (
    ValidationRejectFileError,
    HttpSenderError,
)
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.dynamic_loader import load_handler_instance
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.generic_engine import (
    build_canonical_payloads_from_bytes,
    build_final_payloads_from_canonical,
)
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.integration_loader import (
    load_active_source_endpoints,
    load_runtime_config_by_integration_id,
)
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.notification_service import (
    persist_validation_issues,
    send_pending_validation_notifications,
)

INTEGRATIONS_TO_RUN = ["R011B_POLIZA_NOMINA_212_SPLIT"]

AWS_CONN_ID = "one_axo_s3"
POSTGRES_CONN_ID = "gobierno_central_postgres"

RAW_BUCKET_NAME = "one-axo-raw"
CANONICAL_BUCKET_NAME = "one-axo-canonical"
OUT_BUCKET_NAME = "one-axo-out"

_val = Variable.get("schedule_generic_pipeline", default_var="*/4 * * * *").strip().lower()
SCHEDULE_UI = None if _val in ("none", "manual", "") else _val
CATCHUP_UI = str(
    Variable.get("catchup_generic_pipeline", default_var="false")
).strip().lower() in ("1", "true", "yes", "y", "on")


def _ctx():
    context = get_current_context()
    dag_run = context.get("dag_run")
    return {
        "dag_id": context["dag"].dag_id,
        "run_id": dag_run.run_id if dag_run else None,
        "task_id": context["task"].task_id,
    }


def _safe_stem(filename: str) -> str:
    stem = filename.rsplit(".", 1)[0]
    return stem.replace(" ", "_").replace("/", "_").replace("\\", "_")


def _s3_put_bytes(
    s3_hook: S3Hook,
    bucket_name: str,
    key: str,
    payload: bytes,
    content_type: str | None = None,
):
    extra = {"ContentType": content_type} if content_type else {}
    s3_hook.get_conn().put_object(
        Bucket=bucket_name,
        Key=key,
        Body=payload,
        **extra,
    )


def _s3_get_bytes(s3_hook: S3Hook, bucket_name: str, key: str) -> bytes:
    response = s3_hook.get_conn().get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read()


def _payload_to_bytes(payload) -> bytes:
    """
    Normaliza cualquier salida custom/generic para guardarla en S3.
    - bytes: se guarda directo.
    - str: se codifica UTF-8.
    - dict/list: se convierte a JSON.
    """
    if isinstance(payload, bytes):
        return payload

    if isinstance(payload, str):
        return payload.encode("utf-8")

    return json.dumps(
        payload,
        indent=4,
        ensure_ascii=False,
    ).encode("utf-8")


with DAG(
    dag_id="GENERIC_INTEGRATION_PIPELINE",
    start_date=datetime(2024, 1, 1),
    schedule=SCHEDULE_UI,
    catchup=CATCHUP_UI,
    tags=["etl", "generic"],
) as dag:

    @task
    def discover_and_stage_files() -> list[dict]:
        meta = _ctx()
        source_endpoints = load_active_source_endpoints(
            POSTGRES_CONN_ID,
            INTEGRATIONS_TO_RUN,
        )
        all_jobs = []

        for endpoint in source_endpoints:
            source_config = endpoint["source_config"]
            handler_module = source_config.get("handler_module")
            handler_class = source_config.get("handler_class")

            if not handler_module or not handler_class:
                logging.warning(
                    "Endpoint %s no tiene handler de origen definido. Saltando.",
                    endpoint["endpoint_id"],
                )
                continue

            try:
                global_context = {
                    "aws_conn_id": AWS_CONN_ID,
                    "postgres_conn_id": POSTGRES_CONN_ID,
                    "raw_bucket_name": RAW_BUCKET_NAME,
                    "integrations_to_run": INTEGRATIONS_TO_RUN,
                    "endpoint_id": endpoint["endpoint_id"],
                    "endpoint_name": endpoint["endpoint_name"],
                    "dag_run_id": meta["run_id"],
                    "_s3_put_bytes": _s3_put_bytes,
                    "_safe_stem": _safe_stem,
                }

                handler_instance = load_handler_instance(
                    handler_module,
                    handler_class,
                    config=source_config,
                    global_context=global_context,
                )

                discovered_jobs = handler_instance.discover()

                if discovered_jobs:

                    for job in discovered_jobs:

                        # ==========================================================
                        # NOTIFICACIONES SIN ARCHIVO ADJUNTO
                        # ==========================================================
                        if job.get("notification_only"):

                            persist_validation_issues(
                                postgres_conn_id=POSTGRES_CONN_ID,
                                dag_id=meta["dag_id"],
                                run_id=meta["run_id"],
                                task_id=meta["task_id"],
                                issues=[job["notification_issue"]],
                            )

                            safe_log_integration_event(
                                postgres_conn_id=POSTGRES_CONN_ID,
                                dag_id=meta["dag_id"],
                                run_id=meta["run_id"],
                                task_id=meta["task_id"],
                                integration_id=job["integration_id"],
                                phase="DISCOVER",
                                status="FAILED",
                                source_file=job["source_file"],
                                raw_s3_key=None,
                                message="Correo recibido sin archivo adjunto.",
                                error_message=job["notification_issue"]["message"],
                            )

                            logging.warning(
                                "Se registró issue missing_attachment para correo %s",
                                job["source_file"],
                            )

                            continue

                        # ==========================================================
                        # JOB NORMAL
                        # ==========================================================
                        all_jobs.append(job)

                        safe_log_integration_event(
                            postgres_conn_id=POSTGRES_CONN_ID,
                            dag_id=meta["dag_id"],
                            run_id=meta["run_id"],
                            task_id=meta["task_id"],
                            integration_id=job["integration_id"],
                            phase="DISCOVER",
                            status="SUCCESS",
                            source_file=job["source_file"],
                            raw_s3_key=job["raw_s3_key"],
                            message="Job creado a partir de archivo en staging.",
                        )

            except Exception as handler_exc:
                logging.error(
                    "Fallo el handler %s para endpoint %s: %s",
                    handler_class,
                    endpoint["endpoint_id"],
                    handler_exc,
                )
                safe_log_integration_event(
                    postgres_conn_id=POSTGRES_CONN_ID,
                    dag_id=meta["dag_id"],
                    run_id=meta["run_id"],
                    task_id=meta["task_id"],
                    integration_id=None,
                    phase="DISCOVER",
                    status="FAILED",
                    message=f"Handler {handler_class} falló.",
                    error_message=str(handler_exc),
                )

        return all_jobs

    @task
    def build_and_store_canonical(raw_job: dict) -> dict:
        meta = _ctx()

        integration_id = raw_job["integration_id"]
        source_file = raw_job["source_file"]
        raw_s3_key = raw_job["raw_s3_key"]
        timestamp = raw_job["timestamp"]
        safe_filename = raw_job["safe_filename"]

        try:
            runtime_config = load_runtime_config_by_integration_id(
                POSTGRES_CONN_ID,
                integration_id,
            )
            raw_job["runtime_config"] = runtime_config

            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            raw_bytes = _s3_get_bytes(s3_hook, RAW_BUCKET_NAME, raw_s3_key)

            engine_mode = str(runtime_config.get("engine_mode", "generic")).lower()

            if engine_mode == "custom":
                handler_module_path = runtime_config.get("handler_module_path")
                handler_class = runtime_config.get("handler_class")

                if not handler_module_path or not handler_class:
                    raise ValueError(
                        "Integración custom requiere handler_module_path y handler_class."
                    )

                handler_instance = load_handler_instance(
                    handler_module_path,
                    handler_class,
                )

                if not hasattr(handler_instance, "build_canonical"):
                    raise ValueError(
                        f"El handler custom {handler_class} no implementa build_canonical."
                    )

                canonical_payload = handler_instance.build_canonical(
                    file_bytes=raw_bytes,
                    integration_config=runtime_config,
                    raw_job=raw_job,
                    global_context={
                        "postgres_conn_id": POSTGRES_CONN_ID,
                        "aws_conn_id": AWS_CONN_ID,
                        "raw_bucket_name": RAW_BUCKET_NAME,
                        "canonical_bucket_name": CANONICAL_BUCKET_NAME,
                        "out_bucket_name": OUT_BUCKET_NAME,
                    },
                )

            else:
                canonical_payload = build_canonical_payloads_from_bytes(
                    file_bytes=raw_bytes,
                    integration_config=runtime_config,
                )

            canonical_filename = (
                f"{runtime_config['canonical_filename_prefix']}_{safe_filename}_{timestamp}.json"
            )
            canonical_s3_key = f"{integration_id}/canonical/{canonical_filename}"
            canonical_bytes = json.dumps(
                canonical_payload,
                indent=4,
                ensure_ascii=False,
            ).encode("utf-8")

            _s3_put_bytes(
                s3_hook=s3_hook,
                bucket_name=CANONICAL_BUCKET_NAME,
                key=canonical_s3_key,
                payload=canonical_bytes,
                content_type="application/json",
            )

            safe_log_integration_event(
                postgres_conn_id=POSTGRES_CONN_ID,
                dag_id=meta["dag_id"],
                run_id=meta["run_id"],
                task_id=meta["task_id"],
                integration_id=integration_id,
                phase="CANONICAL",
                status="SUCCESS",
                source_file=source_file,
                raw_s3_key=raw_s3_key,
                canonical_s3_key=canonical_s3_key,
                message="Canonical generado y guardado en S3",
            )

            return {
                **raw_job,
                "canonical_s3_key": canonical_s3_key,
            }

        except ValidationRejectFileError as exc:
            issues = [
                {
                    "integration_id": integration_id,
                    "source_file": source_file,
                    "issue_code": issue.get("issue_code"),
                    "severity": issue.get("severity", "error"),
                    "field_name": issue.get("field_name"),
                    "message": issue.get("message"),
                    "record_identifier": issue.get("record_identifier"),
                }
                for issue in (exc.issues or [])
            ]

            persist_validation_issues(
                postgres_conn_id=POSTGRES_CONN_ID,
                dag_id=meta["dag_id"],
                run_id=meta["run_id"],
                task_id=meta["task_id"],
                issues=issues,
            )

            try:
                sent = send_pending_validation_notifications(
                    postgres_conn_id=POSTGRES_CONN_ID,
                    dag_id=meta["dag_id"],
                    run_id=meta["run_id"],
                )
                logging.info(
                    "Notificaciones de validación enviadas desde CANONICAL: %s",
                    sent,
                )
            except Exception as notify_exc:
                logging.exception(
                    "Falló envío de notificaciones de validación: %s",
                    notify_exc,
                )
                safe_log_integration_event(
                    postgres_conn_id=POSTGRES_CONN_ID,
                    dag_id=meta["dag_id"],
                    run_id=meta["run_id"],
                    task_id=meta["task_id"],
                    integration_id=integration_id,
                    phase="NOTIFY",
                    status="FAILED",
                    source_file=source_file,
                    raw_s3_key=raw_s3_key,
                    message="Error enviando notificación de validación desde CANONICAL",
                    error_message=str(notify_exc),
                )

            safe_log_integration_event(
                postgres_conn_id=POSTGRES_CONN_ID,
                dag_id=meta["dag_id"],
                run_id=meta["run_id"],
                task_id=meta["task_id"],
                integration_id=integration_id,
                phase="CANONICAL",
                status="FAILED",
                source_file=source_file,
                raw_s3_key=raw_s3_key,
                message="Archivo rechazado por validación. Se conserva RAW como evidencia.",
                error_message=str(exc),
            )
            raise

        except Exception as exc:
            safe_log_integration_event(
                postgres_conn_id=POSTGRES_CONN_ID,
                dag_id=meta["dag_id"],
                run_id=meta["run_id"],
                task_id=meta["task_id"],
                integration_id=integration_id,
                phase="CANONICAL",
                status="FAILED",
                source_file=source_file,
                raw_s3_key=raw_s3_key,
                message="Error en fase CANONICAL",
                error_message=str(exc),
            )
            raise

    @task
    def build_and_store_out(canonical_job: dict) -> dict:
        meta = _ctx()

        integration_id = canonical_job["integration_id"]
        source_file = canonical_job["source_file"]
        canonical_s3_key = canonical_job["canonical_s3_key"]
        timestamp = canonical_job["timestamp"]
        safe_filename = canonical_job["safe_filename"]

        try:
            runtime_config = canonical_job["runtime_config"]
            target_format = runtime_config["target_format"]

            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            canonical_bytes = _s3_get_bytes(
                s3_hook,
                CANONICAL_BUCKET_NAME,
                canonical_s3_key,
            )
            canonical_payload = json.loads(canonical_bytes.decode("utf-8"))
            route = canonical_payload.get("route")

            engine_mode = str(runtime_config.get("engine_mode", "generic")).lower()

            if engine_mode == "custom":
                handler_module_path = runtime_config.get("handler_module_path")
                handler_class = runtime_config.get("handler_class")

                if not handler_module_path or not handler_class:
                    raise ValueError(
                        "Integración custom requiere handler_module_path y handler_class."
                    )

                handler_instance = load_handler_instance(
                    handler_module_path,
                    handler_class,
                )

                if not hasattr(handler_instance, "build_out"):
                    raise ValueError(
                        f"El handler custom {handler_class} no implementa build_out."
                    )

                # ==========================================================
                # MULTI OUT SUPPORT
                # ==========================================================
                # Caso estándar: el handler regresa 1 payload.
                # Caso multi: el handler puede regresar:
                #   {"outputs": [{"filename_suffix": "...", "payload": {...}}, ...]}
                #
                # Compatibilidad específica 1037:
                # Si canonical trae varias pólizas y el handler expone _build_sap_payload,
                # generamos 1 OUT por póliza aunque build_out legacy solo genere polizas[0].
                if (
                    integration_id == "1037_VIATICOS_AMEX"
                    and route != "ECC"
                    and isinstance(canonical_payload.get("polizas"), list)
                    and len(canonical_payload.get("polizas") or []) > 1
                    and hasattr(handler_instance, "_build_sap_payload")
                ):
                    final_payload = {
                        "outputs": [
                            {
                                "filename_suffix": str(poliza.get("IDEXT") or index + 1),
                                "payload": handler_instance._build_sap_payload(
                                    poliza=poliza,
                                    canonical_payload=canonical_payload,
                                ),
                            }
                            for index, poliza in enumerate(canonical_payload.get("polizas") or [])
                        ]
                    }
                else:
                    final_payload = handler_instance.build_out(
                        canonical_payload=canonical_payload,
                        integration_config=runtime_config,
                        canonical_job=canonical_job,
                        global_context={
                            "postgres_conn_id": POSTGRES_CONN_ID,
                            "aws_conn_id": AWS_CONN_ID,
                            "raw_bucket_name": RAW_BUCKET_NAME,
                            "canonical_bucket_name": CANONICAL_BUCKET_NAME,
                            "out_bucket_name": OUT_BUCKET_NAME,
                        },
                    )

            else:
                final_payload = build_final_payloads_from_canonical(
                    canonical_payloads=canonical_payload,
                    integration_config=runtime_config,
                )

            # ==========================================================
            # NORMALIZACIÓN DE SALIDAS
            # ==========================================================
            # Single output:
            #   final_payload = {...}
            #
            # Multi output:
            #   final_payload = {
            #       "outputs": [
            #           {"filename_suffix": "AXM...", "payload": {...}},
            #           ...
            #       ]
            #   }
            if isinstance(final_payload, dict) and isinstance(final_payload.get("outputs"), list):
                output_items = final_payload["outputs"]
            else:
                output_items = [
                    {
                        "filename_suffix": None,
                        "payload": final_payload,
                    }
                ]

            out_s3_keys = []
            out_s3_send_map = {}

            for index, output_item in enumerate(output_items, start=1):
                if isinstance(output_item, dict) and "payload" in output_item:
                    payload_to_store = output_item.get("payload")
                    filename_suffix = output_item.get("filename_suffix")
                    output_target_format = str(
                        output_item.get("target_format") or target_format
                    ).strip().lower()
                    output_folder = str(output_item.get("folder") or "out").strip()
                    output_send = bool(output_item.get("send", True))
                else:
                    payload_to_store = output_item
                    filename_suffix = None
                    output_target_format = str(target_format).strip().lower()
                    output_folder = "out"
                    output_send = True

                output_target_format = output_target_format.lstrip(".") or str(target_format).strip().lower()
                output_folder = output_folder.strip("/") or "out"

                filename_suffix = filename_suffix or str(index).zfill(3)
                suffix_clean = (
                    str(filename_suffix)
                    .strip()
                    .replace(" ", "_")
                    .replace("/", "_")
                    .replace("\\", "_")
                )

                if len(output_items) == 1 and not (
                    isinstance(output_item, dict) and output_item.get("filename_suffix")
                ):
                    output_filename = (
                        f"{runtime_config['output_filename_prefix']}_{safe_filename}_{timestamp}.{output_target_format}"
                    )
                else:
                    output_filename = (
                        f"{runtime_config['output_filename_prefix']}_{safe_filename}_{suffix_clean}_{timestamp}.{output_target_format}"
                    )

                out_s3_key = f"{integration_id}/{output_folder}/{output_filename}"
                out_bytes = _payload_to_bytes(payload_to_store)

                if output_target_format == "json":
                    content_type = "application/json"
                elif output_target_format == "txt":
                    content_type = "text/plain; charset=utf-8"
                else:
                    content_type = "application/octet-stream"

                _s3_put_bytes(
                    s3_hook=s3_hook,
                    bucket_name=OUT_BUCKET_NAME,
                    key=out_s3_key,
                    payload=out_bytes,
                    content_type=content_type,
                )

                out_s3_keys.append(out_s3_key)
                out_s3_send_map[out_s3_key] = output_send

                safe_log_integration_event(
                    postgres_conn_id=POSTGRES_CONN_ID,
                    dag_id=meta["dag_id"],
                    run_id=meta["run_id"],
                    task_id=meta["task_id"],
                    integration_id=integration_id,
                    phase="OUT",
                    status="SUCCESS",
                    source_file=source_file,
                    canonical_s3_key=canonical_s3_key,
                    out_s3_key=out_s3_key,
                    message=(
                        "Salida final generada y guardada en OUT"
                        if len(output_items) == 1
                        else f"Salida final {index}/{len(output_items)} generada y guardada en OUT"
                    ),
                )

            return {
                **canonical_job,
                "out_s3_key": out_s3_keys[0] if out_s3_keys else None,
                "out_s3_keys": out_s3_keys,
                "out_count": len(out_s3_keys),
                "out_s3_send_map": out_s3_send_map,
                "route": route,
            }

        except Exception as exc:
            safe_log_integration_event(
                postgres_conn_id=POSTGRES_CONN_ID,
                dag_id=meta["dag_id"],
                run_id=meta["run_id"],
                task_id=meta["task_id"],
                integration_id=integration_id,
                phase="OUT",
                status="FAILED",
                source_file=source_file,
                canonical_s3_key=canonical_s3_key,
                message="Error en fase OUT",
                error_message=str(exc),
            )
            raise

    @task
    def send_to_destination(out_job: dict) -> dict:
        meta = _ctx()
        runtime_config = out_job["runtime_config"]
        destination_config = runtime_config["target_config"]

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        out_s3_keys = out_job.get("out_s3_keys") or [out_job.get("out_s3_key")]
        out_s3_keys = [key for key in out_s3_keys if key]

        if not out_s3_keys:
            raise ValueError("No hay out_s3_key/out_s3_keys para enviar.")

        send_results = []
        out_s3_send_map = out_job.get("out_s3_send_map") or {}

        for index, out_s3_key in enumerate(out_s3_keys, start=1):
            should_send = out_s3_send_map.get(out_s3_key, True)

            if (
                should_send is False
                or "/txt/" in out_s3_key.lower()
                or out_s3_key.lower().endswith(".txt")
                or "_TXT_" in out_s3_key
            ):
                logging.info("Output de evidencia TXT. No se enviará: %s", out_s3_key)
                continue

            current_out_job = {
                **out_job,
                "out_s3_key": out_s3_key,
            }

            try:
                out_bytes = _s3_get_bytes(
                    s3_hook,
                    OUT_BUCKET_NAME,
                    out_s3_key,
                )

                source_file = (
                    current_out_job.get("filename")
                    or current_out_job.get("source_file")
                    or out_s3_key.split("/")[-1]
                )

                send_kwargs = {
                    "output_filename": out_s3_key.split("/")[-1],
                }

                # ==========================================================
                # TARGET ROUTING
                # ==========================================================
                if destination_config.get("type") == "routing":
                    try:
                        route_payload = json.loads(out_bytes.decode("utf-8"))
                    except Exception as route_decode_exc:
                        raise ValueError(
                            "El target_config type=routing requiere que el OUT sea JSON "
                            "para poder leer el campo route."
                        ) from route_decode_exc

                    route_field = destination_config.get("route_field", "route")
                    route_value = current_out_job.get("route") or route_payload.get(route_field)

                    if not route_value:
                        raise ValueError(
                            f"No se encontró el campo de ruteo '{route_field}' en el OUT ni en out_job."
                        )

                    routes_config = destination_config.get("routes") or {}
                    selected_route_config = routes_config.get(route_value)

                    if not selected_route_config:
                        raise ValueError(
                            f"No existe configuración de ruta para route={route_value}. "
                            f"Rutas disponibles: {list(routes_config.keys())}"
                        )

                    handler_module = selected_route_config.get("handler_module")
                    handler_class = selected_route_config.get("handler_class")
                    selected_config = selected_route_config.get("config") or {}

                    if not handler_module or not handler_class:
                        raise ValueError(
                            f"La configuración de ruta {route_value} requiere "
                            "handler_module y handler_class."
                        )

                    sender_instance = load_handler_instance(
                        handler_module,
                        handler_class,
                    )

                    route_out_bytes = out_bytes

                    if route_value == "ECC" and "payload_text" in route_payload:
                        route_out_bytes = str(route_payload["payload_text"]).encode("utf-8")

                    elif "payload" in route_payload and selected_route_config.get("use_payload_field"):
                        route_out_bytes = _payload_to_bytes(route_payload["payload"])

                    result_info = sender_instance.send(
                        route_out_bytes,
                        selected_config,
                        **send_kwargs,
                    )

                    handler_class_for_log = handler_class

                else:
                    handler_module = destination_config.get("handler_module")
                    handler_class = destination_config.get("handler_class")

                    if not handler_module or not handler_class:
                        raise ValueError(
                            "La 'target_config' debe contener 'handler_module' y 'handler_class'."
                        )

                    sender_instance = load_handler_instance(handler_module, handler_class)

                    result_info = sender_instance.send(
                        out_bytes,
                        destination_config,
                        **send_kwargs,
                    )

                    handler_class_for_log = handler_class

                send_results.append(
                    {
                        "out_s3_key": out_s3_key,
                        "result": result_info,
                    }
                )

                safe_log_integration_event(
                    postgres_conn_id=POSTGRES_CONN_ID,
                    dag_id=meta["dag_id"],
                    run_id=meta["run_id"],
                    task_id=meta["task_id"],
                    integration_id=current_out_job["integration_id"],
                    phase="SEND",
                    status="SUCCESS",
                    source_file=current_out_job["source_file"],
                    out_s3_key=out_s3_key,
                    message=(
                        f"Archivo enviado con éxito usando handler: {handler_class_for_log}"
                        if len(out_s3_keys) == 1
                        else f"Archivo {index}/{len(out_s3_keys)} enviado con éxito usando handler: {handler_class_for_log}"
                    ),
                    send_details=json.dumps(result_info),
                )

            except HttpSenderError as sender_err:
                logging.error("Error de envío capturado por el handler: HttpSenderError")

                error_response_json = json.dumps(
                    {
                        "url": sender_err.target,
                        "status_code": sender_err.status_code,
                        "response": str(sender_err.response_text),
                    }
                )

                safe_log_integration_event(
                    postgres_conn_id=POSTGRES_CONN_ID,
                    dag_id=meta["dag_id"],
                    run_id=meta["run_id"],
                    task_id=meta["task_id"],
                    integration_id=current_out_job["integration_id"],
                    phase="SEND",
                    status="FAILED",
                    source_file=current_out_job["source_file"],
                    out_s3_key=out_s3_key,
                    message=(
                        "Error en fase SEND reportado por el handler"
                        if len(out_s3_keys) == 1
                        else f"Error en fase SEND para archivo {index}/{len(out_s3_keys)}"
                    ),
                    error_message=str(sender_err),
                    send_details=error_response_json,
                )
                raise

            except Exception as exc:
                safe_log_integration_event(
                    postgres_conn_id=POSTGRES_CONN_ID,
                    dag_id=meta["dag_id"],
                    run_id=meta["run_id"],
                    task_id=meta["task_id"],
                    integration_id=current_out_job["integration_id"],
                    phase="SEND",
                    status="FAILED",
                    source_file=current_out_job["source_file"],
                    out_s3_key=out_s3_key,
                    message=(
                        "Error inesperado en fase SEND"
                        if len(out_s3_keys) == 1
                        else f"Error inesperado en fase SEND para archivo {index}/{len(out_s3_keys)}"
                    ),
                    error_message=str(exc),
                    send_details=None,
                )
                raise

        return {
            **out_job,
            "send_results": send_results,
            "send_count": len(send_results),
        }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify_validation_issues():
        meta = _ctx()
        try:
            sent = send_pending_validation_notifications(
                postgres_conn_id=POSTGRES_CONN_ID,
                dag_id=meta["dag_id"],
                run_id=meta["run_id"],
            )
            if sent:
                safe_log_integration_event(
                    postgres_conn_id=POSTGRES_CONN_ID,
                    dag_id=meta["dag_id"],
                    run_id=meta["run_id"],
                    task_id=meta["task_id"],
                    integration_id=None,
                    phase="NOTIFY",
                    status="SUCCESS",
                    source_file="__RUN__",
                    message=f"Notificaciones enviadas: {sent}",
                )
            logging.info("Notificaciones enviadas: %s", sent)

        except Exception as exc:
            safe_log_integration_event(
                postgres_conn_id=POSTGRES_CONN_ID,
                dag_id=meta["dag_id"],
                run_id=meta["run_id"],
                task_id=meta["task_id"],
                integration_id=None,
                phase="NOTIFY",
                status="FAILED",
                source_file="__RUN__",
                message="Error enviando notificaciones",
                error_message=str(exc),
            )
            logging.exception("Falló notify_validation_issues: %s", exc)

    @task
    def mark_as_processed(sent_job: dict):
        order_number = sent_job.get("order_number")
        if not order_number:
            return

        meta = _ctx()
        safe_log_processed_order_event(
            postgres_conn_id=POSTGRES_CONN_ID,
            order_number=order_number,
            integration_id=sent_job["integration_id"],
            source_file=sent_job["source_file"],
            target_file=sent_job["out_s3_key"],
            run_id=meta["run_id"],
        )

    @task_group(group_id="process_one_integration")
    def process_one_integration(job: dict):
        canonical_job = build_and_store_canonical(job)
        out_job = build_and_store_out(canonical_job)
        sent_job = send_to_destination(out_job)
        mark_as_processed(sent_job)

    staged_jobs = discover_and_stage_files()
    mapped = process_one_integration.expand(job=staged_jobs)
    notify = notify_validation_issues()

    staged_jobs >> notify
    mapped >> notify