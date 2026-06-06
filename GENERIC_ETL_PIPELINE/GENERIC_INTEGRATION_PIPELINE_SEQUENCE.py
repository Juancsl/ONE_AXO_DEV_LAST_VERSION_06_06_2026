# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from io import BytesIO
from collections import defaultdict

from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.db_logger import (safe_log_integration_event, safe_log_processed_order_event)
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.exceptions import (ValidationRejectFileError,HttpSenderError)
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

INTEGRATIONS_TO_RUN = ["DHL_R263", "R047-A-ITEM"]

AWS_CONN_ID = "one_axo_s3"
POSTGRES_CONN_ID = "gobierno_central_postgres"

RAW_BUCKET_NAME = "one-axo-raw"
CANONICAL_BUCKET_NAME = "one-axo-canonical"
OUT_BUCKET_NAME = "one-axo-out"

_val = Variable.get("schedule_generic_pipeline", default_var="*/5 * * * *").strip().lower()
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


def _s3_put_bytes(s3_hook: S3Hook, bucket_name: str, key: str, payload: bytes, content_type: str | None = None):
    extra = {"ContentType": content_type} if content_type else {}
    s3_hook.get_conn().put_object(Bucket=bucket_name, Key=key, Body=payload, **extra)


def _s3_get_bytes(s3_hook: S3Hook, bucket_name: str, key: str) -> bytes:
    response = s3_hook.get_conn().get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read()


with DAG(
    dag_id="GENERIC_INTEGRATION_PIPELINE_SEQUENCE",
    start_date=datetime(2024, 1, 1),
    schedule=SCHEDULE_UI,
    catchup=CATCHUP_UI,
    tags=["etl", "generic"],
) as dag:

    @task
    def discover_and_stage_files() -> list[dict]:
        """
        Tarea orquestadora. Llama a los Handlers de Origen configurados, quienes
        se encargan de descubrir, poner en escena y generar los jobs.
        """
        meta = _ctx()
        # 1. Cargar todos los endpoints de origen activos
        source_endpoints = load_active_source_endpoints(POSTGRES_CONN_ID,INTEGRATIONS_TO_RUN) # Carga de la función en integration_loader.py [5]
        all_jobs = []

        for endpoint in source_endpoints:
            source_config = endpoint['source_config']
            handler_module = source_config.get('handler_module')
            handler_class = source_config.get('handler_class')

            if not handler_module or not handler_class:
                logging.warning(f"Endpoint {endpoint['endpoint_id']} no tiene un handler de origen definido. Saltando.")
                continue
            
            try:
                # 2. Preparamos un contexto global con las dependencias que el handler necesita
                global_context = {
                    "aws_conn_id": AWS_CONN_ID,
                    "postgres_conn_id": POSTGRES_CONN_ID,
                    "raw_bucket_name": RAW_BUCKET_NAME,
                    "endpoint_id": endpoint['endpoint_id'],
                    # Pasamos referencias a las funciones de utilidad del DAG para que el handler las use
                    "_s3_put_bytes": _s3_put_bytes,
                    "_safe_stem": _safe_stem,
                    # También podrías pasar 'safe_log_integration_event' si quieres que el handler loguee
                }

                # 3. Instanciamos y ejecutamos el handler
                handler_instance = load_handler_instance(handler_module, handler_class, config=source_config, global_context=global_context) # Usa el cargador dinámico [12]

                discovered_jobs = handler_instance.discover()

                # 4. Recolectamos los jobs generados por el handler
                if discovered_jobs:
                    all_jobs.extend(discovered_jobs)
                    for job in discovered_jobs:
                        # El log de éxito se puede hacer aquí, de forma centralizada
                        safe_log_integration_event(postgres_conn_id=POSTGRES_CONN_ID, dag_id=meta["dag_id"], run_id=meta["run_id"], task_id=meta["task_id"], integration_id=job["integration_id"], phase="DISCOVER", status="SUCCESS", source_file=job["source_file"], raw_s3_key=job["raw_s3_key"], message="Job creado a partir de archivo en staging.")

            except Exception as handler_exc:
                logging.error(f"Fallo el handler {handler_class} para el endpoint {endpoint['endpoint_id']}: {handler_exc}")
                safe_log_integration_event(postgres_conn_id=POSTGRES_CONN_ID, dag_id=meta["dag_id"], run_id=meta["run_id"], task_id=meta["task_id"], integration_id=None, phase="DISCOVER", status="FAILED", message=f"Handler {handler_class} falló.", error_message=str(handler_exc))

        return all_jobs

    @task
    def classify_jobs(staged_jobs: list[dict]) -> dict:
        """
        Clasifica los trabajos en 'independientes' y 'secuenciales'
        basado en la configuración de la integración.
        """
        independent_jobs = []
        # Usamos un defaultdict para agrupar automáticamente los jobs por su delivery_group
        sequenced_jobs_by_group = defaultdict(list)

        for job in staged_jobs:
            # Cargamos la configuración para leer los nuevos campos
            runtime_config = load_runtime_config_by_integration_id(POSTGRES_CONN_ID, job["integration_id"])
            job["runtime_config"] = runtime_config

            delivery_group = runtime_config.get("delivery_group")

            if delivery_group:
                sequenced_jobs_by_group[delivery_group].append(job)
            else:
                independent_jobs.append(job)

        return {
            "independent": independent_jobs,
            # Convertimos el defaultdict a una lista de listas de trabajos
            "sequenced": list(sequenced_jobs_by_group.values())
        }

    @task
    def flatten_sequenced_jobs(classified: dict) -> list[dict]:
        """Toma la lista de grupos y la aplana en una sola lista de jobs."""
        flat_list = []
        for group in classified.get("sequenced", []):
            flat_list.extend(group)
        return flat_list

    @task
    def regroup_processed_jobs(processed_jobs: list[dict]) -> list[list[dict]]:
        """
        Toma la lista plana de resultados y la reagrupa por 'delivery_group'
        para la tarea de envío secuencial.
        """
        if not processed_jobs:
            return []

        groups = defaultdict(list)
        for job in processed_jobs:
            # Es crucial que 'delivery_group' se pase a través de las tareas de procesamiento.
            delivery_group = job.get("runtime_config", {}).get("delivery_group")
            if delivery_group:
                groups[delivery_group].append(job)

        return list(groups.values())

    #@task(
    #    retries=3,
    #    retry_delay=timedelta(minutes=5)
    #)
    #def download_and_store_raw(job: dict) -> dict:
    #    meta = _ctx()
#
    #    endpoint_id = job["endpoint_id"]
    #    integration_id = job["integration_id"]
    #    filename = job["filename"]
    #    remote_full_path = job["remote_full_path"]
    #    safe_filename = _safe_stem(filename)
#
    #    sftp_hook = SFTPHook(ssh_conn_id=job["sftp_conn_id"])
    #    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    #    sftp_client = None
#
    #    try:
    #        runtime_config = load_runtime_config_by_endpoint_id(
    #            postgres_conn_id=POSTGRES_CONN_ID,
    #            endpoint_id=endpoint_id,
    #        )
#
    #        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #        raw_s3_key = f"{integration_id}/raw/{timestamp}_{filename}"
#
    #        sftp_client = sftp_hook.get_conn()
    #        buffer = BytesIO()
    #        sftp_client.getfo(remote_full_path, buffer)
    #        raw_bytes = buffer.getvalue()
#
    #        _s3_put_bytes(
    #            s3_hook=s3_hook,
    #            bucket_name=RAW_BUCKET_NAME,
    #            key=raw_s3_key,
    #            payload=raw_bytes,
    #            content_type="application/octet-stream",
    #        )
#
    #        sftp_client.remove(remote_full_path)
#
    #        safe_log_integration_event(
    #            postgres_conn_id=POSTGRES_CONN_ID,
    #            dag_id=meta["dag_id"],
    #            run_id=meta["run_id"],
    #            task_id=meta["task_id"],
    #            integration_id=integration_id,
    #            phase="RAW",
    #            status="SUCCESS",
    #            source_file=remote_full_path,
    #            raw_s3_key=raw_s3_key,
    #            message="Archivo guardado en RAW y borrado del SFTP",
    #        )
#
    #        return {
    #            **job,
    #            "source_file": remote_full_path,
    #            "raw_s3_key": raw_s3_key,
    #            "timestamp": timestamp,
    #            "safe_filename": safe_filename,
    #            "runtime_config": runtime_config,
    #        }
#
    #    except Exception as exc:
    #        safe_log_integration_event(
    #            postgres_conn_id=POSTGRES_CONN_ID,
    #            dag_id=meta["dag_id"],
    #            run_id=meta["run_id"],
    #            task_id=meta["task_id"],
    #            integration_id=integration_id,
    #            phase="RAW",
    #            status="FAILED",
    #            source_file=remote_full_path,
    #            message="Error en fase RAW",
    #            error_message=str(exc),
    #        )
    #        raise
    #    finally:
    #        if sftp_client is not None:
    #            sftp_client.close()

    @task
    def build_and_store_canonical(raw_job: dict) -> dict:
        meta = _ctx()

        endpoint_id = raw_job["endpoint_id"]
        integration_id = raw_job["integration_id"]
        source_file = raw_job["source_file"]
        raw_s3_key = raw_job["raw_s3_key"]
        timestamp = raw_job["timestamp"]
        safe_filename = raw_job["safe_filename"]

        try:
            runtime_config = load_runtime_config_by_integration_id(POSTGRES_CONN_ID, raw_job["integration_id"])
            raw_job["runtime_config"] = runtime_config
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            raw_bytes = _s3_get_bytes(s3_hook, RAW_BUCKET_NAME, raw_s3_key)

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
                ensure_ascii=False
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

        endpoint_id = canonical_job["endpoint_id"]
        integration_id = canonical_job["integration_id"]
        source_file = canonical_job["source_file"]
        canonical_s3_key = canonical_job["canonical_s3_key"]
        timestamp = canonical_job["timestamp"]
        safe_filename = canonical_job["safe_filename"]

        try:
            runtime_config = canonical_job["runtime_config"]
            target_format = runtime_config["target_format"]

            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            canonical_bytes = _s3_get_bytes(s3_hook, CANONICAL_BUCKET_NAME, canonical_s3_key)
            canonical_payload = json.loads(canonical_bytes.decode("utf-8"))

            final_payload = build_final_payloads_from_canonical(
                canonical_payloads=canonical_payload,
                integration_config=runtime_config,
            )

            output_filename = (
                f"{runtime_config['output_filename_prefix']}_{safe_filename}_{timestamp}.{target_format}"
            )
            out_s3_key = f"{integration_id}/out/{output_filename}"
            
            out_bytes = final_payload

            _s3_put_bytes(
                s3_hook=s3_hook,
                bucket_name=OUT_BUCKET_NAME,
                key=out_s3_key,
                payload=out_bytes,
                content_type="application/json",
            )

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
                message="Salida final generada y guardada en OUT",
            )

            return {
                **canonical_job,
                "out_s3_key": out_s3_key,
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
        # Asumiendo que ya has implementado la refactorización para pasar la config
        runtime_config = out_job["runtime_config"] 
        # Obtenemos la nueva configuración del destino, que es un JSON
        destination_config = runtime_config["target_config"]
        
        handler_module = destination_config.get("handler_module")
        handler_class = destination_config.get("handler_class")

        if not handler_module or not handler_class:
            raise ValueError("La 'target_config' debe contener 'handler_module' y 'handler_class'.")

        try:
            # 1. Cargar el handler dinámicamente usando tu dynamic_loader
            sender_instance = load_handler_instance(handler_module, handler_class)
    
            # 2. Obtener el payload a enviar (lógica común)
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            out_bytes = _s3_get_bytes(s3_hook, OUT_BUCKET_NAME, out_job["out_s3_key"])
    
            # 3. Preparar kwargs para el formateo de la ruta
            # El handler puede usar estos valores si los necesita
            send_kwargs = {
                "output_filename": out_job["out_s3_key"].split("/")[-1]
            }

            # 4. Delegar la lógica de envío al handler
            result_info = sender_instance.send(out_bytes, destination_config, **send_kwargs)
            
            # 5. Loguear el éxito
            safe_log_integration_event(
                postgres_conn_id=POSTGRES_CONN_ID,
                dag_id=meta["dag_id"],
                run_id=meta["run_id"],
                task_id=meta["task_id"],
                integration_id=out_job["integration_id"],
                phase="SEND",
                status="SUCCESS",
                source_file=out_job["source_file"],
                out_s3_key=out_job["out_s3_key"],
                message=f"Archivo enviado con éxito usando handler: {handler_class}",
                send_details = json.dumps(result_info),
            )
            return {**out_job, **result_info}
        
        except HttpSenderError as sender_err:
            logging.error(f"Error de envío capturado por el handler: HttpSenderError")

            error_response_json = json.dumps({
                "url": sender_err.target,
                "status_code": sender_err.status_code,
                "response": str(sender_err.response_text)
            })

            safe_log_integration_event(
                postgres_conn_id=POSTGRES_CONN_ID,
                dag_id=meta["dag_id"],
                run_id=meta["run_id"],
                task_id=meta["task_id"],
                integration_id=out_job["integration_id"],
                phase="SEND",
                status="FAILED",
                source_file=out_job["source_file"],
                out_s3_key=out_job["out_s3_key"],
                message="Error en fase SEND reportado por el handler",
                error_message=str(sender_err),
                send_details=error_response_json
            )
            raise

        except Exception as exc:
            safe_log_integration_event(
                postgres_conn_id=POSTGRES_CONN_ID,
                dag_id=meta["dag_id"],
                run_id=meta["run_id"],
                task_id=meta["task_id"],
                integration_id=out_job["integration_id"],
                phase="SEND",
                status="FAILED",
                source_file=out_job["source_file"],
                out_s3_key=out_job["out_s3_key"],
                message=f"Error inesperado en fase SEND usando handler: {handler_class}",
                error_message=str(exc),
                send_details=None
            )
            raise

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify_validation_issues():
        meta = _ctx()
        try:
            sent = send_pending_validation_notifications(
                postgres_conn_id=POSTGRES_CONN_ID,
                dag_id=meta["dag_id"],
                run_id=meta["run_id"],
            )

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
        """Inserta el número de pedido en la tabla de seguimiento."""
        order_number = sent_job.get("order_number")
        if not order_number:
            return
        meta = _ctx()
        safe_log_processed_order_event(
            postgres_conn_id=POSTGRES_CONN_ID,
            order_number=order_number,
            integration_id=sent_job['integration_id'],
            source_file=sent_job['source_file'],
            target_file=sent_job['out_s3_key'],
            run_id=meta['run_id'],
        )

    @task_group(group_id="process_and_send_independently")
    def process_and_send_independently(job: dict):
        # Este grupo ejecuta el pipeline completo, incluyendo el envío
        canonical_job = build_and_store_canonical(job)
        out_job = build_and_store_out(canonical_job)
        sent_job = send_to_destination(out_job)
        mark_as_processed(sent_job)

    # GRUPO 2: Solo procesamiento para la ruta secuencial
    @task_group(group_id="process_for_sequencing")
    def process_for_sequencing(job: dict) -> dict:
        # Este grupo NO envía. Solo transforma los datos.
        canonical_job = build_and_store_canonical(job)
        out_job = build_and_store_out(canonical_job)
        # Importante: Devuelve el job listo para ser enviado por la tarea secuenciadora
        return out_job

    @task
    def sequenced_sender(job_group: list[dict]):
        """
        Recibe un grupo de trabajos, los ordena, y los envía secuencialmente.
        """
        meta = _ctx()
        if not job_group:
            logging.info("No hay trabajos en este grupo secuencial para enviar.")
            return

        # 1. Ordenar los trabajos usando el 'delivery_order' de su configuración
        sorted_jobs = sorted(job_group, key=lambda j: j.get("runtime_config", {}).get("delivery_order", 100))
        logging.info(f"Enviando {len(sorted_jobs)} trabajos en orden secuencial.")

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        # 2. Iterar y enviar UNO POR UNO
        for job in sorted_jobs:
            # Esta es la lógica que antes estaba en la tarea 'send_to_destination' [2]
            # pero ejecutada dentro de un bucle.
            target_config = job["runtime_config"]["target_config"]
            handler_module = target_config["handler_module"]
            handler_class = target_config["handler_class"]

            try:
                sender_instance = load_handler_instance(handler_module, handler_class)
                out_bytes = _s3_get_bytes(s3_hook, OUT_BUCKET_NAME, job["out_s3_key"])
                result_info = sender_instance.send(out_bytes, target_config, **job)
                safe_log_integration_event(
                    postgres_conn_id=POSTGRES_CONN_ID,
                    dag_id=meta["dag_id"],
                    run_id=meta["run_id"],
                    task_id=meta["task_id"],
                    integration_id=job["integration_id"],
                    phase="SEND",
                    status="SUCCESS",
                    source_file=job["source_file"],
                    out_s3_key=job["out_s3_key"],
                    message=f"Archivo enviado con éxito usando handler: {handler_class}",
                    send_details = json.dumps(result_info),
                )
                # Marcamos como procesado después de cada envío exitoso
                mark_as_processed.function({**job, **result_info})
            except Exception as e:
                logging.error(f"Fallo al enviar el job para {job['integration_id']}. Abortando secuencia. Error: {e}")
                safe_log_integration_event(
                    postgres_conn_id=POSTGRES_CONN_ID,
                    dag_id=meta["dag_id"],
                    run_id=meta["run_id"],
                    task_id=meta["task_id"],
                    integration_id=job["integration_id"],
                    phase="SEND",
                    status="FAILED",
                    source_file=job["source_file"],
                    out_s3_key=job["out_s3_key"],
                    message=f"Error inesperado en fase SEND usando handler: {handler_class}",
                    error_message=str(e),
                    send_details=None
                )
                raise # Detiene la tarea y marca el DAG como fallido para preservar el orden


    # --- FLUJO PRINCIPAL DEL DAG [2] CORREGIDO ---

    staged_jobs = discover_and_stage_files()
    classified = classify_jobs(staged_jobs)

    join = DummyOperator(task_id="join_paths", trigger_rule="all_done")

    # --- RAMA A: Trabajos Independientes (sin cambios) ---

    has_independent_jobs = ShortCircuitOperator(
        task_id="check_for_independent_jobs",
        python_callable=lambda classified: bool(classified["independent"]),
        op_kwargs={"classified": classified},
    )

    independent_processing = process_and_send_independently.expand(job=classified["independent"])

    has_independent_jobs >> independent_processing >> join

    # --- RAMA B: Trabajos Secuenciales (flujo corregido) ---

    has_sequenced_jobs = ShortCircuitOperator(
        task_id="check_for_sequenced_jobs",
        python_callable=lambda classified: bool(classified["sequenced"]),
        op_kwargs={"classified": classified},
    )

    # 1. Aplanar la lista de trabajos
    flat_sequenced_jobs = flatten_sequenced_jobs(classified)

    # 2. Procesar todos los trabajos aplanados en paralelo
    processed_jobs_flat = process_for_sequencing.expand(job=flat_sequenced_jobs)

    # 3. Reagrupar los resultados por 'delivery_group'
    regrouped_results = regroup_processed_jobs(processed_jobs_flat)

    # 4. Expandir la tarea de envío sobre cada grupo
    sequenced_sending = sequenced_sender.expand(job_group=regrouped_results)

    # Conectamos la rama secuencial
    has_sequenced_jobs >> flat_sequenced_jobs >> processed_jobs_flat >> regrouped_results >> sequenced_sending >> join

    # --- Tarea final de notificación ---
    notify = notify_validation_issues()
    join >> notify