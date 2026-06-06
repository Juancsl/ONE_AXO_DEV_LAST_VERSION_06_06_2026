import logging
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageTrigger
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.python import get_current_context

AWS_CONN_ID = "one_axo_s3"
POSTGRES_CONN_ID = "gobierno_central_postgres"

RAW_BUCKET_NAME = "one-axo-raw"
CANONICAL_BUCKET_NAME = "one-axo-canonical"
OUT_BUCKET_NAME = "one-axo-out"

KAFKA_CONFIG = Variable.get("kafka_config")
KAFKA_TOPIC = "TP_INT_DISP_DEBMAS07"
KAFKA_GROUP = "GRP_TP_INT_DISP_DEBMAS07_EPO_ORC"

def _ctx():
    context = get_current_context()
    dag_run = context.get("dag_run")
    return {
        "dag_id": context["dag"].dag_id,
        "run_id": dag_run.run_id if dag_run else None,
        "task_id": context["task"].task_id,
    }

def _s3_put_bytes(s3_hook: S3Hook, bucket_name: str, key: str, payload: bytes, content_type: str | None = None):
    extra = {"ContentType": content_type} if content_type else {}
    s3_hook.get_conn().put_object(Bucket=bucket_name, Key=key, Body=payload, **extra)


def _s3_get_bytes(s3_hook: S3Hook, bucket_name: str, key: str) -> bytes:
    response = s3_hook.get_conn().get_object(Bucket=bucket_name, Key=key)


@dag(
    dag_id="KAFKA_EVENT_DRIVEN_PIPELINE",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kafka", "event-driven", "generic"],
)

def kafka_event_driven_pipeline():

    kafka_sensor = AwaitMessageTrigger(
        task_id="wait_for_kafka_message",
        kafka_config_id=KAFKA_CONFIG,
        topics=[KAFKA_TOPIC],
        apply_function="json.loads",
        event_trigger_config={"messages": 1, "max_wait_time": 300},
    )

    @task
    def prepare_jobs_from_kafka(messages: list) -> list[dict]:
        """
        Tarea "adaptadora". Toma los mensajes crudos de Kafka, los sube a S3 Raw
        y los transforma en el formato de "job" que la lógica de procesamiento espera.
        """
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        prepared_jobs = []
        
        if not messages:
            logging.info("No se recibieron mensajes de Kafka en este lote.")
            return []

        for msg in messages:
            try:
                # Asumimos que el 'integration_id' viene como la clave del mensaje de Kafka
                integration_id = msg.get("key")
                if not integration_id:
                    logging.warning(f"Mensaje de Kafka recibido sin 'integration_id' en la clave. Saltando. Mensaje: {msg}")
                    continue

                payload_bytes = msg.get("value").encode('utf-8')
                
                # --- Lógica de "Stage" ---
                # 1. Guardamos el payload del mensaje en S3 Raw, imitando la fase de staging
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                kafka_offset = msg.get("offset")
                filename = f"from_kafka_{integration_id}_{kafka_offset}.json"
                raw_s3_key = f"staged/kafka/{integration_id}/{timestamp}_{filename}"
                _s3_put_bytes(s3_hook, RAW_BUCKET_NAME, raw_s3_key, payload_bytes)
                
                # 2. Construimos el diccionario "job" que el task_group espera
                job = {
                    "integration_id": integration_id,
                    "endpoint_id": f"kafka_{KAFKA_TOPIC}", # Identificador del origen
                    "source_file": f"kafka://{KAFKA_TOPIC}/partition/{msg.get('partition')}/offset/{kafka_offset}",
                    "filename": filename,
                    "raw_s3_key": raw_s3_key,
                    "timestamp": timestamp,
                }
                prepared_jobs.append(job)
            except Exception as e:
                logging.error(f"Error preparando el job desde el mensaje de Kafka: {msg}. Error: {e}")
        
        return prepared_jobs
    
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

    @task_group(group_id="process_one_integration")
    def process_one_integration(job: dict):
        canonical_job = build_and_store_canonical(job)
        out_job = build_and_store_out(canonical_job)
        send_to_destination(out_job)

     # --- FLUJO DEL DAG ---
    prepared_jobs = prepare_jobs_from_kafka(kafka_sensor.output)
    processed = process_one_integration.expand(job=prepared_jobs)
