# Archivo: dags/kafka_sensor_to_s3.py

import json
import logging
from datetime import datetime
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- Configuración ---
KAFKA_CONN_ID = "kafka_default"
KAFKA_TOPIC = "TP_INT_DISP_WBBDLD07"

AWS_CONN_ID = "one_axo_s3"
S3_BUCKET_NAME = "one-axo-raw"

# 1. Función de Aplicación para el Sensor
# Esta función define qué constituye un "mensaje de interés".
# Debe estar en una ruta importable para que Airflow la encuentre.
def get_message_if_present(message) -> str | None:
    """
    Función que el AwaitMessageSensor usará para evaluar cada mensaje.
    Si el mensaje es válido, devuelve su contenido para ser pusheado a XCom.
    Si no, devuelve None y el sensor seguirá esperando.
    """
    if message:
        # Decodificamos el valor del mensaje (que viene en bytes) a un string.
        return message.value().decode('utf-8')
    return None

with DAG(
    dag_id="KAFKA_SENSOR_TO_S3",
    start_date=datetime(2024, 1, 1),
    schedule="*/2 * * * *",  # Se ejecuta cada 2 minutos para buscar un mensaje.
    catchup=False,
    max_active_runs=1, # Evita que múltiples ejecuciones compitan por el mismo mensaje.
    tags=["kafka", "sensor", "s3"],
) as dag:
    
    def kafka_sensor_to_s3_dag():
        """
        Este DAG utiliza un AwaitMessageSensor para esperar un mensaje de un tópico
        de Kafka y, cuando lo encuentra, lo guarda en un archivo en S3.
        """

        # 2. La Tarea del Sensor
        # Esta tarea se conecta a Kafka y espera hasta que `get_message_if_present`
        # devuelva algo que no sea None.
        wait_for_single_message = AwaitMessageSensor(
            task_id="wait_for_single_kafka_message",
            kafka_config_id="kafka_default",
            topics=["TP_INT_DISP_WBBDLD07"],
            apply_function="dags.kafka_sensor_to_s3.get_message_if_present",
            xcom_push_key="retrieved_message",
        )

        @task
        def save_message_to_s3(message_content: str | None):
            """
            Tarea que se ejecuta después de que el sensor tiene éxito.
            Recibe el contenido del mensaje desde XCom.
            """
            if not message_content:
                logging.info("El sensor no encontró ningún mensaje dentro del timeout. Finalizando.")
                return

            logging.info("Mensaje recibido, guardando en S3.")
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

            try:
                # Para mantener la consistencia, intentamos parsear el JSON
                # y guardarlo con formato "pretty".
                data = json.loads(message_content)
                output_string = json.dumps(data, indent=4)
            except json.JSONDecodeError:
                # Si no es un JSON válido, lo guardamos tal cual.
                output_string = message_content

            # Creamos un nombre de archivo único
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"from_kafka_sensor/{KAFKA_TOPIC}/{timestamp}.json"

            s3_hook.load_string(
                string_data=output_string,
                key=s3_key,
                bucket_name=S3_BUCKET_NAME,
                replace=True
            )
            logging.info(f"Mensaje individual guardado en s3://{S3_BUCKET_NAME}/{s3_key}")

        # 3. Flujo del DAG
        # El contenido del mensaje encontrado por el sensor se pasa
        # automáticamente a la siguiente tarea a través de XComs.
        save_message_to_s3(wait_for_single_message.output)


    # Instanciamos el DAG
    kafka_sensor_to_s3_dag()