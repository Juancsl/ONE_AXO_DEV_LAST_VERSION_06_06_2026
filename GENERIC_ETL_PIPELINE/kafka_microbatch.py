# Archivo: dags/kafka_to_s3_microbatch.py

import json
import logging
from datetime import datetime
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable, Connection
from airflow.utils.session import provide_session

# --- Configuración ---
KAFKA_CONN_ID = "kafka_default_programmatic" # Usamos un nuevo ID para no confundir
KAFKA_CONFIG_VAR_NAME = "kafka_connection_config"
KAFKA_TOPIC = "TP_INT_DISP_WBBDLD07"

AWS_CONN_ID = "one_axo_s3"
S3_BUCKET_NAME = "one-axo-raw"  # Usando el bucket de tu framework [2]

def save_batch_to_s3(messages: list, **kwargs):
    """
    Función que se aplica a un LOTE de mensajes de Kafka.
    Los agrupa todos y los guarda como un único archivo en S3.
    """
    if not messages:
        logging.info("No se recibieron mensajes en este lote.")
        return

    logging.info(f"Procesando un lote de {len(messages)} mensajes.")
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    # Extraemos el contenido de cada mensaje.
    # Asumimos que el valor del mensaje es un string JSON.
    message_contents = []
    for msg in messages:
        try:
            # El valor del mensaje viene en bytes, lo decodificamos a un string
            # y luego lo parseamos como JSON.
            message_contents.append(json.loads(msg.value().decode('utf-8')))
        except Exception as e:
            logging.warning(f"No se pudo decodificar un mensaje, se omitirá. Error: {e}")

    if not message_contents:
        logging.info("No hay contenido válido para guardar después de decodificar.")
        return

    # Convertimos la lista completa de diccionarios a un solo string JSON.
    output_string = json.dumps(message_contents, indent=4)
    
    # Creamos un nombre de archivo único para el lote, usando los offsets.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    first_offset = messages[0].offset()
    last_offset = messages[-1].offset()
    s3_key = f"from_kafka/{KAFKA_TOPIC}/{timestamp}_offsets_{first_offset}_to_{last_offset}.json"
    
    # Guardamos el archivo en S3.
    s3_hook.load_string(
        string_data=output_string,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True
    )
    logging.info(f"Lote de {len(messages)} mensajes guardado en s3://{S3_BUCKET_NAME}/{s3_key}")

@provide_session
def create_kafka_connection_from_variable(conn_id: str, var_name: str, session=None):
    """
    Verifica si una conexión de Airflow existe. Si no, la crea
    utilizando la configuración almacenada en una Variable de Airflow.
    """
    # 1. Verifica si la conexión ya existe en la base de datos de Airflow
    connection_exists = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if connection_exists:
        logging.info(f"La conexión '{conn_id}' ya existe. No se realizarán cambios.")
        return

    logging.info(f"La conexión '{conn_id}' no existe. Creándola desde la variable '{var_name}'...")
    
    # 2. Si no existe, lee la configuración desde la Variable
    try:
        kafka_config_dict = Variable.get(var_name, deserialize_json=True)
    except Exception as e:
        logging.error(f"No se pudo leer o deserializar la Variable de Airflow '{var_name}'. Error: {e}")
        raise

    # 3. Crea el nuevo objeto de Conexión
    new_conn = Connection(
        conn_id=conn_id,
        conn_type='kafka', # El tipo debe ser 'kafka'
        extra=json.dumps(kafka_config_dict) # La configuración se guarda en el campo 'extra'
    )
    
    # 4. Añade y guarda la nueva conexión en la base de datos de Airflow
    session.add(new_conn)
    session.commit()
    logging.info(f"Conexión '{conn_id}' creada exitosamente.")


with DAG(
    dag_id="KAFKA_TO_S3_MICROBATCH",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",  # Ejecutar cada 5 minutos para procesar en micro-lotes.
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "s3", "micro-batch"],
) as dag:
    
    def kafka_to_s3_microbatch_dag():
        """
        Este DAG consume mensajes de Kafka en micro-lotes y los guarda
        como archivos JSON consolidados en S3.
        """
        create_kafka_connection_from_variable(KAFKA_CONN_ID, KAFKA_CONFIG_VAR_NAME)
        ConsumeFromTopicOperator(
            task_id="consume_from_kafka_and_save_to_s3",
            kafka_config_id=KAFKA_CONN_ID,
            topics=[KAFKA_TOPIC],
            # Usamos apply_function_batch para procesar lotes, es más eficiente.
            apply_function_batch=save_batch_to_s3,
            # Configuración del consumidor para el lote.
            max_messages=1000,       # Máximo de mensajes a consumir por cada ejecución del DAG.
            max_batch_size=200,      # Máximo de mensajes a pasar a la función en cada lote [18].
            commit_cadence="end_of_batch", # Confirma el offset después de que el lote se guarda en S3, para mayor seguridad.
        )

    # Instanciamos el DAG
    kafka_to_s3_microbatch_dag()