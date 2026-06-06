from airflow import DAG

from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaConsumer
import json
import os


def consume_kafka():

    consumer = KafkaConsumer(
        'TP_INT_DISP_ZARMAS10', #topico
        bootstrap_servers=[
           'b-2.oneaxokafkaqa.5ha5aj.c20.kafka.us-east-1.amazonaws.com:9096',
		   'b-3.oneaxokafkaqa.5ha5aj.c20.kafka.us-east-1.amazonaws.com:9096',
		   'b-1.oneaxokafkaqa.5ha5aj.c20.kafka.us-east-1.amazonaws.com:9096'
        ],
        group_id='GRP_TP_INT_DISP_ZARMAS10_TEST_V1', # G de consumidores
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        security_protocol='SASL_SSL',
        sasl_mechanism='SCRAM-SHA-512',
        sasl_plain_username='kafkauser',
        sasl_plain_password='kafkauser',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    print(" Conectado a Kafka ,todo bien")

    messages = consumer.poll(timeout_ms=15000)
    total = 0

    file_path = "/opt/airflow/data/mensajes_dlq.json"

    for tp, msgs in messages.items():
        for msg in msgs:
            total += 1

            try:
                # Parse JSON principal
                data = json.loads(msg.value)

                # Parse JSON interno si existe
                if "inboundcontent" in data:
                    try:
                        data["inboundcontent_parsed"] = json.loads(data["inboundcontent"])
                    except:
                        data["inboundcontent_parsed"] = "No se pudo parsear"

                # Agregar metadata
                output_record = {
                    "timestamp_consumo": datetime.utcnow().isoformat(),
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "data": data
                }

                # Guardar en archivo (append)
                with open(file_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(output_record, ensure_ascii=False) + "\n")

                print(f"✅ Mensaje guardado en archivo (offset {msg.offset})")

            except Exception as e:
                print(f"❌ Error procesando mensaje: {e}")

    consumer.commit()
    consumer.close()

    print(f"✅ Total mensajes leídos: {total}")
    print(f"✅ Archivo destino: {file_path}")


with DAG(
    dag_id='kafka_dlq_consumer_test',
    start_date=datetime(2024, 1, 1),
    schedule='*/5 * * * *',
    catchup=False,
    tags=['kafka']
) as dag:

    consume_task = PythonOperator(
        task_id='consume_kafka',
        python_callable=consume_kafka
    )