from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import shutil

# 🔥 RUTAS DENTRO DEL CONTENEDOR
BASE_PATH = "/opt/airflow/data"
INPUT_FOLDER = os.path.join(BASE_PATH, "input")
PROCESSING_FOLDER = os.path.join(BASE_PATH, "processing")
PROCESSED_FOLDER = os.path.join(BASE_PATH, "processed")


def mover_a_processing():
    files = os.listdir(INPUT_FOLDER)

    if not files:
        raise Exception("No hay archivos en input")

    file_name = files[0]

    src = os.path.join(INPUT_FOLDER, file_name)
    dst = os.path.join(PROCESSING_FOLDER, file_name)

    shutil.move(src, dst)

    print(f"Movido a processing: {file_name}")
    return file_name


def procesar_archivo(ti):
    file_name = ti.xcom_pull(task_ids="mover_a_processing")

    path = os.path.join(PROCESSING_FOLDER, file_name)

    print(f"Procesando archivo: {path}")

    with open(path, "r") as f:
        print(f.read())


def mover_a_processed(ti):
    file_name = ti.xcom_pull(task_ids="mover_a_processing")

    src = os.path.join(PROCESSING_FOLDER, file_name)
    dst = os.path.join(PROCESSED_FOLDER, file_name)

    shutil.move(src, dst)

    print(f"Archivo finalizado: {file_name}")


with DAG(
    dag_id="file_sensor_dev",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dev"]
) as dag:

    # 🔥 SENSOR CORRECTO
    esperar_archivo = FileSensor(
        task_id="esperar_archivo",
        filepath="/opt/airflow/data/input/test.txt",    # 👈 RELATIVO al fs_default
        poke_interval=10,
        timeout=300,
        fs_conn_id="fs_default",     # 👈 CONEXIÓN OBLIGATORIA
        mode="poke"
    )

    mover = PythonOperator(
        task_id="mover_a_processing",
        python_callable=mover_a_processing
    )

    procesar = PythonOperator(
        task_id="procesar_archivo",
        python_callable=procesar_archivo
    )

    finalizar = PythonOperator(
        task_id="mover_a_processed",
        python_callable=mover_a_processed
    )

    esperar_archivo >> mover >> procesar >> finalizar