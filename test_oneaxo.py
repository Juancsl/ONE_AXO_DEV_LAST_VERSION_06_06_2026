from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello_oneaxo():
    print("🔥 ONE_AXO pipeline funcionando correctamente")


with DAG(
    dag_id="oneaxo_test_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["oneaxo"],
) as dag:

    task_hello = PythonOperator(
        task_id="hello_task",
        python_callable=hello_oneaxo,
    )

    task_hello