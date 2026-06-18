from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.handlers.nom2001_9999d_handler import (
    load_nom2001_csv_to_json,
    load_control_table_to_json,
    load_sociedades_to_json,
    classify_records_to_json,
    detect_bajas_to_json,
    build_final_payloads_to_json,
    write_report,
    send_to_s4h,
)


DAG_ID = "NOM2001_9999D"
POSTGRES_CONN_ID = "gobierno_central_postgres"
ENDPOINT_NAME = "NOM2001_9999D_S4H_TARGET"

NOM2001_CSV_PATH = "/opt/airflow/data/input/nom_2001_completo.csv"
OUTPUT_DIR = "/opt/airflow/data/output/9999D"


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["9999D", "NOM2001", "S4H", "FINZ"],
)
def nom2001_9999d():

    @task
    def t_load_nom2001_csv() -> str:
        return load_nom2001_csv_to_json(
            csv_path=NOM2001_CSV_PATH,
            output_dir=OUTPUT_DIR,
            only_active=True,
        )

    @task
    def t_load_control_table() -> str:
        return load_control_table_to_json(
            output_dir=OUTPUT_DIR,
            postgres_conn_id=POSTGRES_CONN_ID,
        )

    @task
    def t_load_sociedades() -> str:
        return load_sociedades_to_json(
            output_dir=OUTPUT_DIR,
            postgres_conn_id=POSTGRES_CONN_ID,
        )

    @task
    def t_classify_records(
        nom_records_path: str,
        control_records_path: str,
        sociedades_path: str,
    ) -> str:
        return classify_records_to_json(
            nom_records_path=nom_records_path,
            control_records_path=control_records_path,
            sociedades_path=sociedades_path,
            output_dir=OUTPUT_DIR,
        )

    @task
    def t_detect_bajas(
        control_records_path: str,
        nom_records_path: str,
    ) -> str:
        return detect_bajas_to_json(
            control_records_path=control_records_path,
            nom_records_path=nom_records_path,
            output_dir=OUTPUT_DIR,
        )

    @task
    def t_build_payloads(
        classification_path: str,
        bajas_path: str,
        sociedades_path: str,
    ) -> str:
        return build_final_payloads_to_json(
            classification_path=classification_path,
            bajas_path=bajas_path,
            sociedades_path=sociedades_path,
            output_dir=OUTPUT_DIR,
        )

    @task
    def t_write_report(
        classification_path: str,
        bajas_path: str,
        payloads_path: str,
    ) -> dict:
        return write_report(
            classification_path=classification_path,
            bajas_path=bajas_path,
            payloads_path=payloads_path,
            output_dir=OUTPUT_DIR,
        )

    @task
    def t_send_to_s4h(payloads_path: str, **context) -> dict:
        return send_to_s4h(
            payloads_path=payloads_path,
            endpoint_name=ENDPOINT_NAME,
            dag_id=DAG_ID,
            run_id=context["run_id"],
            task_id="t_send_to_s4h",
            postgres_conn_id=POSTGRES_CONN_ID,
        )

    nom_records_path = t_load_nom2001_csv()
    control_records_path = t_load_control_table()
    sociedades_path = t_load_sociedades()

    classification_path = t_classify_records(
        nom_records_path,
        control_records_path,
        sociedades_path,
    )

    bajas_path = t_detect_bajas(
        control_records_path,
        nom_records_path,
    )

    payloads_path = t_build_payloads(
        classification_path,
        bajas_path,
        sociedades_path,
    )

    report_task = t_write_report(
        classification_path,
        bajas_path,
        payloads_path,
    )

    send_task = t_send_to_s4h(payloads_path)

    report_task >> send_task


nom2001_9999d()