from __future__ import annotations

from airflow.providers.postgres.hooks.postgres import PostgresHook


def log_integration_event(
    postgres_conn_id: str,
    dag_id: str,
    run_id: str | None,
    task_id: str | None,
    integration_id: str | None,
    phase: str,
    status: str,
    source_file: str,
    raw_s3_key: str | None = None,
    canonical_s3_key: str | None = None,
    out_s3_key: str | None = None,
    message: str | None = None,
    error_message: str | None = None,
    send_details: str | None = None,
) -> None:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    sql = """
        INSERT INTO ctrlplane.tbl_integration_log_scm (
            dag_id,
            run_id,
            task_id,
            integration_id,
            phase,
            status,
            source_file,
            raw_s3_key,
            canonical_s3_key,
            out_s3_key,
            message,
            error_message,
            send_details
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    hook.run(
        sql,
        parameters=(
            dag_id,
            run_id,
            task_id,
            integration_id,
            phase,
            status,
            source_file,
            raw_s3_key,
            canonical_s3_key,
            out_s3_key,
            message,
            error_message,
            send_details,
        ),
    )

def log_processed_order(
          postgres_conn_id: str,
          order_number: str| None,
          integration_id: str| None,
          source_file: str| None,
          target_file: str| None,
          run_id: str| None,
        ) -> None:

        db_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        sql = """
            INSERT INTO ctrlplane.processed_purchase_orders 
            (purchase_order_number, integration_id, source_file, target_file, dag_run_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (purchase_order_number, integration_id) DO NOTHING;
        """
        db_hook.run(sql, parameters=(
            order_number,
            integration_id,
            source_file,
            target_file,
            run_id,
        ))


def safe_log_processed_order_event(**kwargs) -> None:
    try:
          log_processed_order(**kwargs)
    except Exception as exc:
         print(f"[WARN] No se pudo escribir la orden procesada en ctrlplane.tbl_cfg_processed_orders: {exc}")

def safe_log_integration_event(**kwargs) -> None:
    try:
        log_integration_event(**kwargs)
    except Exception as exc:
        print(f"[WARN] No se pudo escribir log en ctrlplane.tbl_integration_log_scm: {exc}")