from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_output_mappings(postgres_conn_id: str, integration_id: str, output_key: str) -> list[dict]:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    sql = """
        SELECT
            target_field,
            source_path,
            transform_rule,
            transform_params,
            default_value,
            required
        FROM scm.integration_field_mapping
        WHERE integration_id = %s
          AND output_key = %s
          AND active = true
        ORDER BY field_order
    """

    rows = hook.get_records(sql, parameters=(integration_id, output_key))

    if not rows:
        raise ValueError(
            f"No hay mappings para integration_id={integration_id}, output_key={output_key}"
        )

    return [
        {
            "target_field": row[0],
            "source_path": row[1],
            "transform_rule": row[2],
            "transform_params": row[3] or {},
            "default_value": row[4],
            "required": row[5],
        }
        for row in rows
    ]