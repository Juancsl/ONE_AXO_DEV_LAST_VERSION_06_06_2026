import logging
import fnmatch
import re

from airflow.providers.postgres.hooks.postgres import PostgresHook


def _hook(postgres_conn_id: str) -> PostgresHook:
    return PostgresHook(postgres_conn_id=postgres_conn_id)


def load_active_source_endpoints(postgres_conn_id: str, integration_list: list[str] | None) -> list[dict]:
    """
    Carga todos los endpoints de origen ('source') en la lista 'integration_list' activos desde el catálogo.
    """
    hook = _hook(postgres_conn_id)
    if integration_list:
            logging.info(f"Ejecutando con filtro de integraciones desde constante: {integration_list}")
            # Primero, buscamos qué endpoints de origen están asociados a las integraciones de nuestra lista.
            sql_get_endpoints = "SELECT DISTINCT source_endpoint_id FROM scm.int_config_ctl WHERE integration_id IN %s"
            endpoint_id_rows = hook.get_records(sql_get_endpoints, parameters=(tuple(integration_list),))

            if not endpoint_id_rows:
                logging.warning(f"Ningún endpoint de origen encontrado para las integraciones: {integration_list}")
                return []

            relevant_endpoint_ids = tuple(row[0] for row in endpoint_id_rows if row[0] is not None)
            if not relevant_endpoint_ids:
                return []

            # Ahora, cargamos solo la configuración de esos endpoints relevantes.
            sql_load_endpoints = "SELECT endpoint_id, endpoint_name, config FROM ctrlplane.tbl_cfg_endpoints_airflow WHERE endpoint_id IN %s AND active = true AND endpoint_type = 'source'"
            endpoint_rows = hook.get_records(sql_load_endpoints, parameters=(relevant_endpoint_ids,))
            
    else:
        logging.info("Ejecutando sin filtro de integraciones (todas las activas).")
        # Si la constante está vacía, cargamos todos los endpoints de origen como antes.
        sql_load_endpoints = """
            SELECT
                endpoint_id,
                endpoint_name,
                config
            FROM ctrlplane.tbl_cfg_endpoints_airflow
            WHERE active = true AND endpoint_type = 'source'
            ORDER BY endpoint_id
        """
        endpoint_rows = hook.get_records(sql_load_endpoints)

    return [{"endpoint_id": r[0], "endpoint_name": r[1], "source_config": r[2]} for r in endpoint_rows]


def load_runtime_config_by_integration_id(postgres_conn_id: str, integration_id: str) -> dict:
    """
    Carga la configuración completa para una integración específica, haciendo JOIN
    con el catálogo de endpoints para obtener la configuración del destino.
    """
    hook = _hook(postgres_conn_id)
    sql = """
        SELECT
            c.integration_id,
            c.source_format,
            c.target_format,
            c.engine_mode,
            c.handler_module_path,
            c.handler_class,
            c.parser_config,
            c.entities_config,
            c.models_config,
            c.outputs_config,
            c.business_rules_config,
            c.canonical_filename_prefix,
            c.output_filename_prefix,
            c.delivery_group,
            c.delivery_order,
            -- Campos de la tabla de endpoints de destino
            t.config as target_config
        FROM scm.int_config_ctl c
        -- Hacemos JOIN para obtener la configuración del endpoint de destino
        LEFT JOIN ctrlplane.tbl_cfg_endpoints_airflow t
          ON c.target_endpoint_id = t.endpoint_id
        WHERE c.integration_id = %s
          AND c.active = true
        LIMIT 1
    """
    row = hook.get_first(sql, parameters=(integration_id,))
    if not row:
        raise ValueError(f"No existe configuración activa para integration_id={integration_id}")
    
    return {
        "integration_id": row[0],
        "source_format": row[1],
        "target_format": row[2],
        "engine_mode": row[3],
        "handler_module_path": row[4],
        "handler_class": row[5],
        "parser_config": row[6] or {},
        "entities_config": row[7] or [],
        "models_config": row[8] or {},
        "outputs_config": row[9] or {},
        "business_rules_config": row[10] or {},
        "canonical_filename_prefix": row[11],
        "output_filename_prefix": row[12],
        "delivery_group": row[13],
        "delivery_order": row[14],
        "target_config": row[15] or {}, # <-- NUEVO: Configuración del destino

    }


def _filename_matches(filename: str, match_type: str, pattern: str) -> bool:
    # Esta función de utilidad se mantiene sin cambios
    match_type = (match_type or "glob").lower()
    if match_type == "exact":
        return filename == pattern
    if match_type == "regex":
        return re.match(pattern, filename) is not None
    if match_type == "glob":
        return fnmatch.fnmatch(filename, pattern)
    raise ValueError(f"filename_match_type no soportado: {match_type}")


def _resolve_endpoint_for_file(endpoints: list[dict], remote_dir: str, filename: str):
    candidates = [
        e for e in endpoints
        if e["sftp_input_path"] == remote_dir
        and _filename_matches(filename, e["filename_match_type"], e["filename_pattern"])
    ]

    if not candidates:
        return None

    candidates = sorted(candidates, key=lambda x: (x["priority"], x["integration_id"], x["endpoint_id"]))
    best_priority = candidates[0]["priority"]
    best = [c for c in candidates if c["priority"] == best_priority]

    if len(best) > 1:
        raise ValueError(
            f"Archivo ambiguo: {remote_dir}/{filename} coincide con múltiples endpoints de la misma prioridad: "
            + ", ".join([str(b["endpoint_id"]) for b in best])
        )

    return best[0]


def discover_jobs_from_sftp(sftp_hook, endpoint_configs: list[dict]) -> list[dict]:
    jobs = []
    input_paths = sorted(set(e["sftp_input_path"] for e in endpoint_configs))

    for remote_dir in input_paths:
        filenames = sftp_hook.list_directory(remote_dir)

        for filename in filenames:
            match = _resolve_endpoint_for_file(endpoint_configs, remote_dir, filename)
            if not match:
                continue

            jobs.append({
                "endpoint_id": match["endpoint_id"],
                "integration_id": match["integration_id"],
                "remote_dir": remote_dir,
                "filename": filename,
                "remote_full_path": f"{remote_dir.rstrip('/')}/{filename}",
            })

    jobs.sort(key=lambda x: (x["integration_id"], x["filename"]))
    return jobs