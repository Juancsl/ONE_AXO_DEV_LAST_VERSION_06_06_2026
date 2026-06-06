# Archivo: GENERIC_ETL_PIPELINE/oneaxo_cfg_pkg/senders/mysql_sender.py

import logging
import json
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook

class MySqlUpsertSender:
    def send(self, payload: bytes, config: dict, **kwargs) -> dict:
        # 1. Obtener y validar la configuración (sin cambios)
        mysql_conn_id = config.get("mysql_conn_id")
        table_name = config.get("table_name")
        conflict_keys = config.get("conflict_keys")
        
        if not all([mysql_conn_id, table_name, conflict_keys]):
            raise ValueError("La configuración para MySqlUpsertSender debe incluir 'mysql_conn_id', 'table_name' y 'conflict_keys'.")

        # 2. Parsear el payload (sin cambios)
        try:
            records = json.loads(payload.decode('utf-8'))
        except json.JSONDecodeError:
            raise ValueError("El payload no es un JSON válido.")

        if not records or not isinstance(records, list):
            logging.info("El payload está vacío o no es una lista. No se realizará ningún envío.")
            return {"upserted_rows": 0}
        
        # 3. Construir la consulta SQL (sin cambios)
        columns = records[0].keys()
        cols_str = ", ".join([f"`{c}`" for c in columns])
        placeholders_str = ", ".join(["%s"] * len(columns))
        update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in columns if col not in conflict_keys])
        
        sql = (
            f"INSERT INTO {table_name} ({cols_str}) "
            f"VALUES ({placeholders_str}) "
            f"ON DUPLICATE KEY UPDATE {update_clause};"
        )

        # 4. Preparar los datos (sin cambios)
        data_tuples = [tuple(rec.get(col) for col in columns) for rec in records]
        
        # --- INICIO DE LA MODIFICACIÓN ---
        
        # 5. Ejecutar la operación usando un cursor y 'executemany'
        logging.info(f"Ejecutando UPSERT en la tabla '{table_name}' con {len(records)} registros.")
        print(sql)
        hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        
        # Usamos un 'with' para asegurar que la conexión se cierre correctamente
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # 'executemany' es el método correcto para ejecutar una consulta
                # para cada tupla de datos en la lista.
                cursor.executemany(sql, data_tuples)
            conn.commit()
        # --- FIN DE LA MODIFICACIÓN ---

        logging.info(f"UPSERT completado con éxito para {len(records)} registros.")
        
        return {"upserted_rows": len(records), "table": table_name}
