from __future__ import annotations

import pendulum
import logging
from datetime import datetime
from psycopg2.extras import execute_values # Aún la necesitamos para el bulk UPSERT

from airflow.decorators import dag, task
from airflow.models import Variable
# ¡Reintroducimos el PostgresHook!
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Asumimos que el cliente de API está en include/clients/api_client.py
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.api_auth_client import ApiClient

# ---- CONSTANTES DEL DAG ----
API_VAR_NAME = "global_dictionary_config"
POSTGRES_CONN_ID = "gobierno_central_postgres"
TARGET_TABLE = "ctrlplane.tbl_cat_gd"
WATERMARK_COLUMN = "insertdate" 
UNIQUE_KEY_COLUMN = "lookupkey"

@dag(
    dag_id="etl_hybrid_incremental_to_postgres",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    doc_md="""
    ### DAG Híbrido de Carga Incremental
    Usa un cliente manual para la API (con refresco de token) y un Hook de Airflow
    para una gestión limpia y segura de la conexión a Postgres.
    """
)
def etl_hybrid_incremental_dag():

    @task
    def get_high_water_mark() -> str:
        """
        Usa el PostgresHook para obtener la marca de agua de forma limpia.
        """
        logging.info(f"Obteniendo la marca de agua con PostgresHook desde la tabla {TARGET_TABLE}.")
        
        # 1. Instanciamos el hook.
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # 2. El método `get_first` es perfecto para obtener un único resultado.
        #    El hook se encarga de abrir y cerrar la conexión y el cursor.
        sql = f"SELECT MAX({WATERMARK_COLUMN}) FROM {TARGET_TABLE};"
        result = pg_hook.get_first(sql)
        
        # El resultado de get_first es una tupla, ej. (datetime.datetime(...),) o (None,)
        if result and result[0]:
            watermark = pendulum.instance(result[0]).to_iso8601_string()
            logging.info(f"Marca de agua encontrada: {watermark}")
            return watermark
        else:
            default_watermark = pendulum.datetime(1970, 1, 1).to_iso8601_string()
            logging.info("No se encontró marca de agua. Usando valor por defecto.")
            return default_watermark

    @task
    def extract_incremental_data(high_water_mark: str) -> list[dict]:
        """
        Esta tarea no cambia. Sigue usando el cliente manual para la API.
        """
        # (El código de esta tarea es idéntico al de la respuesta anterior)
        logging.info(f"Extrayendo datos de la API modificados desde: {high_water_mark}")
        api_creds = Variable.get(API_VAR_NAME, deserialize_json=True)
        client = ApiClient(**api_creds) # Desempaquetamos el diccionario
        params = {"modules": ["FINANZAS","SUPPLY","EPOS_ORACLE"]}
        response = client.get(endpoint="/dictionary/findByModule", params=params)
        data = response.json()
        data_filtered = [x for x in data if pendulum.parse(x["lastUpdate"]) > pendulum.parse(high_water_mark)]
        if data_filtered:
            logging.info(f"Extracción exitosa. Se obtuvieron {len(data_filtered)} registros.")
        else:
            logging.info("La API no devolvió nuevos registros.")
        
        data_cleaned = []
        keys = []
        for x in data_filtered:
            lookupkey = f"{str(x['module']).strip()}-{str(x['catalog']).strip()}-{str(x['sourceValue1']).strip()}"
            if lookupkey not in keys:
                keys.append(lookupkey)
                data_cleaned.append({
                    'lookupkey': lookupkey,
                    'source_value':str(x["sourceValue1"]).strip(),
                    'target_value':str(x["targetValue1"]).strip(),
                    'description':str(x["description"]).strip(),
                    'insertdate':datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })

        return data_cleaned
    

    @task
    def upsert_data_to_postgres(api_data: list[dict]):
        """
        Usa el PostgresHook para gestionar la conexión y la transacción del UPSERT.
        """
        if not api_data:
            logging.info("No hay datos para cargar. Se omite la tarea.")
            return

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        target_fields = ["lookupkey", "source_value", "target_value", "description","insertdate"]
        update_fields = [f for f in target_fields if f != UNIQUE_KEY_COLUMN]
        
        sql_upsert = f"""
            INSERT INTO {TARGET_TABLE} ({', '.join(target_fields)})
            VALUES %s
            ON CONFLICT ({UNIQUE_KEY_COLUMN}) DO UPDATE SET
                {', '.join([f'{field} = EXCLUDED.{field}' for field in update_fields])};
        """
        
        insert_data = [tuple(d.get(field) for field in target_fields) for d in api_data]

        logging.info(f"Ejecutando UPSERT de {len(insert_data)} registros con PostgresHook.")

        # Usamos el hook para obtener una conexión gestionada.
        # El `with` se asegura de que la conexión se cierre incluso si hay errores.
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    execute_values(cursor, sql_upsert, insert_data, page_size=1000)
                    # El commit sigue siendo necesario para confirmar la transacción
                    conn.commit()
                    logging.info("UPSERT completado y transacción confirmada.")
                except Exception as e:
                    logging.error(f"Error durante el UPSERT: {e}")
                    # El rollback es crucial para deshacer cambios parciales
                    conn.rollback()
                    raise

    # ---- Flujo del DAG ----
    watermark = get_high_water_mark()
    incremental_data = extract_incremental_data(watermark)
    upsert_data_to_postgres(incremental_data)

etl_hybrid_incremental_dag()