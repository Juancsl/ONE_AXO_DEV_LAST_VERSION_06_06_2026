# Archivo: GENERIC_ETL_PIPELINE/oneaxo_cfg_pkg/sources/http_source.py

import logging
import json
from datetime import datetime
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Reutilizamos tu función para navegar diccionarios
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.parsers import get_path_value

class HttpSourceHandler:
    def __init__(self, config: dict, global_context: dict):
        """
        El constructor recibe la configuración específica del endpoint y un contexto global.
        """
        self.config = config
        self.global_context = global_context

    def discover(self) -> list[dict]:
        """
        Realiza una petición GET a una API, guarda cada registro en S3 (staging),
        y genera los jobs para el "Fan-Out".
        """
        # 1. Obtener la configuración del handler
        http_conn_id = self.config.get("http_conn_id")
        endpoint_path = self.config.get("endpoint")
        request_params = self.config.get("request_params", {})
        response_data_path = self.config.get("response_data_path", "")
        record_id_path = self.config.get("record_id_path") # Campo para identificar cada registro

        if not http_conn_id or not endpoint_path:
            raise ValueError("HttpSourceHandler requiere 'http_conn_id' y 'endpoint' en su configuración.")

        # 2. Realizar la petición HTTP usando el HttpHook
        logging.info(f"Realizando petición GET a la conexión '{http_conn_id}' en el endpoint '{endpoint_path}'.")
        http_hook = HttpHook(method='GET', http_conn_id=http_conn_id)
        response = http_hook.run(endpoint=endpoint_path, data=request_params)
        response.raise_for_status() # Falla si el status no es 2xx

        # 3. Procesar la respuesta
        response_json = response.json()
        # Navegamos hasta la lista de registros dentro de la respuesta
        records = get_path_value(response_json, response_data_path) or []
        
        if not records:
            logging.info("La API no devolvió registros en la ruta especificada.")
            return []

        logging.info(f"API devolvió {len(records)} registros para procesar.")
        
        # 4. Fase de "Stage" y "Fan-Out" por cada registro
        s3_hook = S3Hook(aws_conn_id=self.global_context['aws_conn_id'])
        db_hook = PostgresHook(postgres_conn_id=self.global_context['postgres_conn_id'])
        endpoint_id = self.global_context['endpoint_id']
        raw_bucket_name = self.global_context['raw_bucket_name']
        
        all_jobs = []
        for record in records:
            try:
                # Convertimos cada registro en su propio archivo JSON en S3
                payload_bytes = json.dumps(record, indent=4).encode('utf-8')
                
                # Creamos un nombre de archivo único
                record_id = get_path_value(record, record_id_path) if record_id_path else f"record_{len(all_jobs)}"
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"from_api_{record_id}.json"
                raw_s3_key = f"staged/{endpoint_id}/{timestamp}_{filename}"

                # Subimos a S3
                self.global_context['_s3_put_bytes'](s3_hook, raw_bucket_name, raw_s3_key, payload_bytes)
                
                # Búsqueda inversa para encontrar las integraciones suscritas
                sql = "SELECT integration_id FROM scm.int_config_ctl WHERE source_endpoint_id = %s AND active = true"
                target_integrations = db_hook.get_records(sql, parameters=(endpoint_id,))

                # Generamos un job por cada integración
                for (integration_id,) in target_integrations:
                    job = {
                        "endpoint_id": endpoint_id,
                        "integration_id": integration_id,
                        "source_file": f"api://{http_conn_id}{endpoint_path}/record/{record_id}",
                        "filename": filename,
                        "raw_s3_key": raw_s3_key,
                        "timestamp": timestamp,
                        "safe_filename": self.global_context['_safe_stem'](filename)
                    }
                    all_jobs.append(job)
            
            except Exception as e:
                logging.error(f"Fallo al procesar un registro de la API: {record}. Error: {e}")
                
        return all_jobs