# Archivo: GENERIC_ETL_PIPELINE/oneaxo_cfg_pkg/sources/iterating_http_source.py

import logging
import json
import requests
from datetime import datetime
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.parsers import get_path_value
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.api_auth_client import ApiClient

class IteratingHttpSourceHandler:
    def __init__(self, config: dict, global_context: dict):
        self.config = config
        self.global_context = global_context

    def get_plants(self) -> list[str]:
        api_creds = Variable.get("global_dictionary_config", deserialize_json=True)
        client = ApiClient(**api_creds)
        params = {"modules": ["EPOS_ORACLE"]}
        response = client.get(endpoint="/dictionary/findByModule", params=params)
        data = response.json()
        data_filtered = [str(x["targetValue1"]).strip() for x in data if x["catalog"]=="centros_inventarios"]

        print(data_filtered)
        return data_filtered


    def discover(self) -> list[dict]:
        """
        Itera sobre una lista de parámetros, ejecuta una llamada GET a la API por cada uno,
        y genera los jobs para el "Fan-Out".
        """
        # 1. Obtener la configuración del handler
        conn_details = self.config.get("connection_details", {})
        base_url = conn_details.get("base_url")
        endpoint_template = self.config.get("endpoint_template")
        parameters_list = self.config.get("parameters_list", [])
        car_id = self.config.get("car_id", "170")
        
        parameters_list = self.get_plants()

        # Obtenemos la configuración de autenticación
        auth_config = self.config.get("auth", {})
        variable_name = auth_config.get("airflow_variable_name")
        creds = Variable.get(variable_name, deserialize_json=True) if variable_name else {}
        auth_param = (creds.get("username"), creds.get("password")) if auth_config.get("type") == "basic" else None

        if not all([base_url, endpoint_template, parameters_list]):
            logging.warning("Configuración de IteratingHttpSourceHandler incompleta. Saltando.")
            return []

        all_jobs = []
        
        # 2. Bucle principal: una llamada a la API por cada parámetro
        for param in parameters_list:
            try:
                # Formateamos la URL para esta iteración
                specific_endpoint = endpoint_template.format(param=param)
                full_url = f"{base_url.rstrip('/')}{specific_endpoint}"
                
                logging.info(f"Realizando petición GET a: {full_url}")
                response = requests.get(full_url, auth=auth_param, headers={"Accept":"application/json", "sap-client":car_id},timeout=120)
                response.raise_for_status()
                
                response_json = response.json()
                records = get_path_value(response_json, "d.results") or []

                for record in records:
                    record.pop("__metadata", None)

                records = {"results": records}

                if not records:
                    logging.info(f"No se encontraron registros para el parámetro '{param}'.")
                    continue
                
                # 3. Fase de "Stage" y "Fan-Out" para los registros de esta llamada
                # (Esta lógica es idéntica a la del HttpSourceHandler anterior)
                logging.info(f"Procesando {len(records)} registros para el parámetro '{param}'.")
                job = self._stage_and_fan_out(records, param)
                all_jobs.append(job)

            except requests.exceptions.RequestException as e:
                logging.error(f"Error al realizar la petición para el parámetro '{param}': {e}")
                # Decidimos continuar con el siguiente parámetro
                continue
        
        return all_jobs

    def _stage_and_fan_out(self, record: list, record_id:str) -> list[dict]:
        """Función auxiliar para la lógica de staging y fan-out."""
        s3_hook = S3Hook(aws_conn_id=self.global_context['aws_conn_id'])
        db_hook = PostgresHook(postgres_conn_id=self.global_context['postgres_conn_id'])
        endpoint_id = self.global_context['endpoint_id']
        
        raw_bucket_name = self.global_context['raw_bucket_name']

        try:
            # Convertimos cada registro en su propio archivo JSON en S3
            payload_bytes = json.dumps(record, indent=4).encode('utf-8')
            
            # Creamos un nombre de archivo único
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
                    "source_file": f"api://record/{record_id}",
                    "filename": filename,
                    "raw_s3_key": raw_s3_key,
                    "timestamp": timestamp,
                    "safe_filename": self.global_context['_safe_stem'](filename)
                }
        
        except Exception as e:
            logging.error(f"Fallo al procesar un registro de la API: {record}. Error: {e}")
                
        return job
