# En: oneaxo_cfg_pkg/sources/sftp_source.py
import logging
from io import BytesIO
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.integration_loader import _filename_matches

class SftpSourceHandler:
    def __init__(self, config: dict, global_context: dict):
        """
        El constructor ahora recibe tanto la configuración específica del endpoint
        como un contexto global con IDs de conexión y constantes.
        """
        self.config = config
        self.global_context = global_context

    def discover(self) -> list[dict]:
        """
        Descubre, pone en escena (stage) y genera los jobs para los archivos encontrados.
        """
        # Obtenemos los parámetros del contexto global y de la configuración específica
        sftp_conn_id = self.config.get('sftp_conn_id')
        input_path = self.config.get('input_path')
        endpoint_id = self.global_context['endpoint_id']
        endpoint_name = self.global_context['endpoint_name']
        integrations_to_run = self.global_context.get('integrations_to_run')
        
        # Obtenemos las conexiones y constantes globales
        aws_conn_id = self.global_context['aws_conn_id']
        postgres_conn_id = self.global_context['postgres_conn_id']
        raw_bucket_name = self.global_context['raw_bucket_name']

        # Instanciamos los hooks que necesitamos aquí dentro
        sftp_hook = SFTPHook(ssh_conn_id=sftp_conn_id)
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        db_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        
        jobs = []
        try:
            filenames = sftp_hook.list_directory(input_path)
        except Exception as e:
            logging.error(f"No se pudo listar el directorio {input_path} en SFTP {sftp_conn_id}: {e}")
            return []

        for filename in filenames:
            if _filename_matches(filename, self.config.get('filename_match_type'), self.config.get('filename_pattern')):
                remote_full_path = f"{input_path.rstrip('/')}/{filename}"
                try:
                    # --- FASE DE STAGE: Lógica movida aquí ---
                    with sftp_hook.get_conn() as sftp_client:
                        buffer = BytesIO()
                        sftp_client.getfo(remote_full_path, buffer)
                        raw_bytes = buffer.getvalue()
                        sftp_client.remove(remote_full_path)

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    raw_s3_key = f"staged/{endpoint_name}/{timestamp}_{filename}"
                    self.global_context['_s3_put_bytes'](s3_hook, raw_bucket_name, raw_s3_key, raw_bytes)
                    
                    # --- FASE DE FAN-OUT: Búsqueda inversa movida aquí ---
                    sql = "SELECT integration_id FROM scm.int_config_ctl WHERE source_endpoint_id = %s AND active = true"
                    target_integrations = db_hook.get_records(sql, parameters=(endpoint_id,))
                    target_integrations_filtered = []
                    for (integration_id,) in target_integrations:
                        if integration_id in integrations_to_run:
                            target_integrations_filtered.append((integration_id,))

                    if not target_integrations_filtered:
                        logging.warning(f"Archivo {remote_full_path} procesado, pero ninguna integración está suscrita al endpoint {endpoint_id}.")
                        continue

                    for (integration_id,) in target_integrations_filtered:
                        job = {
                            "endpoint_id": endpoint_id,
                            "integration_id": integration_id,
                            "source_file": remote_full_path,
                            "filename": filename,
                            "raw_s3_key": raw_s3_key,
                            "timestamp": timestamp,
                            "safe_filename": self.global_context['_safe_stem'](filename)
                        }
                        jobs.append(job)
                
                except Exception as file_exc:
                    # El log de error por archivo ahora es responsabilidad del handler
                    logging.error(f"Fallo en la fase de STAGE para {remote_full_path}: {file_exc}")
                    # Opcional: podrías loguear este fallo en la BD si el handler tiene acceso a la función de logging
        
        return jobs