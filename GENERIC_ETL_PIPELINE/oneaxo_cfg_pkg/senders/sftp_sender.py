# Archivo: GENERIC_ETL_PIPELINE/oneaxo_cfg_pkg/senders/sftp_sender.py

import json
import logging
from io import BytesIO
from pathlib import Path

from airflow.providers.sftp.hooks.sftp import SFTPHook


class SftpSender:
    def send(self, payload: bytes, config: dict, **kwargs) -> dict:
        sftp_conn_id = config["sftp_conn_id"]
        output_path = config["sftp_output_path"]

        raw_output_filename = kwargs["output_filename"]
        output_filename = Path(str(raw_output_filename)).name

        payload_to_send = payload

        try:
            decoded = payload.decode("utf-8")
            parsed = json.loads(decoded)

            if isinstance(parsed, dict) and parsed.get("original_text") is not None:
                payload_to_send = parsed["original_text"].encode("utf-8")

                source_file = parsed.get("source_file")
                if source_file:
                    output_filename = Path(str(source_file)).name

        except Exception:
            payload_to_send = payload

        remote_path = f"{output_path.rstrip('/')}/{output_filename}"

        logging.info(f"Iniciando envío a SFTP. Destino: {remote_path}")

        hook = SFTPHook(ssh_conn_id=sftp_conn_id)

        with hook.get_conn() as sftp_client:
            self._ensure_remote_dir(sftp_client, output_path)

            logging.info(f"Subiendo archivo a {remote_path}...")
            sftp_client.putfo(BytesIO(payload_to_send), remote_path)
            logging.info("Archivo subido con éxito.")

        return {
            "remote_output_path": remote_path,
            "output_filename": output_filename,
        }

    def _ensure_remote_dir(self, sftp_client, remote_dir: str) -> None:
        """
        Crea directorios remotos de forma recursiva.
        """
        remote_dir = remote_dir.rstrip("/")

        if not remote_dir:
            return

        parts = [p for p in remote_dir.split("/") if p]
        current = ""

        for part in parts:
            current += f"/{part}"

            try:
                sftp_client.stat(current)
            except IOError:
                logging.info(f"Creando directorio remoto: {current}")
                sftp_client.mkdir(current)