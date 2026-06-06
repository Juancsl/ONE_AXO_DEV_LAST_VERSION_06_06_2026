import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.senders.sftp_sender import SftpSender

AWS_CONN_ID = "one_axo_s3"
OUT_BUCKET_NAME = "one-axo-out"


def _s3_get_bytes(s3_hook: S3Hook, bucket_name: str, key: str) -> bytes:
    response = s3_hook.get_conn().get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read()


class CartaPorteNoMerchSender:

    def send(self, payload: bytes, config: dict, **kwargs) -> dict:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        sftp_sender = SftpSender()

        payload_json = json.loads(payload)

        if isinstance(payload_json, dict):
            payload_items = [payload_json]
        else:
            payload_items = payload_json

        results = []

        for item in payload_items:
            numero_transporte = item.get("numero_transporte")
            transportista = item.get("transportista")
            numero_transporte_gd = item.get("numero_transporte_gd")
            tipo_operacion = item.get("tipo_operacion", "NOMERCH")

            xml_out_s3_key = item.get("xml_out_s3_key")
            xlsx_out_s3_key = item.get("xlsx_out_s3_key")

            if not xml_out_s3_key:
                raise ValueError("Payload sin xml_out_s3_key")

            if not xlsx_out_s3_key:
                raise ValueError("Payload sin xlsx_out_s3_key")

            if not numero_transporte_gd:
                raise ValueError(
                    f"No se encontró numero_transporte_gd para transportista {transportista}"
                )

            filename_xml = xml_out_s3_key.split("/")[-1]
            filename_xlsx = xlsx_out_s3_key.split("/")[-1]

            xml_bytes = _s3_get_bytes(
                s3_hook=s3_hook,
                bucket_name=OUT_BUCKET_NAME,
                key=xml_out_s3_key,
            )

            xlsx_bytes = _s3_get_bytes(
                s3_hook=s3_hook,
                bucket_name=OUT_BUCKET_NAME,
                key=xlsx_out_s3_key,
            )

            base_path = config.get("sftp_output_path", "/")
            dynamic_config = dict(config)
            dynamic_config["sftp_output_path"] = (
                f"{base_path.rstrip('/')}/{numero_transporte_gd}/"
            )

            xml_result = sftp_sender.send(
                payload=xml_bytes,
                config=dynamic_config,
                output_filename=filename_xml,
            )

            xlsx_result = sftp_sender.send(
                payload=xlsx_bytes,
                config=dynamic_config,
                output_filename=filename_xlsx,
            )

            results.append(
                {
                    "numero_transporte": numero_transporte,
                    "transportista": transportista,
                    "numero_transporte_gd": numero_transporte_gd,
                    "tipo_operacion": tipo_operacion,
                    "xml_out_s3_key": xml_out_s3_key,
                    "xlsx_out_s3_key": xlsx_out_s3_key,
                    "remote_folder": dynamic_config["sftp_output_path"],
                    "xml_result": xml_result,
                    "xlsx_result": xlsx_result,
                }
            )

        return {
            "status": "success",
            "files_sent": results,
        }