import json
import logging
import base64
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.senders.http_sender import HttpApiSender
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.senders.sftp_sender import SftpSender


AWS_CONN_ID = "one_axo_s3"
OUT_BUCKET_NAME = "one-axo-out"
  
def _s3_get_bytes(s3_hook: S3Hook, bucket_name: str, key: str) -> bytes:
    response = s3_hook.get_conn().get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read()

class CartaPorteSender:

    def send(self, payload: bytes, config: dict, **kwargs) -> dict:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        payload = json.loads(payload)

        tipo_operacion = payload.get("tipo_operacion")
        numero_transporte = payload.get("numero_transporte")
        transportista = payload.get("transportista")
        xml_out_s3_key = payload.get("xml_out_s3_key")
        xlsx_out_s3_key = payload.get("xlsx_out_s3_key")

        filename_xml = xml_out_s3_key.split("/")[-1]
        filename_xlsx = xlsx_out_s3_key.split("/")[-1]
        
        if tipo_operacion == "INBOUND":
            xlsx_bytes = _s3_get_bytes(s3_hook, OUT_BUCKET_NAME, xlsx_out_s3_key)
            xlsx_base64 = base64.b64encode(xlsx_bytes).decode()
            sftp_config = config.get("SFTPCONNFINBOUND")
        else: 
            sftp_config = config.get("SFTPCONNFOUTBOUND")    

        xml_bytes = _s3_get_bytes(s3_hook, OUT_BUCKET_NAME, xml_out_s3_key)
        xml_base64 = base64.b64encode(xml_bytes).decode()

        namespace = config.get("namespace")
        destinatarios = config.get("destinatarios")
        ambiente = config.get("ambiente")
        template = config.get("template")

        result = {}
        sftp_sender = SftpSender()

        result["xml_result"] = sftp_sender.send(
            payload=xml_bytes,
            config=sftp_config,
            output_filename=filename_xml
        )
        
        if tipo_operacion == "INBOUND":

            notification_payload = {
                "notification": {
                    "namespace": namespace,
                    "source": "job_airflow",
                    "target": destinatarios,
                    "subject": f"Archivos Inbound Cartaporte 122 ({ambiente})",
                    "area": f"{ambiente}",
                    "idTemplate": int(template),
                    "online": True,
                    "variables": [
                        {
                    "name": "${description}",
                     "value": f"<strong>El proceso de Cartaporte integración 122 ha finalizado en el ambiente de desarrollo, se anexa el archivo Excel {filename_xlsx} y XML {filename_xml} del transporte {numero_transporte} y Transportista {transportista} asociado a la ejecución. </strong>"
                },
		    	{
                    "name": "${extra_info}",
                     "value": " "
                },
		    	{
                    "name": "${tables}",
                     "value": " "
                }
                    ],
                    "files": [
                        {
                            "name": filename_xlsx,
                            "content": xlsx_base64,
                            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            "size": len(xlsx_base64)
                        },
                        {
                            "name": filename_xml,
                            "content": xml_base64,
                            "mimeType": "application/xml",
                            "size": len(xml_bytes)
                        }
                    ]
                }
            }
            
            result["xlsx_result"] = sftp_sender.send(
                payload=xlsx_bytes,
                config=sftp_config,
                output_filename=filename_xlsx
            )

            http_sender = HttpApiSender()
            result["mail_result"] = http_sender.send(
                payload=json.dumps(notification_payload).encode("utf-8"),
                config=config.get("HTTPCONN")
            )

        return result
        


