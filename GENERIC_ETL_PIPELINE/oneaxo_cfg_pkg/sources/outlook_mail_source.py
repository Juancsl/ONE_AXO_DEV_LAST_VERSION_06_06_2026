from __future__ import annotations

import base64
import logging
from datetime import datetime, timezone
from urllib.parse import quote

import requests
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class OutlookMailSourceHandler:
    def __init__(self, config: dict, global_context: dict):
        self.config = config
        self.global_context = global_context

        self.aws_conn_id = global_context["aws_conn_id"]
        self.postgres_conn_id = global_context["postgres_conn_id"]
        self.raw_bucket_name = global_context["raw_bucket_name"]
        self.integration_id = config.get("integration_id", "R092B_NOMERCH")

        self.mailbox = config["mailbox"]
        self.folder = config.get("folder", "inbox")
        self.subject_contains = config.get("subject_contains", "INT_123")
        self.attachment_extensions = config.get("attachment_extensions", [".xlsx"])
        self.ignore_inline = config.get("ignore_inline", True)

        self.graph_config_variable = config.get(
            "graph_config_variable",
            "outlook_graph_config",
        )

    def _get_graph_config(self) -> dict:
        return Variable.get(self.graph_config_variable, deserialize_json=True)

    def _get_access_token(self) -> str:
        graph_config = self._get_graph_config()
        tenant_id = graph_config["tenant_id"]

        token_url = graph_config.get(
            "token_url",
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        )

        body = {
            "client_id": graph_config["client_id"],
            "client_secret": graph_config["client_secret"],
            "scope": graph_config.get("scope", "https://graph.microsoft.com/.default"),
            "grant_type": graph_config.get("grant_type", "client_credentials"),
        }

        response = requests.post(token_url, data=body, timeout=60)
        response.raise_for_status()

        return response.json()["access_token"]

    def _headers(self) -> dict:
        token = self._get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _list_messages(self, headers: dict) -> list[dict]:
        today_utc = datetime.now(timezone.utc).date().isoformat() + "T00:00:00Z"

        url = (
            f"https://graph.microsoft.com/v1.0/users/{self.mailbox}"
            f"/mailFolders/{self.folder}/messages"
            "?$select=id,subject,from,receivedDateTime,hasAttachments"
            "&$top=50"
            "&$orderby=receivedDateTime desc"
        )

        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()

        messages = response.json().get("value", [])

        filtered_messages = [
            msg for msg in messages
            if self.subject_contains.lower() in (msg.get("subject") or "").lower()
            and (msg.get("receivedDateTime") or "") >= today_utc
        ]

        logging.info(
            "Se encontraron %s correos del día actual para asunto %s",
            len(filtered_messages),
            self.subject_contains,
        )

        return filtered_messages

    def _list_attachments(self, headers: dict, message_id: str) -> list[dict]:
        url = (
            f"https://graph.microsoft.com/v1.0/users/{self.mailbox}"
            f"/messages/{quote(message_id)}/attachments"
        )

        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()

        return response.json().get("value", [])

    def _is_valid_attachment(self, attachment: dict) -> bool:
        name = attachment.get("name") or ""

        if self.ignore_inline and attachment.get("isInline") is True:
            return False

        if not attachment.get("contentBytes"):
            return False

        return any(
            name.lower().endswith(ext.lower())
            for ext in self.attachment_extensions
        )

    def _safe_filename(self, filename: str) -> str:
        return filename.replace("\\", "_").replace("/", "_").replace(" ", "_")

    def _is_message_processed(self, message_id: str) -> bool:
        if not message_id:
            return False

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        result = hook.get_first(
            """
            SELECT 1
            FROM ctrlplane.tbl_cfg_processed_orders_2
            WHERE id_value = %s
              AND integration_id = %s
            LIMIT 1
            """,
            parameters=(message_id, self.integration_id),
        )

        return result is not None

    def _mark_message_processed(self, message: dict, source_file: str) -> None:
        message_id = message.get("id")

        if not message_id:
            return

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        hook.run(
            """
            INSERT INTO ctrlplane.tbl_cfg_processed_orders_2
            (
                id_value,
                integration_id,
                source_file,
                dag_run_id
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id_value, integration_id)
            DO NOTHING
            """,
            parameters=(
                message_id,
                self.integration_id,
                source_file,
                self.global_context.get("dag_run_id"),
            ),
        )

    def _build_missing_attachment_job(self, message: dict) -> dict:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        source_file = f"mail://{self.mailbox}/NO_ATTACHMENT"

        return {
            "endpoint_id": self.global_context["endpoint_id"],
            "endpoint_name": self.global_context["endpoint_name"],
            "integration_id": "R092B_NOMERCH",
            "source_file": source_file,
            "raw_s3_key": None,
            "timestamp": timestamp,
            "safe_filename": "NO_ATTACHMENT",
            "mail_subject": message.get("subject"),
            "mail_receivedDateTime": message.get("receivedDateTime"),
            "mail_from": (
                message.get("from", {})
                .get("emailAddress", {})
                .get("address", "")
            ),
            "notification_only": True,
            "notification_issue": {
                "integration_id": "R092B_NOMERCH",
                "source_file": source_file,
                "issue_code": "missing_attachment",
                "severity": "error",
                "field_name": "attachment",
                "message": (
                    "No se encontró archivo adjunto para procesar "
                    f"en el correo {message.get('subject')}"
                ),
                "record_identifier": message.get("id"),
            },
        }

    def _stage_attachment(
        self,
        s3_hook: S3Hook,
        attachment: dict,
        message: dict,
    ) -> dict:
        filename = attachment["name"]
        safe_filename = self._safe_filename(filename)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        raw_s3_key = (
            f"staged/OUTLOOK_R092B_NOMERCH_SOURCE/"
            f"{timestamp}_{safe_filename}"
        )

        file_bytes = base64.b64decode(attachment["contentBytes"])

        s3_hook.get_conn().put_object(
            Bucket=self.raw_bucket_name,
            Key=raw_s3_key,
            Body=file_bytes,
            ContentType=attachment.get(
                "contentType",
                "application/octet-stream",
            ),
        )

        return {
            "endpoint_id": self.global_context["endpoint_id"],
            "endpoint_name": self.global_context["endpoint_name"],
            "integration_id": "R092B_NOMERCH",
            "source_file": f"mail://{self.mailbox}/{filename}",
            "raw_s3_key": raw_s3_key,
            "timestamp": timestamp,
            "safe_filename": safe_filename.rsplit(".", 1)[0],
            "mail_subject": message.get("subject"),
            "mail_receivedDateTime": message.get("receivedDateTime"),
            "mail_from": (
                message.get("from", {})
                .get("emailAddress", {})
                .get("address", "")
            ),
        }

    def discover(self) -> list[dict]:
        headers = self._headers()
        messages = self._list_messages(headers)

        if not messages:
            logging.info(
                "No se encontraron correos con asunto %s para mailbox %s",
                self.subject_contains,
                self.mailbox,
            )
            return []

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        for message in messages:
            message_id = message.get("id")

            # ==========================================================
            # SIN ADJUNTO: SIEMPRE NOTIFICAR, AUNQUE YA ESTÉ REGISTRADO
            # ==========================================================
            if not message.get("hasAttachments"):
                logging.warning(
                    "Correo sin adjuntos detectado. id=%s subject=%s receivedDateTime=%s",
                    message_id,
                    message.get("subject"),
                    message.get("receivedDateTime"),
                )

                source_file = f"mail://{self.mailbox}/NO_ATTACHMENT"

                self._mark_message_processed(
                    message,
                    source_file,
                )

                return [
                    self._build_missing_attachment_job(message)
                ]

            # ==========================================================
            # CON ADJUNTO: EVITAR DUPLICADOS
            # ==========================================================
            if self._is_message_processed(message_id):
                logging.info(
                    "Correo ya procesado previamente. id=%s subject=%s receivedDateTime=%s",
                    message_id,
                    message.get("subject"),
                    message.get("receivedDateTime"),
                )
                continue

            attachments = self._list_attachments(headers, message_id)

            valid_attachments = [
                attachment
                for attachment in attachments
                if self._is_valid_attachment(attachment)
            ]

            # ==========================================================
            # TIENE ADJUNTOS, PERO NINGUNO ES VÁLIDO: NOTIFICAR
            # ==========================================================
            if not valid_attachments:
                logging.warning(
                    "Correo sin adjuntos válidos detectado. id=%s subject=%s receivedDateTime=%s",
                    message_id,
                    message.get("subject"),
                    message.get("receivedDateTime"),
                )

                source_file = f"mail://{self.mailbox}/NO_ATTACHMENT"

                self._mark_message_processed(
                    message,
                    source_file,
                )

                return [
                    self._build_missing_attachment_job(message)
                ]

            attachment = valid_attachments[0]

            job = self._stage_attachment(
                s3_hook=s3_hook,
                attachment=attachment,
                message=message,
            )

            self._mark_message_processed(
                message,
                job["source_file"],
            )

            logging.info(
                "OutlookMailSourceHandler procesará solo el correo más reciente válido. "
                "subject=%s receivedDateTime=%s attachment=%s",
                message.get("subject"),
                message.get("receivedDateTime"),
                attachment.get("name"),
            )

            return [job]

        logging.info(
            "No se encontró ningún correo %s nuevo con adjunto válido %s",
            self.subject_contains,
            self.attachment_extensions,
        )

        return []