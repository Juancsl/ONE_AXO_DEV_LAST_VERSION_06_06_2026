# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any


class Local212PolizaNominaSource:
    """
    Source local temporal para integración 212.

    Lee TXT desde una carpeta local, sube RAW a S3 usando helpers del
    GENERIC_INTEGRATION_PIPELINE y regresa jobs al framework.
    """

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        global_context: dict[str, Any] | None = None,
    ):
        self.config = config or {}
        self.global_context = global_context or {}

        self.input_dir = Path(
            self.config.get("input_dir", "/opt/airflow/data/input/212")
        )
        self.processed_dir = Path(
            self.config.get("processed_dir", "/opt/airflow/data/processing/212/processed")
        )
        self.file_pattern = self.config.get("file_pattern", "*.TXT")

    def discover(self) -> list[dict[str, Any]]:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        aws_conn_id = self.global_context["aws_conn_id"]
        raw_bucket_name = self.global_context["raw_bucket_name"]
        endpoint_id = self.global_context.get("endpoint_id")
        endpoint_name = self.global_context.get("endpoint_name")
        dag_run_id = self.global_context.get("dag_run_id")
        s3_put_bytes = self.global_context["_s3_put_bytes"]
        safe_stem = self.global_context["_safe_stem"]

        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        files = sorted(self.input_dir.glob(self.file_pattern))
        jobs: list[dict[str, Any]] = []

        if not files:
            logging.info("212 local source: no hay archivos en %s", self.input_dir)
            return jobs

        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        for file_path in files:
            if not file_path.is_file():
                continue

            source_file = file_path.name
            safe_filename = safe_stem(source_file)
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

            integration_id = self.config.get(
                "integration_id",
                "R011A_POLIZA_NOMINA_212",
            )

            raw_s3_key = f"{integration_id}/raw/{safe_filename}_{timestamp}.txt"

            file_bytes = file_path.read_bytes()

            s3_put_bytes(
                s3_hook=s3_hook,
                bucket_name=raw_bucket_name,
                key=raw_s3_key,
                payload=file_bytes,
                content_type="text/plain",
            )

            processed_path = self.processed_dir / source_file
            shutil.move(str(file_path), str(processed_path))

            jobs.append(
                {
                    "integration_id": integration_id,
                    "source_file": source_file,
                    "safe_filename": safe_filename,
                    "timestamp": timestamp,
                    "raw_s3_key": raw_s3_key,
                    "source_endpoint_id": endpoint_id,
                    "source_endpoint_name": endpoint_name,
                    "dag_run_id": dag_run_id,
                    "local_processed_path": str(processed_path),
                }
            )

            logging.info(
                "212 local source: archivo %s enviado a RAW S3 key=%s",
                source_file,
                raw_s3_key,
            )

        return jobs