# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID_DEFAULT = "gobierno_central_postgres"


class PolizaNomina212Handler:

    def build_canonical(
        self,
        file_bytes: bytes,
        integration_config: dict[str, Any],
        raw_job: dict[str, Any] | None = None,
        global_context: dict[str, Any] | None = None,
        **kwargs,
    ) -> dict[str, Any]:

        raw_job = raw_job or {}

        postgres_conn_id = (
            integration_config.get("postgres_conn_id")
            or raw_job.get("postgres_conn_id")
            or POSTGRES_CONN_ID_DEFAULT
        )

        source_file = raw_job.get("source_file") or raw_job.get("filename") or "unknown_file"
        raw_s3_key = raw_job.get("raw_s3_key")

        text = self._decode_file(file_bytes)
        lines = [line.rstrip("\n\r") for line in text.splitlines() if line.strip()]

        if not lines:
            raise ValueError(f"Archivo vacío o sin líneas válidas: {source_file}")

        first_line = lines[0]

        if len(first_line) < 38:
            raise ValueError(
                f"Archivo {source_file} no tiene longitud suficiente para leer sociedad. "
                f"Longitud primera línea: {len(first_line)}"
            )

        sociedad = first_line[34:38].strip()
        gd_key = f"FINANZAS-sociedades-{sociedad}"

        society_flag = self._lookup_global_dictionary(
            postgres_conn_id=postgres_conn_id,
            lookupkey=gd_key,
        )

        route = "S4H" if society_flag == "1" else "ECC"

        logging.info(
            "212 canonical: source_file=%s sociedad=%s gd_key=%s gd_value=%s route=%s",
            source_file,
            sociedad,
            gd_key,
            society_flag,
            route,
        )

        canonical_payload: dict[str, Any] = {
            "integration_id": integration_config.get(
                "integration_id",
                "R011A_POLIZA_NOMINA_212",
            ),
            "source_file": source_file,
            "raw_s3_key": raw_s3_key,
            "sociedad": sociedad,
            "global_dictionary_key": gd_key,
            "global_dictionary_value": society_flag,
            "route": route,
            "source_system": "NOM2001",
            "source_id": "NOMINA",
            "trans_uuid": str(uuid.uuid4()),
            "line_count": len(lines),
            "original_text": text if route == "ECC" else None,
            "polizas": [],
        }

        if route == "ECC":
            canonical_payload["polizas_count"] = 0
            return canonical_payload

        polizas = self._parse_s4h_polizas(
            lines=lines,
            source_file=source_file,
            sociedad=sociedad,
            trans_uuid=canonical_payload["trans_uuid"],
        )

        canonical_payload["polizas"] = polizas
        canonical_payload["polizas_count"] = len(polizas)
        canonical_payload["original_text"] = None

        return canonical_payload

    def build_out(
        self,
        canonical_payload: dict[str, Any],
        integration_config: dict[str, Any],
        canonical_job: dict[str, Any] | None = None,
        global_context: dict[str, Any] | None = None,
        **kwargs,
    ) -> bytes:

        route = canonical_payload.get("route")

        if route == "ECC":
            out_payload = {
                "route": "ECC",
                "source_file": canonical_payload.get("source_file"),
                "raw_s3_key": canonical_payload.get("raw_s3_key"),
                "sociedad": canonical_payload.get("sociedad"),
                "global_dictionary_key": canonical_payload.get("global_dictionary_key"),
                "global_dictionary_value": canonical_payload.get("global_dictionary_value"),
                "original_text": canonical_payload.get("original_text"),
            }

            return json.dumps(
                out_payload,
                ensure_ascii=False,
                indent=2,
            ).encode("utf-8")

        polizas = canonical_payload.get("polizas") or []

        if not polizas:
            raise ValueError("Route S4H pero no hay pólizas en canonical_payload")

        # Por ahora enviamos una póliza por OUT. Si algún archivo trae varias pólizas,
        # aquí se tendría que generar fan-out o lista de requests.
        sap_payload = self._build_sap_payload(
            poliza=polizas[0],
            canonical_payload=canonical_payload,
        )

        return json.dumps(
            sap_payload,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")

    def _build_sap_payload(
        self,
        poliza: dict[str, Any],
        canonical_payload: dict[str, Any],
    ) -> dict[str, Any]:

        header = poliza["header"]
        details = poliza.get("details") or []

        trans_uuid = poliza.get("TRANS_UUID") or canonical_payload.get("trans_uuid")
        idext = poliza.get("IDEXT")
        source_file = Path(str(poliza.get("source_file") or canonical_payload.get("source_file") or "")).name

        return {
            "TransUuid": trans_uuid,
            "Idext": idext,
            "Sourceid": poliza.get("SOURCEID", "NOMINA"),
            "Accountingdocumenttype": header.get("Accountingdocumenttype", ""),
            "Companycode": header.get("Companycode", ""),
            "Totalnumberoflineitem": header.get("Totalnumberoflineitem", ""),
            "Documentdate": header.get("Documentdate", ""),
            "Postingdate": header.get("Postingdate", ""),
            "Documentreferenceid": header.get("Documentreferenceid", ""),
            "Accountingdocumentheadertext": header.get("Accountingdocumentheadertext", ""),
            "Currency": header.get("Currency", ""),
            "Exchangerate": None,
            "CfdiUuid": "",
            "Filename": source_file,
            "NavHeaderToItem": [
                {
                    "TransUuid": trans_uuid,
                    "Idext": idext,
                    "Numberoflineitem": item.get("Numberoflineitem", ""),
                    "Glaccount": item.get("Glaccount", ""),
                    "Postingkey": item.get("Postingkey", ""),
                    "Amount": item.get("Amount", ""),
                    "Assignmentnumber": item.get("Assignmentnumber", ""),
                    "Documentitemtext": item.get("Documentitemtext", ""),
                    "Costcenter": item.get("Costcenter", ""),
                    "Taxcode": "",
                    "Specialglcode": "",
                    "Paymentterms": "",
                    "Duecalculationbasedate": "",
                    "Paymentblockingreason": "",
                    "Wbselement": "",
                    "Taxbaseamount": "",
                    "Profitcenter": "",
                    "Reference1": "",
                    "Reference2": "",
                    "Reference3": item.get("Reference3", ""),
                    "CfdiZuuid": "",
                }
                for item in details
            ],
        }

    def _lookup_global_dictionary(
        self,
        postgres_conn_id: str,
        lookupkey: str,
    ) -> str | None:

        hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        sql = """
            SELECT target_value
            FROM ctrlplane.tbl_cat_gd
            WHERE lookupkey = %s
            LIMIT 1
        """

        row = hook.get_first(sql, parameters=(lookupkey,))

        if not row:
            return None

        return str(row[0]).strip()

    def _decode_file(self, file_bytes: bytes) -> str:
        for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                return file_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue

        return file_bytes.decode("utf-8", errors="ignore")

    def _parse_s4h_polizas(
        self,
        lines: list[str],
        source_file: str,
        sociedad: str,
        trans_uuid: str,
    ) -> list[dict[str, Any]]:

        polizas_by_idext: dict[str, dict[str, Any]] = {}

        for line in lines:
            item_type = line[0:1]
            idext = line[1:13].strip()

            if not idext:
                raise ValueError(f"Línea sin IDEXT en {source_file}: {line}")

            if idext not in polizas_by_idext:
                polizas_by_idext[idext] = {
                    "IDEXT": idext,
                    "TRANS_UUID": trans_uuid,
                    "SOURCEID": "NOMINA",
                    "sociedad": sociedad,
                    "source_file": source_file,
                    "header": None,
                    "details": [],
                }

            if item_type == "H":
                polizas_by_idext[idext]["header"] = self._parse_header(line)

            elif item_type == "D":
                polizas_by_idext[idext]["details"].append(self._parse_detail(line))

            else:
                raise ValueError(
                    f"Tipo de línea no reconocido '{item_type}' en {source_file}: {line}"
                )

        polizas = []

        for idext, payload in polizas_by_idext.items():
            if payload["header"] is None:
                raise ValueError(f"Póliza {idext} no tiene cabecera H")

            expected_details = self._safe_int(
                payload["header"].get("Totalnumberoflineitem")
            )

            payload["expected_details"] = expected_details
            payload["actual_details"] = len(payload["details"])

            if expected_details and expected_details != len(payload["details"]):
                logging.warning(
                    "212 póliza %s: header indica %s detalles pero se leyeron %s",
                    idext,
                    expected_details,
                    len(payload["details"]),
                )

            polizas.append(payload)

        return polizas

    def _parse_header(self, line: str) -> dict[str, Any]:
        return {
            "ItemType": line[0:1].strip(),
            "Idext": line[1:13].strip(),
            "Totalnumberoflineitem": line[13:16].strip(),
            "Documentdate": line[16:24].strip(),
            "Postingdate": line[24:32].strip(),
            "Accountingdocumenttype": line[32:34].strip(),
            "Companycode": line[34:38].strip(),
            "Documentreferenceid": line[38:50].strip(),
            "Accountingdocumentheadertext": line[54:79].strip(),
            "Currency": line[79:84].strip(),
            "raw": line,
        }

    def _parse_detail(self, line: str) -> dict[str, Any]:
        return {
            "ItemType": line[0:1].strip(),
            "Idext": line[1:13].strip(),
            "Numberoflineitem": line[13:16].strip(),
            "Glaccount": line[16:26].strip(),
            "Postingkey": line[26:28].strip(),
            "Amount": line[28:41].strip(),
            "Assignmentnumber": line[41:59].strip(),
            "Documentitemtext": line[59:109].strip(),
            "Costcenter": line[109:119].strip(),
            "Reference3": line[156:].strip(),
            "raw": line,
        }

    def _safe_int(self, value: Any) -> int | None:
        try:
            value = str(value).strip()

            if not value:
                return None

            return int(value)

        except Exception:
            return None