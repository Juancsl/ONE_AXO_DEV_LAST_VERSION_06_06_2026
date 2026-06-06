# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook

from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.exceptions import ValidationRejectFileError


POSTGRES_CONN_ID_DEFAULT = "gobierno_central_postgres"


class ViaticosAmex1037Handler:
    """
    Handler custom para integración 1037 - AMEX Viáticos.

    Flujo equivalente NiFi:
    - Lee archivo H/D.
    - Extrae sociedad en posición 34:38.
    - Lookup FINANZAS-sociedades-{sociedad}.
    - Society == 1 -> S4H.
    - Society != 1 -> ECC.
    - ECC manda archivo original.
    - S4H parsea H y D.
    - Para registros D:
        * Si la partida es 1, fuerza Glaccount = 3000326.
        * Si Postingkey está en 40/50, hace lookup FINANZAS-plan_de_cuentas-{cuenta_origen}.
        * Siempre hace lookup de Costcenter con FINANZAS-ceco-cebe-{centro_origen}.
    - Arma payload SAP.
    """

    def build_canonical(
        self,
        file_bytes: bytes,
        integration_config: dict[str, Any],
        raw_job: dict[str, Any] | None = None,
        global_context: dict[str, Any] | None = None,
        **kwargs,
    ) -> dict[str, Any]:

        raw_job = raw_job or {}
        global_context = global_context or {}

        postgres_conn_id = (
            integration_config.get("postgres_conn_id")
            or raw_job.get("postgres_conn_id")
            or global_context.get("postgres_conn_id")
            or POSTGRES_CONN_ID_DEFAULT
        )

        source_file = raw_job.get("source_file") or raw_job.get("filename") or "unknown_file"
        raw_s3_key = raw_job.get("raw_s3_key")

        if not file_bytes:
            raise ValidationRejectFileError(
                "Archivo vacío",
                issues=[
                    {
                        "integration_id": integration_config.get("integration_id", "1037"),
                        "source_file": source_file,
                        "issue_code": "EMPTY_FILE",
                        "severity": "error",
                        "field_name": "fileSize",
                        "message": "El archivo recibido está vacío.",
                        "record_identifier": source_file,
                    }
                ],
            )

        text = self._decode_file(file_bytes)
        lines = [line.rstrip("\r\n") for line in text.splitlines() if line.strip()]

        if not lines:
            raise ValidationRejectFileError(
                "Archivo sin líneas válidas",
                issues=[
                    {
                        "integration_id": integration_config.get("integration_id", "1037"),
                        "source_file": source_file,
                        "issue_code": "EMPTY_LINES",
                        "severity": "error",
                        "field_name": "content",
                        "message": "El archivo no contiene líneas válidas.",
                        "record_identifier": source_file,
                    }
                ],
            )

        first_line = lines[0]

        if len(first_line) < 38:
            raise ValidationRejectFileError(
                "No se pudo extraer sociedad",
                issues=[
                    {
                        "integration_id": integration_config.get("integration_id", "1037"),
                        "source_file": source_file,
                        "issue_code": "MISSING_SOCIEDAD",
                        "severity": "error",
                        "field_name": "Sociedad",
                        "message": (
                            "La primera línea no tiene longitud suficiente para extraer "
                            "Sociedad en posición 34:38."
                        ),
                        "record_identifier": source_file,
                    }
                ],
            )

        sociedad = first_line[34:38].strip()
        society_key = f"FINANZAS-sociedades-{sociedad}"

        society_flag = self._lookup_global_dictionary(
            postgres_conn_id=postgres_conn_id,
            lookupkey=society_key,
        )

        route = "S4H" if str(society_flag).strip() == "1" else "ECC"

        trans_uuid = str(uuid.uuid4())

        canonical_payload: dict[str, Any] = {
            "integration_id": integration_config.get("integration_id", "1037"),
            "source_file": source_file,
            "raw_s3_key": raw_s3_key,
            "sociedad": sociedad,
            "global_dictionary_key": society_key,
            "global_dictionary_value": society_flag,
            "route": route,
            "source_system": "AMEX",
            "source_id": "VIATICOS",
            "trans_uuid": trans_uuid,
            "line_count": len(lines),
            "original_text": text if route == "ECC" else None,
            "headers": [],
            "details": [],
            "polizas": [],
        }

        logging.info(
            "1037 canonical: source_file=%s sociedad=%s society_key=%s society_flag=%s route=%s",
            source_file,
            sociedad,
            society_key,
            society_flag,
            route,
        )

        if route == "ECC":
            return canonical_payload

        headers: list[dict[str, Any]] = []
        details: list[dict[str, Any]] = []

        for line in lines:
            record_type = line[0:1]

            if record_type == "H":
                headers.append(
                    self._parse_header(
                        line=line,
                        source_file=source_file,
                        trans_uuid=trans_uuid,
                    )
                )
            elif record_type == "D":
                details.append(
                    self._parse_detail(
                        line=line,
                        postgres_conn_id=postgres_conn_id,
                    )
                )
            else:
                raise ValueError(
                    f"Tipo de línea no reconocido '{record_type}' en archivo {source_file}: {line}"
                )

        polizas = self._group_polizas(
            headers=headers,
            details=details,
            source_file=source_file,
            sociedad=sociedad,
            trans_uuid=trans_uuid,
        )

        canonical_payload["headers"] = headers
        canonical_payload["details"] = details
        canonical_payload["polizas"] = polizas
        canonical_payload["polizas_count"] = len(polizas)

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

            return json.dumps(out_payload, ensure_ascii=False, indent=2).encode("utf-8")

        polizas = canonical_payload.get("polizas") or []

        if not polizas:
            raise ValueError("Route S4H pero no hay pólizas para enviar.")

        # Compatibilidad con el pipeline genérico anterior:
        # si llama build_out directo, regresa la primera póliza.
        # El pipeline corregido de fan-out puede usar build_out_payloads() o _build_sap_payload().
        sap_payload = self._build_sap_payload(
            poliza=polizas[0],
            canonical_payload=canonical_payload,
        )

        return json.dumps(sap_payload, ensure_ascii=False, indent=2).encode("utf-8")

    def build_out_payloads(
        self,
        canonical_payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """
        Helper para pipelines que soportan fan-out.
        Devuelve un payload SAP por cada póliza H.
        """

        if canonical_payload.get("route") == "ECC":
            return [
                {
                    "route": "ECC",
                    "source_file": canonical_payload.get("source_file"),
                    "raw_s3_key": canonical_payload.get("raw_s3_key"),
                    "sociedad": canonical_payload.get("sociedad"),
                    "global_dictionary_key": canonical_payload.get("global_dictionary_key"),
                    "global_dictionary_value": canonical_payload.get("global_dictionary_value"),
                    "original_text": canonical_payload.get("original_text"),
                }
            ]

        polizas = canonical_payload.get("polizas") or []

        return [
            self._build_sap_payload(
                poliza=poliza,
                canonical_payload=canonical_payload,
            )
            for poliza in polizas
        ]

    def _parse_header(
        self,
        line: str,
        source_file: str,
        trans_uuid: str,
    ) -> dict[str, Any]:
        """
        Regex NiFi H:
        ^(H.{12})(.{3})(.{8})(.{8})(.{2})(.{4})(.{16})(.{25})(.{0,5})$
        """

        idext_raw = line[0:13].strip()
        idext = idext_raw[1:13].strip() if idext_raw.startswith("H") else idext_raw

        return {
            "IDEXT": idext,
            "IDEXT_H": idext_raw,
            "uuid": trans_uuid,
            "Amex": "AMEX",
            "Head.1": line[13:16].strip(),
            "Head.2": line[16:24].strip(),
            "Head.3": line[24:32].strip(),
            "Head.4": line[32:34].strip(),
            "Head.5": line[34:38].strip(),
            "Head.6": line[38:54].strip(),
            "Head.7": line[54:79].strip(),
            "Head.8": line[79:84].strip(),
            "Head.9": line[84:89].strip(),
            "filename": Path(str(source_file)).name,
            "raw": line,
        }

    def _parse_detail(
        self,
        line: str,
        postgres_conn_id: str,
    ) -> dict[str, Any]:
        """
        Regex NiFi D:
        ^(D.{12})(.{3})(.{10})(.{2})(.{13})(.{18})(.{50})(.{10})(.{10})(.{1})(.{2})(.{12})(.{12})(.{20})(.{1})(.{4})(.{8})(.{1})(.{12})(.{8})(.{24})(.{13})(.{36})(.{36})(.*)$

        Equivalencia de campos NiFi más relevante:
        - Grupo 1 / IDEXT_D: DAXM...
        - Desc.2 NiFi: Numberoflineitem
        - Desc.3 NiFi: Glaccount
        - Desc.4 NiFi: Postingkey
        - Desc.8 NiFi: Costcenter
        """

        idext_d = line[0:13].strip()
        idext = line[1:13].strip() if idext_d.startswith("D") else idext_d

        numberoflineitem = line[13:16].strip()
        gl_original = line[16:26].strip()
        posting_key = line[26:28].strip()
        amount = line[28:41].strip()
        assignmentnumber = line[41:59].strip()
        documentitemtext = line[59:109].strip()
        costcenter_original = line[109:119].strip()

        taxcode = line[130:132].strip()
        reference1 = line[132:144].strip()
        reference2 = line[144:156].strip()
        reference3 = line[156:176].strip()
        specialglcode = line[176:177].strip()
        paymentterms = line[177:181].strip()
        due_date = line[181:189].strip()
        payment_blocking = line[189:190].strip()
        wbselement = line[190:202].strip()
        profitcenter = line[202:210].strip()
        taxbaseamount = line[234:247].strip()
        cfdi_zuuid = line[283:319].strip()

        gl_key = gl_original
        gl_value: str | None = None

        # Regla NiFi:
        # RouteOnAttribute: ${Desc.2:toNumber():equals(1)}
        # UpdateAttribute: Desc.3 = 3000326
        if self._safe_int(numberoflineitem) == 1:
            gl_key = "NIFI_RULE_DESC2_EQUALS_1"
            gl_value = "3000326"
        elif posting_key in ("40", "50"):
            gl_key = f"FINANZAS-plan_de_cuentas-{gl_original}"
            gl_value = self._lookup_global_dictionary(
                postgres_conn_id=postgres_conn_id,
                lookupkey=gl_key,
            )

        costcenter_key = f"FINANZAS-ceco-cebe-{costcenter_original}"
        costcenter_value = self._lookup_global_dictionary(
            postgres_conn_id=postgres_conn_id,
            lookupkey=costcenter_key,
        )

        glaccount = gl_value if gl_value is not None else gl_original
        costcenter = costcenter_value if costcenter_value is not None else costcenter_original

        return {
            "IDEXT": idext,
            "IDEXT_D": idext_d,

            # Campos normalizados para armar payload SAP.
            "Numberoflineitem": numberoflineitem,
            "Glaccount": glaccount,
            "GlaccountOriginal": gl_original,
            "GlaccountLookupKey": gl_key,
            "Postingkey": posting_key,
            "Amount": amount,
            "Assignmentnumber": assignmentnumber,
            "Documentitemtext": documentitemtext,
            "Costcenter": costcenter,
            "CostcenterOriginal": costcenter_original,
            "CostcenterLookupKey": costcenter_key,
            "Taxcode": taxcode,
            "Specialglcode": specialglcode,
            "Paymentterms": paymentterms,
            "Duecalculationbasedate": due_date,
            "Paymentblockingreason": payment_blocking,
            "Wbselement": wbselement,
            "Taxbaseamount": taxbaseamount,
            "Profitcenter": profitcenter,
            "Reference1": reference1,
            "Reference2": reference2,
            "Reference3": reference3,
            "CfdiZuuid": cfdi_zuuid,

            # Compatibilidad/trazabilidad con nomenclatura NiFi.
            "Desc.2": numberoflineitem,
            "Desc.3": glaccount,
            "Desc.3_original": gl_original,
            "Desc.3_key": gl_key,
            "Desc.4": posting_key,
            "Desc.5": amount,
            "Desc.6": assignmentnumber,
            "Desc.7": documentitemtext,
            "Desc.8": costcenter,
            "Desc.8_original": costcenter_original,
            "Desc.8_key": costcenter_key,
            "Desc.10": taxcode,
            "Desc.11": specialglcode,
            "Desc.12": paymentterms,
            "Desc.13": due_date,
            "Desc.14": payment_blocking,
            "Desc.15": wbselement,
            "Desc.16": taxbaseamount,
            "Desc.17": profitcenter,
            "Desc.18": reference1,
            "Desc.19": reference2,
            "Desc.20": reference3,
            "Desc.21": cfdi_zuuid,
            "raw": line,
        }

    def _group_polizas(
        self,
        headers: list[dict[str, Any]],
        details: list[dict[str, Any]],
        source_file: str,
        sociedad: str,
        trans_uuid: str,
    ) -> list[dict[str, Any]]:

        polizas_by_idext: dict[str, dict[str, Any]] = {}

        for header in headers:
            idext = str(header.get("IDEXT", "")).strip()

            polizas_by_idext[idext] = {
                "IDEXT": idext,
                "TRANS_UUID": trans_uuid,
                "SOURCEID": "AMEX",
                "sociedad": sociedad,
                "source_file": source_file,
                "header": header,
                "details": [],
            }

        for detail in details:
            idext = str(detail.get("IDEXT", "")).strip()

            if idext not in polizas_by_idext:
                polizas_by_idext[idext] = {
                    "IDEXT": idext,
                    "TRANS_UUID": trans_uuid,
                    "SOURCEID": "AMEX",
                    "sociedad": sociedad,
                    "source_file": source_file,
                    "header": None,
                    "details": [],
                }

            polizas_by_idext[idext]["details"].append(detail)

        polizas: list[dict[str, Any]] = []

        for idext, payload in polizas_by_idext.items():
            if payload["header"] is None:
                raise ValueError(f"Póliza {idext} no tiene cabecera H en archivo {source_file}")

            expected_details = self._safe_int(payload["header"].get("Head.1"))
            actual_details = len(payload["details"])

            payload["expected_details"] = expected_details
            payload["actual_details"] = actual_details

            if expected_details and expected_details != actual_details:
                logging.warning(
                    "1037 póliza %s: header indica %s detalles pero se leyeron %s",
                    idext,
                    expected_details,
                    actual_details,
                )

            # Ordena por número de partida para igualar salida de NiFi.
            payload["details"] = sorted(
                payload["details"],
                key=lambda x: self._safe_int(x.get("Numberoflineitem")) or 0,
            )

            polizas.append(payload)

        # Ordena por Idext para salida estable.
        return sorted(polizas, key=lambda x: str(x.get("IDEXT", "")))

    def _build_sap_payload(
        self,
        poliza: dict[str, Any],
        canonical_payload: dict[str, Any],
    ) -> dict[str, Any]:

        header = poliza["header"]
        details = poliza.get("details") or []

        trans_uuid = poliza.get("TRANS_UUID") or canonical_payload.get("trans_uuid")
        idext = poliza.get("IDEXT")
        source_file = Path(
            str(poliza.get("source_file") or canonical_payload.get("source_file") or "")
        ).name

        cfdi_uuid = ""
        for item in details:
            if item.get("CfdiZuuid"):
                cfdi_uuid = item.get("CfdiZuuid", "")
                break

        return {
            "TransUuid": trans_uuid,
            "Idext": idext,
            "Sourceid": poliza.get("SOURCEID", "AMEX"),
            "Accountingdocumenttype": header.get("Head.4", ""),
            "Companycode": header.get("Head.5", ""),
            "Totalnumberoflineitem": header.get("Head.1", ""),
            "Documentdate": header.get("Head.2", ""),
            "Postingdate": header.get("Head.3", ""),
            "Documentreferenceid": header.get("Head.6", ""),
            "Accountingdocumentheadertext": header.get("Head.7", ""),
            "Currency": header.get("Head.8", ""),
            "Exchangerate": None,
            "CfdiUuid": cfdi_uuid,
            "Filename": source_file,
            "NavHeaderToItem": [
                {
                    "TransUuid": trans_uuid,
                    "Idext": idext,
                    "Numberoflineitem": item.get("Numberoflineitem", item.get("Desc.2", "")),
                    "Glaccount": item.get("Glaccount", item.get("Desc.3", "")),
                    "Postingkey": item.get("Postingkey", item.get("Desc.4", "")),
                    "Amount": item.get("Amount", item.get("Desc.5", "")),
                    "Assignmentnumber": item.get("Assignmentnumber", item.get("Desc.6", "")),
                    "Documentitemtext": item.get("Documentitemtext", item.get("Desc.7", "")),
                    "Costcenter": item.get("Costcenter", item.get("Desc.8", "")),
                    "Taxcode": item.get("Taxcode", item.get("Desc.10", "")),
                    "Specialglcode": item.get("Specialglcode", item.get("Desc.11", "")),
                    "Paymentterms": item.get("Paymentterms", item.get("Desc.12", "")),
                    "Duecalculationbasedate": item.get(
                        "Duecalculationbasedate",
                        item.get("Desc.13", ""),
                    ),
                    "Paymentblockingreason": item.get(
                        "Paymentblockingreason",
                        item.get("Desc.14", ""),
                    ),
                    "Wbselement": item.get("Wbselement", item.get("Desc.15", "")),
                    "Taxbaseamount": item.get("Taxbaseamount", item.get("Desc.16", "")),
                    "Profitcenter": item.get("Profitcenter", item.get("Desc.17", "")),
                    "Reference1": item.get("Reference1", item.get("Desc.18", "")),
                    "Reference2": item.get("Reference2", item.get("Desc.19", "")),
                    "Reference3": item.get("Reference3", item.get("Desc.20", "")),
                    "CfdiZuuid": item.get("CfdiZuuid", item.get("Desc.21", "")),
                }
                for item in details
            ],
        }

    def _lookup_global_dictionary(
        self,
        postgres_conn_id: str,
        lookupkey: str,
    ) -> str | None:

        if not lookupkey:
            return None

        hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        sql = """
            SELECT target_value
            FROM ctrlplane.tbl_cat_gd
            WHERE lookupkey = %s
            LIMIT 1
        """

        row = hook.get_first(sql, parameters=(lookupkey,))

        if not row:
            logging.warning("No se encontró lookupkey en tbl_cat_gd: %s", lookupkey)
            return None

        return str(row[0]).strip()

    def _decode_file(self, file_bytes: bytes) -> str:
        for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                return file_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue

        return file_bytes.decode("utf-8", errors="ignore")

    def _safe_int(self, value: Any) -> int | None:
        try:
            value = str(value).strip()

            if not value:
                return None

            return int(value)

        except Exception:
            return None
