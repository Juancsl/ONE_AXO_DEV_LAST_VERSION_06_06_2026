# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import uuid
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID_DEFAULT = "gobierno_central_postgres"

# ============================================================================
# PRUEBA:
#   Para probar la generación de múltiples documentos usar 300.
#
# PRODUCCIÓN:
#   Cambiar a 999 cuando se libere la regla final del layout.
#
# Regla:
#   - No se rechaza si un grupo supera este número.
#   - Se divide en N documentos.
#   - Cada documento tiene 1 Header + máximo N detalles.
#   - Cada documento renumera sus detalles desde 001.
# ============================================================================
MAX_DETAILS_PER_DOCUMENT = 999

GROUP_TOMMY = "TOMMY"
GROUP_BASECO = "BASECO"


class PolizaNomina212BHandler:

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

        max_details_per_document = self._safe_int(
            integration_config.get("max_details_per_document")
            or raw_job.get("max_details_per_document")
            or MAX_DETAILS_PER_DOCUMENT
        )

        if not max_details_per_document or max_details_per_document < 1:
            raise ValueError(
                f"max_details_per_document inválido: {max_details_per_document}"
            )

        source_file = raw_job.get("source_file") or raw_job.get("filename") or "unknown_file"
        raw_s3_key = raw_job.get("raw_s3_key")

        text = self._decode_file(file_bytes)
        lines = [line.rstrip("\n\r") for line in text.splitlines() if line.strip()]

        if not lines:
            raise ValueError(f"Archivo vacío o sin líneas válidas: {source_file}")

        header_lines = [line for line in lines if line.startswith("H")]
        detail_lines = [line for line in lines if line.startswith("D")]

        if len(header_lines) != 1:
            raise ValueError(
                f"Archivo {source_file} debe tener exactamente 1 header H. "
                f"Headers encontrados: {len(header_lines)}"
            )

        if not detail_lines:
            raise ValueError(f"Archivo {source_file} no contiene detalles D.")

        header_line = header_lines[0]

        if len(header_line) < 38:
            raise ValueError(
                f"Archivo {source_file} no tiene longitud suficiente para leer sociedad. "
                f"Longitud header: {len(header_line)}"
            )

        sociedad = header_line[34:38].strip()
        gd_key = f"FINANZAS-sociedades-{sociedad}"

        society_flag = self._lookup_global_dictionary(
            postgres_conn_id=postgres_conn_id,
            lookupkey=gd_key,
        )

        route = "S4H" if society_flag == "1" else "ECC"
        trans_uuid = str(uuid.uuid4())

        non_neto_detail_lines = [
            line for line in detail_lines
            if not self._is_neto_detail(line)
        ]

        ignored_input_neto_count = len(detail_lines) - len(non_neto_detail_lines)

        if not non_neto_detail_lines:
            raise ValueError(
                f"Archivo {source_file} no contiene detalles válidos para calcular NETO."
            )

        groups = self._split_details_by_group(non_neto_detail_lines)

        # Reservamos una posición por documento para la línea NETO generada.
        # Si max_details_per_document = 999:
        #   998 detalles originales + 1 línea NETO = 999 detalles totales.
        chunk_size = max_details_per_document - 1

        if chunk_size < 1:
            raise ValueError(
                "max_details_per_document debe ser mínimo 2 porque se agrega línea NETO."
            )

        documents: list[dict[str, Any]] = []

        for group, group_details in groups.items():
            if not group_details:
                continue

            chunks = self._chunk_details(
                details=group_details,
                chunk_size=chunk_size,
            )

            original_idext = header_line[1:13].strip()
            self._validate_available_idexts(
                group=group,
                original_idext=original_idext,
                required_documents=len(chunks),
            )

            for chunk_index, chunk_details in enumerate(chunks):
                document_idext = self._increment_idext(original_idext, chunk_index)

                split_txt = self._build_split_txt(
                    header_line=header_line,
                    detail_lines=chunk_details,
                    new_idext=document_idext,
                )
                split_source_file = self._build_split_filename(
                    source_file=source_file,
                    group=group,
                    document_idext=document_idext,
                )

                split_lines = [
                    line.rstrip("\n\r")
                    for line in split_txt.splitlines()
                    if line.strip()
                ]

                polizas = self._parse_s4h_polizas(
                    lines=split_lines,
                    source_file=split_source_file,
                    sociedad=sociedad,
                    trans_uuid=trans_uuid,
                )

                documents.append(
                    {
                        "group": group,
                        "source_file": split_source_file,
                        "detail_count": len(chunk_details) + 1,
                        "txt_content": split_txt,
                        "polizas": polizas,
                        "polizas_count": len(polizas),
                        "document_index": chunk_index + 1,
                        "document_idext": document_idext,
                        "max_details_per_document": max_details_per_document,
                    }
                )

        if not documents:
            raise ValueError(f"Archivo {source_file} no generó documentos de salida.")

        logging.info(
            (
                "212B canonical: source_file=%s sociedad=%s gd_key=%s gd_value=%s "
                "route=%s documents=%s max_details_per_document=%s"
            ),
            source_file,
            sociedad,
            gd_key,
            society_flag,
            route,
            len(documents),
            max_details_per_document,
        )

        return {
            "integration_id": integration_config.get(
                "integration_id",
                "R011B_POLIZA_NOMINA_212_SPLIT",
            ),
            "source_file": source_file,
            "raw_s3_key": raw_s3_key,
            "sociedad": sociedad,
            "global_dictionary_key": gd_key,
            "global_dictionary_value": society_flag,
            "route": route,
            "source_system": "NOM2001",
            "source_id": "NOMINA",
            "trans_uuid": trans_uuid,
            "line_count": len(lines),
            "original_detail_count": len(detail_lines),
            "ignored_input_neto_count": ignored_input_neto_count,
            "max_details_per_document": max_details_per_document,
            "documents": documents,
            "documents_count": len(documents),
        }

    def build_out(
        self,
        canonical_payload: dict[str, Any],
        integration_config: dict[str, Any],
        canonical_job: dict[str, Any] | None = None,
        global_context: dict[str, Any] | None = None,
        **kwargs,
    ) -> dict[str, Any]:

        route = canonical_payload.get("route")
        documents = canonical_payload.get("documents") or []

        if not documents:
            raise ValueError("No hay documentos split en canonical_payload.")

        outputs: list[dict[str, Any]] = []

        for document in documents:
            group = document["group"]
            split_source_file = document["source_file"]
            txt_content = document["txt_content"]
            document_idext = document.get("document_idext") or "IDEXT"
            document_index = document.get("document_index")

            output_suffix_base = f"{group}_{document_idext}"

            # Evidencia del TXT reconstruido por el split.
            # Este output NO se envía a destino; se guarda como archivo .txt en S3.
            # Requiere que el GENERIC respete target_format/folder/send=False.
            outputs.append(
                {
                    "filename_suffix": output_suffix_base,
                    "send": False,
                    "target_format": "txt",
                    "folder": "txt",
                    "payload": txt_content,
                    "metadata": {
                        "route": route,
                        "artifact_type": "TXT_SPLIT",
                        "group": group,
                        "source_file": split_source_file,
                        "sociedad": canonical_payload.get("sociedad"),
                        "detail_count": document.get("detail_count"),
                        "document_index": document_index,
                        "document_idext": document_idext,
                        "max_details_per_document": document.get("max_details_per_document"),
                    },
                }
            )

            if route == "ECC":
                outputs.append(
                    {
                        "filename_suffix": f"{output_suffix_base}_ECC",
                        "payload": {
                            "route": "ECC",
                            "artifact_type": "ECC_TXT",
                            "group": group,
                            "source_file": split_source_file,
                            "raw_s3_key": canonical_payload.get("raw_s3_key"),
                            "sociedad": canonical_payload.get("sociedad"),
                            "global_dictionary_key": canonical_payload.get("global_dictionary_key"),
                            "global_dictionary_value": canonical_payload.get("global_dictionary_value"),
                            "original_text": txt_content,
                            "document_index": document_index,
                            "document_idext": document_idext,
                        },
                    }
                )

            elif route == "S4H":
                polizas = document.get("polizas") or []

                if not polizas:
                    raise ValueError(f"Documento {output_suffix_base} no tiene pólizas para S4H.")

                for index, poliza in enumerate(polizas, start=1):
                    sap_payload = self._build_sap_payload(
                        poliza=poliza,
                        canonical_payload=canonical_payload,
                    )

                    outputs.append(
                        {
                            "filename_suffix": f"{output_suffix_base}_JSON_{index:03d}",
                            "payload": sap_payload,
                        }
                    )

            else:
                raise ValueError(f"Ruta no soportada: {route}")

        return {"outputs": outputs}

    def _classify_group(self, line: str) -> str:
        cost_center = line[109:119].strip()

        if cost_center.startswith("MX003"):
            return GROUP_TOMMY

        return GROUP_BASECO

    def _split_details_by_group(self, detail_lines: list[str]) -> dict[str, list[str]]:
        groups = {
            GROUP_BASECO: [],
            GROUP_TOMMY: [],
        }

        for line in detail_lines:
            group = self._classify_group(line)
            groups[group].append(line)

        return groups

    def _chunk_details(self, details: list[str], chunk_size: int) -> list[list[str]]:
        return [
            details[index:index + chunk_size]
            for index in range(0, len(details), chunk_size)
        ]

    def _validate_available_idexts(
        self,
        group: str,
        original_idext: str,
        required_documents: int,
    ) -> None:
        if required_documents <= 0:
            return

        if len(original_idext) < 2:
            raise ValueError(f"IDEXT inválido para grupo {group}: {original_idext}")

        start_letter = original_idext[-2].upper()

        if not ("A" <= start_letter <= "Z"):
            raise ValueError(
                f"IDEXT {original_idext} inválido para grupo {group}. "
                f"La penúltima posición debe ser una letra A-Z."
            )

        available_documents = ord("Z") - ord(start_letter) + 1

        if required_documents > available_documents:
            raise ValueError(
                f"Grupo {group} requiere {required_documents} documentos partiendo de "
                f"IDEXT {original_idext}. Máximo disponible: {available_documents} "
                f"documentos ({start_letter}-Z)."
            )

    def _increment_idext(self, idext: str, chunk_index: int) -> str:
        """
        Cambia la penúltima posición del IDEXT.

        Ejemplos:
            M003NM09N1A6 + 0 = M003NM09N1A6
            M003NM09N1A6 + 1 = M003NM09N1B6
            M003NM09T1T6 + 1 = M003NM09T1U6
        """
        if len(idext) < 2:
            raise ValueError(f"IDEXT inválido: {idext}")

        start_letter = idext[-2].upper()
        suffix = idext[-1]

        if not ("A" <= start_letter <= "Z"):
            raise ValueError(
                f"IDEXT {idext} inválido. La penúltima posición debe ser A-Z."
            )

        new_ord = ord(start_letter) + chunk_index

        if new_ord > ord("Z"):
            raise ValueError(
                f"No se puede generar IDEXT para {idext}. "
                f"Se excede la letra Z."
            )

        return idext[:-2] + chr(new_ord) + suffix

    def _replace_idext(self, line: str, new_idext: str) -> str:
        """
        Reemplaza el IDEXT en posiciones [1:13] conservando longitud fija de 12.
        """
        return line[:1] + new_idext.ljust(12)[:12] + line[13:]

    def _rebuild_header(self, header_line: str, detail_count: int, new_idext: str) -> str:
        line = self._replace_idext(header_line, new_idext)
        return line[:13] + str(detail_count).zfill(3) + line[16:]

    def _renumber_detail(self, detail_line: str, number: int, new_idext: str) -> str:
        line = self._replace_idext(detail_line, new_idext)
        return line[:13] + str(number).zfill(3) + line[16:]

    def _build_split_txt(
        self,
        header_line: str,
        detail_lines: list[str],
        new_idext: str,
    ) -> str:
        if not detail_lines:
            raise ValueError("No se puede construir TXT sin detalles para calcular NETO.")

        total_detail_count = len(detail_lines) + 1

        new_header = self._rebuild_header(
            header_line=header_line,
            detail_count=total_detail_count,
            new_idext=new_idext,
        )

        new_details = [
            self._renumber_detail(
                detail_line=line,
                number=index,
                new_idext=new_idext,
            )
            for index, line in enumerate(detail_lines, start=1)
        ]

        neto_signed_amount = self._calculate_neto_amount(detail_lines)
        neto_posting_key = self._get_neto_posting_key(neto_signed_amount)

        neto_line = self._build_neto_detail_line(
            template_detail_line=detail_lines[0],
            number=total_detail_count,
            new_idext=new_idext,
            amount=neto_signed_amount,
            posting_key=neto_posting_key,
        )

        return "\n".join([new_header, *new_details, neto_line]) + "\n"

    def _is_neto_detail(self, line: str) -> bool:
        return line.startswith("D") and line[59:109].strip().upper() == "NETO"

    def _calculate_neto_amount(self, detail_lines: list[str]) -> Decimal:
        """
        Regla confirmada para NETO:
            Posting key 40 = positivo
            Posting key 50 = negativo

        Por lo tanto:
            NETO = SUM(40) - SUM(50)

        La línea NETO se escribe con posting key dinámico:
            - Si NETO >= 0, posting key 50
            - Si NETO < 0, posting key 40
        """
        total_40 = Decimal("0.00")
        total_50 = Decimal("0.00")

        for line in detail_lines:
            posting_key = line[26:28].strip()
            amount = self._parse_decimal_amount(line[28:41].strip())

            if posting_key == "40":
                total_40 += amount
            elif posting_key == "50":
                total_50 += amount

        return (total_40 - total_50).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )

    def _get_neto_posting_key(self, amount: Decimal) -> str:
        """
        La línea NETO usa el posting key contrario al signo del balance:
            - NETO positivo o cero -> 50
            - NETO negativo -> 40
        """
        return "50" if amount >= Decimal("0.00") else "40"

    def _build_neto_detail_line(
        self,
        template_detail_line: str,
        number: int,
        new_idext: str,
        amount: Decimal,
        posting_key: str,
    ) -> str:
        """
        Genera la línea final NETO por documento.

        Reglas:
            - Glaccount fijo: 215010
            - Postingkey dinámico: 50 si NETO >= 0, 40 si NETO < 0
            - Importe siempre positivo
            - Texto fijo: NETO
            - Costcenter: el de la primera línea del archivo/grupo generado
        """
        line = self._replace_idext(template_detail_line, new_idext)
        line = line[:13] + str(number).zfill(3) + line[16:]

        amount_text = self._format_amount(amount)
        first_cost_center = template_detail_line[109:119].strip()

        return (
            line[:16]
            + "215010".ljust(10)[:10]
            + posting_key.ljust(2)[:2]
            + amount_text.ljust(13)[:13]
            + new_idext.ljust(18)[:18]
            + "NETO".ljust(50)[:50]
            + first_cost_center.ljust(10)[:10]
            ##+ line[119:]
        )

    def _parse_decimal_amount(self, value: str) -> Decimal:
        value = str(value or "").strip().replace(",", "")

        if not value:
            return Decimal("0.00")

        return Decimal(value)

    def _format_amount(self, amount: Decimal) -> str:
        amount = Decimal(amount).copy_abs().quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )

        value = f"{amount:.2f}"

        if len(value) > 13:
            raise ValueError(
                f"Importe NETO excede longitud del layout Amount[28:41]: {value}"
            )

        return value

    def _build_split_filename(
        self,
        source_file: str,
        group: str,
        document_idext: str,
    ) -> str:
        path = Path(str(source_file))
        stem = path.stem.replace(" ", "_")
        suffix = path.suffix or ".TXT"

        return f"{stem}_{group}_{document_idext}{suffix}"

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
                    "212B póliza %s: header indica %s detalles pero se leyeron %s",
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

    def _safe_int(self, value: Any) -> int | None:
        try:
            value = str(value).strip()

            if not value:
                return None

            return int(value)

        except Exception:
            return None
