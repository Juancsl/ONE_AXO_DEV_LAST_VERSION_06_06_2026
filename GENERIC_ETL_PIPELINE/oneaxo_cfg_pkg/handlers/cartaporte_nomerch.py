import json
import os
from io import BytesIO
from collections import defaultdict
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

import requests
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from openpyxl import Workbook

from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.transformers import transform_output_to_target_format


AWS_CONN_ID = "one_axo_s3"
OUT_BUCKET_NAME = "one-axo-out"

TOKEN_URL = Variable.get(
    "SAP_TOKEN_URL",
    default_var=os.getenv(
        "SAP_TOKEN_URL",
        "https://dev-integraciones.grupoaxo.com/token/realms/axo/protocol/openid-connect/token",
    ),
)
GRAPHQL_URL = Variable.get(
    "SAP_GRAPHQL_URL",
    default_var=os.getenv(
        "SAP_GRAPHQL_URL",
        "https://dev-integraciones.grupoaxo.com/logistics-sap-connector/graphql",
    ),
)

SAP_CLIENT_ID = Variable.get("SAP_CLIENT_ID", default_var=os.getenv("SAP_CLIENT_ID", "sap-client"))
SAP_CLIENT_SECRET = Variable.get("SAP_CLIENT_SECRET", default_var=os.getenv("SAP_CLIENT_SECRET", ""))
SAP_USERNAME = Variable.get("SAP_USERNAME", default_var=os.getenv("SAP_USERNAME", "logistics"))
SAP_PASSWORD = Variable.get("SAP_PASSWORD", default_var=os.getenv("SAP_PASSWORD", ""))


XLSX_HEADERS = [
    "NumeroTransporte",
    "Idorigen",
    "RFCRemitente",
    "NombreRemitente",
    "NumRegldTrib",
    "ResidenciaFiscal",
    "NumEstacion",
    "NombreEstacion",
    "NavegacionTrafico",
    "FechaHoraSalida",
    "IDDestino",
    "RFCDestinatario",
    "NombreDestinatario",
    "Calle",
    "NumeroExterior",
    "NumeroInterior",
    "Colonia",
    "Localidad",
    "Referencia",
    "Municipio",
    "Estado",
    "Pais",
    "CodigoPostal",
    "NumPedimento",
    "NumTotalMercancias",
    "BienesTransp",
    "ClaveUnidad",
    "Unidad",
    "PesoEnKg",
    "CvesTransporte",
    "Cantidad",
    "ValorMercancia",
    "Moneda",
    "Calle_Origen",
    "NumeroExterior_Origen",
    "NumeroInterior_Origen",
    "Colonia_Origen",
    "Localidad_Origen",
    "Referencia_Origen",
    "Municipio_Origen",
    "Estado_Origen",
    "Pais_Origen",
    "CodigoPostal_Origen",
]


def safe_str(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_number(value):
    text = safe_str(value)
    if not text:
        return 0.0

    text = text.replace(",", "").replace("$", "").strip()

    try:
        return float(text)
    except Exception:
        return 0.0


def format_number(value):
    value = parse_number(value)
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}"


def format_id(value, prefix):
    value = safe_str(value)[-6:].zfill(6)
    return f"{prefix}{value}"


def format_bp(value):
    value = safe_str(value)[-10:].zfill(10)
    return value


def flatten_list(items):
    flat = []
    for item in items:
        if isinstance(item, list):
            flat.extend(flatten_list(item))
        else:
            flat.append(item)
    return flat


def get_value(data, key):
    for item in data:
        if isinstance(item, dict) and item.get("name") == key:
            return item.get("value", "")
    return ""


def get_paval_for_party(rows, target_party):
    current_paval = ""

    for item in rows:
        if not isinstance(item, dict):
            continue

        if item.get("name") == "PAVAL":
            current_paval = item.get("value", "")
        elif item.get("name") == "PARTY":
            if item.get("value") == target_party:
                return current_paval
            current_paval = ""

    return ""


def _record_to_dict(record):
    if isinstance(record, dict):
        return record
    if hasattr(record, "model_dump"):
        return record.model_dump()
    if hasattr(record, "dict"):
        return record.dict()
    return dict(record)


# =========================
# SAP / GRAPHQL
# =========================
def get_token():
    if not SAP_CLIENT_SECRET or not SAP_PASSWORD:
        raise ValueError(
            "Faltan credenciales SAP. Configura Airflow Variables SAP_CLIENT_SECRET y SAP_PASSWORD."
        )

    payload = {
        "client_id": SAP_CLIENT_ID,
        "client_secret": SAP_CLIENT_SECRET,
        "grant_type": "password",
        "username": SAP_USERNAME,
        "password": SAP_PASSWORD,
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(TOKEN_URL, data=payload, headers=headers, timeout=60)
    response.raise_for_status()

    return response.json()["access_token"]


def sap_query(table_name, filters, token, fields=None):
    query = """
    query sapQuery($tableName: String, $fields: [String], $filters: [FilterInput]) {
      sapQuery(tableName: $tableName, fields: $fields, filters: $filters) {
        name
        value
      }
    }
    """

    body = {
        "query": query,
        "variables": {
            "tableName": table_name,
            "fields": fields or [],
            "filters": filters,
        },
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.post(GRAPHQL_URL, json=body, headers=headers, timeout=60)
    response.raise_for_status()

    payload = response.json()

    if "errors" in payload:
        raise ValueError(f"GraphQL error en {table_name}: {payload['errors']}")

    return flatten_list(payload.get("data", {}).get("sapQuery", []))


def resolve_bukrs_and_adrnr_from_werks(werks, token):
    t001w = sap_query(
        "T001W",
        [{"key": "WERKS", "value": safe_str(werks)}],
        token,
        ["EKORG", "ADRNR"],
    )

    ekorg = get_value(t001w, "EKORG")
    adrnr = get_value(t001w, "ADRNR")

    if not ekorg:
        return "", adrnr

    t024e = sap_query(
        "T024E",
        [{"key": "EKORG", "value": ekorg}],
        token,
        ["BUKRS"],
    )

    bukrs = get_value(t024e, "BUKRS")
    return bukrs, adrnr


def resolve_ekorg_from_werks(werks, token):
    t001w = sap_query(
        "T001W",
        [{"key": "WERKS", "value": safe_str(werks)}],
        token,
        ["EKORG"],
    )

    return get_value(t001w, "EKORG")


def resolve_rfc_from_bukrs(bukrs, token):
    if not bukrs:
        return ""

    t001z = sap_query(
        "T001Z",
        [{"key": "BUKRS", "value": bukrs}],
        token,
        ["PAVAL", "PARTY"],
    )

    return get_paval_for_party(t001z, "MX_RFC")


def resolve_nombre_remitente_from_bukrs(bukrs, token):
    if not bukrs:
        return ""

    t001 = sap_query(
        "T001",
        [{"key": "BUKRS", "value": bukrs}],
        token,
        ["BUTXT"],
    )

    return get_value(t001, "BUTXT")


def resolve_rfc_destinatario(id_destino, token):
    id_destino_clean = safe_str(id_destino)

    if len(id_destino_clean) == 4 :
        bukrs_dest, _ = resolve_bukrs_and_adrnr_from_werks(id_destino_clean, token)
        return resolve_rfc_from_bukrs(bukrs_dest, token)

    bp = format_bp(id_destino_clean)

    aux = sap_query(
        "I_BUSINESSPARTNERTAXNUMBER",
        [{"key": "BUSINESSPARTNER", "value": bp}],
        token,
        ["BUSINESSPARTNER", "BPTAXTYPE", "BPTAXNUMBER"],
    )

    return get_value(aux, "BPTAXNUMBER")


def resolve_nombre_destinatario(id_destino, token):
    bp = format_bp(id_destino)

    data = sap_query(
        "A_BUSINESSPARTNER",
        [{"key": "BUSINESSPARTNER", "value": bp}],
        token,
        ["BUSINESSPARTNERNAME"],
    )

    return get_value(data, "BUSINESSPARTNERNAME")


def resolve_address_from_adrnr(adrnr, token):
    empty = {
        "Calle": "",
        "NumeroExterior": "",
        "NumeroInterior": "",
        "Colonia": "",
        "Localidad": "",
        "Referencia": "",
        "Municipio": "",
        "Estado": "",
        "Pais": "",
        "CodigoPostal": "",
    }

    if not adrnr:
        return empty

    adrc = sap_query(
        "ADRC",
        [{"key": "ADDRNUMBER", "value": adrnr}],
        token,
        [
            "ADDRNUMBER",
            "NAME1",
            "STREET",
            "STREETCODE",
            "HOUSE_NUM1",
            "MC_CITY1",
            "COUNTRY",
            "REGION",
            "POST_CODE1",
            "CITY1",
            "CITY2",
            "MC_STREET",
        ],
    )

    return {
        "Calle": get_value(adrc, "STREET"),
        "NumeroExterior": get_value(adrc, "STREETCODE"),
        "NumeroInterior": get_value(adrc, "HOUSE_NUM1"),
        "Colonia": get_value(adrc, "CITY2"),
        "Localidad": get_value(adrc, "MC_CITY1"),
        "Referencia": get_value(adrc, "MC_STREET"),
        "Municipio": get_value(adrc, "CITY1"),
        "Estado": get_value(adrc, "REGION"),
        "Pais": get_value(adrc, "COUNTRY"),
        "CodigoPostal": get_value(adrc, "POST_CODE1"),
    }


def resolve_destino_adrnr(id_destino, token):
    id_destino_clean = safe_str(id_destino)

    if len(id_destino_clean) == 4 :
        _, adrnr_destino = resolve_bukrs_and_adrnr_from_werks(id_destino_clean, token)
        return adrnr_destino

    partner = format_bp(id_destino_clean)

    but020 = sap_query(
        "BUT020",
        [{"key": "PARTNER", "value": partner}],
        token,
        ["PARTNER", "ADDRNUMBER"],
    )

    return get_value(but020, "ADDRNUMBER")


# =========================
# XML / XLSX
# =========================
def add_text_node(parent, tag, value):
    node = SubElement(parent, tag)
    node.text = safe_str(value)
    return node


def build_detalle_transporte_xml(group_data):
    detalle = Element("DetalleTransporte")

    main_fields = [
        "NumeroTransporte",
        "Idorigen",
        "RFCRemitente",
        "NombreRemitente",
        "NumRegldTrib",
        "ResidenciaFiscal",
        "NumEstacion",
        "NombreEstacion",
        "NavegacionTrafico",
        "FechaHoraSalida",
        "IDDestino",
        "RFCDestinatario",
        "NombreDestinatario",
        "Calle",
        "NumeroExterior",
        "NumeroInterior",
        "Colonia",
        "Localidad",
        "Referencia",
        "Municipio",
        "Estado",
        "Pais",
        "CodigoPostal",
    ]

    for field in main_fields:
        add_text_node(detalle, field, group_data.get(field, ""))

    bienes_container = SubElement(detalle, "BienesTransporte")

    for bien in group_data.get("Bienes", []):
        bien_node = SubElement(bienes_container, "BienesTransporte")

        bienes_fields = [
            "NumPedimento",
            "NumTotalMercancias",
            "BienesTransp",
            "ClaveUnidad",
            "Unidad",
            "PesoEnKg",
        ]

        for field in bienes_fields:
            add_text_node(bien_node, field, bien.get(field, ""))

    final_fields = [
        "CvesTransporte",
        "Cantidad",
        "ValorMercancia",
        "Moneda",
        "Calle_Origen",
        "NumeroExterior_Origen",
        "NumeroInterior_Origen",
        "Colonia_Origen",
        "Localidad_Origen",
        "Referencia_Origen",
        "Municipio_Origen",
        "Estado_Origen",
        "Pais_Origen",
        "CodigoPostal_Origen",
    ]

    for field in final_fields:
        add_text_node(detalle, field, group_data.get(field, ""))

    return detalle


def build_xml_bytes(detalle_nodes):
    if len(detalle_nodes) == 1:
        root = detalle_nodes[0]
    else:
        root = Element("DetalleTransportes")
        for node in detalle_nodes:
            root.append(node)

    xml_bytes = tostring(root, encoding="utf-8")
    return minidom.parseString(xml_bytes).toprettyxml(indent="  ", encoding="utf-8")


def build_xlsx_rows(group_data):
    rows = []
    base = {header: group_data.get(header, "") for header in XLSX_HEADERS}

    for bien in group_data.get("Bienes", []):
        row = dict(base)

        for field in [
            "NumPedimento",
            "NumTotalMercancias",
            "BienesTransp",
            "ClaveUnidad",
            "Unidad",
            "PesoEnKg",
        ]:
            row[field] = bien.get(field, "")

        rows.append(row)

    return rows


def build_xlsx_bytes(rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "Transporte"

    ws.append(XLSX_HEADERS)

    for row in rows:
        ws.append([row.get(header, "") for header in XLSX_HEADERS])

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)

    return bio.getvalue()


def _s3_put_bytes(s3_hook: S3Hook, bucket_name: str, key: str, payload: bytes, content_type: str | None = None):
    extra = {"ContentType": content_type} if content_type else {}
    s3_hook.get_conn().put_object(
        Bucket=bucket_name,
        Key=key,
        Body=payload,
        **extra,
    )


# =========================
# HANDLER
# =========================
class CartaPorteNoMerchPayloadBuilder:
    def build(self, canonical_records, integration_config):
        records = [_record_to_dict(record) for record in canonical_records]

        if not records:
            integration_config["target_format"] = "json"
            return transform_output_to_target_format({}, "json", {})

        token = get_token()

        grouped = defaultdict(list)

        # Agrupación principal del documento.
        # Nota: IDDestino NO forma parte de la llave, porque aunque venga diferente
        # se debe generar un solo XML/XLSX y tomar el primer destino del grupo.
        for record in records:
            key = (
                safe_str(record.get("BURKS")),
                safe_str(record.get("Transportista")),
                safe_str(record.get("Idorigen")),
                safe_str(record.get("FechaHoraSalida")),
            )
            grouped[key].append(record)

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        integration_id = integration_config.get("integration_id") or "R092B_NOMERCH"

        control_payloads = []

        for (burks, transportista, id_origen, fecha_hora_salida), rows in grouped.items():
            # Si vienen varios IDDestino dentro del mismo grupo, se usa el primero
            # para los datos de destinatario/dirección en XML y XLSX.
            id_destino = safe_str(rows[0].get("IDDestino", ""))

            bukrs_origen, adrnr_origen = resolve_bukrs_and_adrnr_from_werks(id_origen, token)
            ekorg_origen = resolve_ekorg_from_werks(id_origen, token)
            rfc_rem = resolve_rfc_from_bukrs(bukrs_origen, token)
            nombre_rem = resolve_nombre_remitente_from_bukrs(bukrs_origen, token)
            direccion_origen = resolve_address_from_adrnr(adrnr_origen, token)

            adrnr_destino = resolve_destino_adrnr(id_destino, token)
            rfc_dest = resolve_rfc_destinatario(id_destino, token)
            nombre_dest = resolve_nombre_destinatario(id_destino, token)
            direccion_dest = resolve_address_from_adrnr(adrnr_destino, token)

            agrupacion_bienes = {}
            total_valor = 0.0

            for row in rows:
                pedimento = safe_str(row.get("NumPedimento", ""))
                bienes_transp = safe_str(row.get("BienesTransp", ""))
                cantidad_num = parse_number(row.get("Cantidad", ""))
                valor_num = parse_number(row.get("ValorMercancia", ""))

                key = (pedimento, bienes_transp)

                if key not in agrupacion_bienes:
                    agrupacion_bienes[key] = {
                        "NumPedimento": pedimento,
                        "NumTotalMercancias": 0.0,
                        "BienesTransp": bienes_transp,
                        "ClaveUnidad": safe_str(row.get("ClaveUnidad", "")) or "H87",
                        "Unidad": safe_str(row.get("Unidad", "")),
                        "PesoEnKg": safe_str(row.get("PesoEnKg", "")),
                    }

                agrupacion_bienes[key]["NumTotalMercancias"] += cantidad_num
                total_valor += valor_num

            bienes_rows = []
            total_cantidad = 0.0

            for bien in agrupacion_bienes.values():
                total_cantidad += bien["NumTotalMercancias"]

                bienes_rows.append(
                    {
                        "NumPedimento": bien["NumPedimento"],
                        "NumTotalMercancias": format_number(bien["NumTotalMercancias"]),
                        "BienesTransp": bien["BienesTransp"],
                        "ClaveUnidad": bien["ClaveUnidad"],
                        "Unidad": bien["Unidad"],
                        "PesoEnKg": bien["PesoEnKg"],
                    }
                )

            numero_transporte = transportista
            numero_transporte_gd = safe_str(rows[0].get("NumeroTransporte_GD", ""))
            # El prefijo del archivo debe venir de T001W.EKORG, no de BUKRS/BURKS.
            org = ekorg_origen

            if not org:
                raise ValueError("EKORG vacío. No se puede construir el nombre del archivo.")

            group_data = {
                "NumeroTransporte": numero_transporte,
                "Idorigen": format_id(id_origen, "OR"),
                "RFCRemitente": rfc_rem,
                "NombreRemitente": nombre_rem,
                "NumRegldTrib": "",
                "ResidenciaFiscal": "",
                "NumEstacion": "",
                "NombreEstacion": "",
                "NavegacionTrafico": "",
                "FechaHoraSalida": fecha_hora_salida,
                "IDDestino": format_id(id_destino, "DE"),
                "RFCDestinatario": rfc_dest,
                "NombreDestinatario": nombre_dest,
                "Calle": direccion_dest.get("Calle", ""),
                "NumeroExterior": direccion_dest.get("NumeroExterior", ""),
                "NumeroInterior": direccion_dest.get("NumeroInterior", ""),
                "Colonia": direccion_dest.get("Colonia", ""),
                "Localidad": direccion_dest.get("Localidad", ""),
                "Referencia": direccion_dest.get("Referencia", ""),
                "Municipio": direccion_dest.get("Municipio", ""),
                "Estado": direccion_dest.get("Estado", ""),
                "Pais": direccion_dest.get("Pais", ""),
                "CodigoPostal": direccion_dest.get("CodigoPostal", ""),
                "Bienes": bienes_rows,
                "CvesTransporte": "01",
                "Cantidad": format_number(total_cantidad),
                "ValorMercancia": format_number(total_valor),
                "Moneda": "MXN",
                "Calle_Origen": direccion_origen.get("Calle", ""),
                "NumeroExterior_Origen": direccion_origen.get("NumeroExterior", ""),
                "NumeroInterior_Origen": direccion_origen.get("NumeroInterior", ""),
                "Colonia_Origen": direccion_origen.get("Colonia", ""),
                "Localidad_Origen": direccion_origen.get("Localidad", ""),
                "Referencia_Origen": direccion_origen.get("Referencia", ""),
                "Municipio_Origen": direccion_origen.get("Municipio", ""),
                "Estado_Origen": direccion_origen.get("Estado", ""),
                "Pais_Origen": direccion_origen.get("Pais", ""),
                "CodigoPostal_Origen": direccion_origen.get("CodigoPostal", ""),
            }

            xml_bytes = build_xml_bytes([build_detalle_transporte_xml(group_data)])
            xlsx_bytes = build_xlsx_bytes(build_xlsx_rows(group_data))

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            base_filename = f"{org}_{numero_transporte}_{timestamp}"

            xml_out_s3_key = f"{integration_id}/out/xml/{base_filename}.xml"
            xlsx_out_s3_key = f"{integration_id}/out/xlsx/{base_filename}.xlsx"

            _s3_put_bytes(
                s3_hook=s3_hook,
                bucket_name=OUT_BUCKET_NAME,
                key=xml_out_s3_key,
                payload=xml_bytes,
                content_type="application/xml",
            )

            _s3_put_bytes(
                s3_hook=s3_hook,
                bucket_name=OUT_BUCKET_NAME,
                key=xlsx_out_s3_key,
                payload=xlsx_bytes,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            control_payloads.append(
                {
                    "numero_transporte": numero_transporte,
                    "transportista": transportista,
                    "numero_transporte_gd": numero_transporte_gd,
                    "tipo_operacion": "NOMERCH",
                    "xml_out_s3_key": xml_out_s3_key,
                    "xlsx_out_s3_key": xlsx_out_s3_key,
                }
            )

        integration_config["target_format"] = "json"

        if len(control_payloads) == 1:
            return transform_output_to_target_format(control_payloads[0], "json", {})

        return transform_output_to_target_format(control_payloads, "json", {})