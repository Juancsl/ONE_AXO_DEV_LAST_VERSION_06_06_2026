import os
import re
import requests
import pandas as pd

from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

from airflow import DAG
from airflow.operators.python import PythonOperator
from openpyxl import Workbook


# =========================
# CONFIG
# =========================
DAG_ID = "carta_porte_merch_v1"

INPUT_DIR = "/opt/airflow/data/input"
FILE_NAME = "CartaPorte NOMERCH.xlsx"
OUTPUT_DIR = "/opt/airflow/data/output"
OUTPUT_FILE = "output.xml"
OUTPUT_XLSX_FILE = "output.xlsx"

TOKEN_URL = "https://dev-integraciones.grupoaxo.com/token/realms/axo/protocol/openid-connect/token"
GRAPHQL_URL = "https://dev-integraciones.grupoaxo.com/logistics-sap-connector/graphql"

SAP_CLIENT_ID = "sap-client"
SAP_CLIENT_SECRET = "Q2546eg0YDO6HZb0DhgkYzclKCpdl2Ei"
SAP_USERNAME = "logistics"
SAP_PASSWORD = "6X6Ohcb5NZ11KDHr"


# =========================
# HELPERS GENERALES
# =========================


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
    if pd.isna(value):
        return ""
    return str(value).strip()


def format_id(value, prefix):
    value = safe_str(value)
    value = value[-6:]
    value = value.zfill(6)
    return f"{prefix}{value}"


def format_bp(value):
    value = safe_str(value)
    value = value[-10:]
    value = value.zfill(10)
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


def parse_number(value):
    text = safe_str(value)
    if not text:
        return 0.0

    text = text.replace(",", "")
    text = text.replace("$", "")
    text = text.strip()

    try:
        return float(text)
    except Exception:
        return 0.0


def format_number(value):
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}"


# =========================
# SAP / GRAPHQL
# =========================
def get_token():
    payload = {
        "client_id": SAP_CLIENT_ID,
        "client_secret": SAP_CLIENT_SECRET,
        "grant_type": "password",
        "username": SAP_USERNAME,
        "password": SAP_PASSWORD,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(TOKEN_URL, data=payload, headers=headers, timeout=60)

    print("TOKEN STATUS:", response.status_code)
    print("TOKEN RESPONSE:", response.text)

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

    print(f"{table_name} STATUS:", response.status_code)
    print(f"{table_name} RESPONSE:", response.text)

    response.raise_for_status()
    payload = response.json()

    if "errors" in payload:
        raise ValueError(f"GraphQL error en {table_name}: {payload['errors']}")

    raw_data = payload.get("data", {}).get("sapQuery", [])
    return flatten_list(raw_data)


# =========================
# SAP LOGIC
# =========================
def resolve_bukrs_and_adrnr_from_werks(werks, token):
    t001w = sap_query(
        "T001W",
        [{"key": "WERKS", "value": werks}],
        token,
        ["EKORG", "ADRNR"]
    )

    ekorg = get_value(t001w, "EKORG")
    adrnr = get_value(t001w, "ADRNR")

    if not ekorg:
        return "", adrnr

    t024e = sap_query(
        "T024E",
        [{"key": "EKORG", "value": ekorg}],
        token,
        ["BUKRS"]
    )

    bukrs = get_value(t024e, "BUKRS")
    return bukrs, adrnr


def resolve_rfc_from_bukrs(bukrs, token):
    if not bukrs:
        return ""

    t001z = sap_query(
        "T001Z",
        [{"key": "BUKRS", "value": bukrs}],
        token,
        ["PAVAL", "PARTY"]
    )

    return get_paval_for_party(t001z, "MX_RFC")


def resolve_nombre_remitente_from_bukrs(bukrs, token):
    if not bukrs:
        return ""

    t001 = sap_query(
        "T001",
        [{"key": "BUKRS", "value": bukrs}],
        token,
        ["BUTXT"]
    )

    return get_value(t001, "BUTXT")


def resolve_rfc_destinatario(id_destino, token):
    id_destino_clean = safe_str(id_destino)

    if len(id_destino_clean) == 4 and id_destino_clean.isdigit():
        bukrs_dest, _ = resolve_bukrs_and_adrnr_from_werks(id_destino_clean, token)
        return resolve_rfc_from_bukrs(bukrs_dest, token)

    bp = format_bp(id_destino_clean)

    aux = sap_query(
        "I_BUSINESSPARTNERAUXNUMBER",
        [{"key": "BUSINESSPARTNER", "value": bp}],
        token,
        ["BUSINESSPARTNER", "BPTAXTYPE", "BPTAXNUMBER"]
    )

    return get_value(aux, "BPTAXNUMBER")


def resolve_nombre_destinatario(id_destino, token):
    bp = format_bp(id_destino)

    data = sap_query(
        "A_BUSINESSPARTNER",
        [{"key": "BUSINESSPARTNER", "value": bp}],
        token,
        ["BUSINESSPARTNERNAME"]
    )

    return get_value(data, "BUSINESSPARTNERNAME")


def resolve_address_from_adrnr(adrnr, token):
    if not adrnr:
        return {
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


# =========================
# XML HELPERS
# =========================
def add_text_node(parent, tag, value):
    node = SubElement(parent, tag)
    node.text = safe_str(value)
    return node


def build_detalle_transporte_xml(group_data):
    detalle = Element("DetalleTransporte")

    # Campos principales
    main_fields = [
        "NumeroTransporte",
        "Idorigen",
        "RFCRemitente",
        "NombreRemitente",
        "NumRegIdTrib",
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

    # BienesTransporte container
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

    # Campos finales
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




def build_xlsx_rows(group_data):
    rows = []
    shared_data = {
        "NumeroTransporte": group_data.get("NumeroTransporte", ""),
        "Idorigen": group_data.get("Idorigen", ""),
        "RFCRemitente": group_data.get("RFCRemitente", ""),
        "NombreRemitente": group_data.get("NombreRemitente", ""),
        "NumRegldTrib": group_data.get("NumRegIdTrib", ""),
        "ResidenciaFiscal": group_data.get("ResidenciaFiscal", ""),
        "NumEstacion": group_data.get("NumEstacion", ""),
        "NombreEstacion": group_data.get("NombreEstacion", ""),
        "NavegacionTrafico": group_data.get("NavegacionTrafico", ""),
        "FechaHoraSalida": group_data.get("FechaHoraSalida", ""),
        "IDDestino": group_data.get("IDDestino", ""),
        "RFCDestinatario": group_data.get("RFCDestinatario", ""),
        "NombreDestinatario": group_data.get("NombreDestinatario", ""),
        "Calle": group_data.get("Calle", ""),
        "NumeroExterior": group_data.get("NumeroExterior", ""),
        "NumeroInterior": group_data.get("NumeroInterior", ""),
        "Colonia": group_data.get("Colonia", ""),
        "Localidad": group_data.get("Localidad", ""),
        "Referencia": group_data.get("Referencia", ""),
        "Municipio": group_data.get("Municipio", ""),
        "Estado": group_data.get("Estado", ""),
        "Pais": group_data.get("Pais", ""),
        "CodigoPostal": group_data.get("CodigoPostal", ""),
        "CvesTransporte": group_data.get("CvesTransporte", ""),
        "Cantidad": group_data.get("Cantidad", ""),
        "ValorMercancia": group_data.get("ValorMercancia", ""),
        "Moneda": group_data.get("Moneda", ""),
        "Calle_Origen": group_data.get("Calle_Origen", ""),
        "NumeroExterior_Origen": group_data.get("NumeroExterior_Origen", ""),
        "NumeroInterior_Origen": group_data.get("NumeroInterior_Origen", ""),
        "Colonia_Origen": group_data.get("Colonia_Origen", ""),
        "Localidad_Origen": group_data.get("Localidad_Origen", ""),
        "Referencia_Origen": group_data.get("Referencia_Origen", ""),
        "Municipio_Origen": group_data.get("Municipio_Origen", ""),
        "Estado_Origen": group_data.get("Estado_Origen", ""),
        "Pais_Origen": group_data.get("Pais_Origen", ""),
        "CodigoPostal_Origen": group_data.get("CodigoPostal_Origen", ""),
    }

    bienes = group_data.get("Bienes", [])
    for bien in bienes:
        row = dict(shared_data)
        row.update({
            "NumPedimento": bien.get("NumPedimento", ""),
            "NumTotalMercancias": bien.get("NumTotalMercancias", ""),
            "BienesTransp": bien.get("BienesTransp", ""),
            "ClaveUnidad": bien.get("ClaveUnidad", ""),
            "Unidad": bien.get("Unidad", ""),
            "PesoEnKg": bien.get("PesoEnKg", ""),
        })
        rows.append(row)

    return rows


def write_output_xlsx(rows, output_path):
    wb = Workbook()
    ws = wb.active
    ws.title = "Transporte"
    ws.append(XLSX_HEADERS)

    for row in rows:
        ws.append([safe_str(row.get(header, "")) for header in XLSX_HEADERS])

    wb.save(output_path)

# =========================
# TASK
# =========================
def process_file():
    file_path = os.path.join(INPUT_DIR, FILE_NAME)
    print("INPUT FILE:", file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No existe el archivo de entrada: {file_path}")

    df = pd.read_excel(file_path, dtype=str).fillna("")
    df.columns = [safe_str(c) for c in df.columns]

    required_cols = [
        "Transportista",
        "IdOrigen",
        "FechaHoraSalida",
        "IdDestino",
        "Pedimento",
        "BienesTransp",
        "Cantidad",
        "ValorMercancia",
        "Unidad",
        "PesoEnKg",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    token = get_token()

    group_keys = ["Transportista", "IdOrigen", "FechaHoraSalida", "IdDestino"]
    grouped = df.groupby(group_keys, dropna=False, sort=False)

    detalle_nodes = []
    xlsx_rows = []

    for group_key, group_df in grouped:
        transportista, id_origen, fecha_hora_salida, id_destino = group_key

        id_origen = safe_str(id_origen)
        id_destino = safe_str(id_destino)
        transportista = safe_str(transportista)
        fecha_hora_salida = safe_str(fecha_hora_salida)

        # ORIGEN
        bukrs_origen, adrnr_origen = resolve_bukrs_and_adrnr_from_werks(id_origen, token)
        rfc_rem = resolve_rfc_from_bukrs(bukrs_origen, token)
        nombre_rem = resolve_nombre_remitente_from_bukrs(bukrs_origen, token)
        direccion_origen = resolve_address_from_adrnr(adrnr_origen, token)

        # DESTINO
        _, adrnr_destino = resolve_bukrs_and_adrnr_from_werks(id_destino, token)
        rfc_dest = resolve_rfc_destinatario(id_destino, token)
        nombre_dest = resolve_nombre_destinatario(id_destino, token)
        direccion_dest = resolve_address_from_adrnr(adrnr_destino, token)

        # Bienes
        bienes_rows = []
        total_items = len(group_df)
        total_cantidad = 0.0
        total_valor = 0.0

        for _, row in group_df.iterrows():
            cantidad_num = parse_number(row.get("Cantidad", ""))
            valor_num = parse_number(row.get("ValorMercancia", ""))

            total_cantidad += cantidad_num
            total_valor += valor_num

            bienes_rows.append(
                {
                    "NumPedimento": safe_str(row.get("Pedimento", "")),
                    "NumTotalMercancias": str(total_items),
                    "BienesTransp": safe_str(row.get("BienesTransp", "")),
                    "ClaveUnidad": "H87",
                    "Unidad": safe_str(row.get("Unidad", "")),
                    "PesoEnKg": safe_str(row.get("PesoEnKg", "")),
                }
            )

        group_data = {
            "NumeroTransporte": transportista,
            "Idorigen": format_id(id_origen, "OR"),
            "RFCRemitente": rfc_rem,
            "NombreRemitente": nombre_rem,
            "NumRegIdTrib": "",
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

        detalle_nodes.append(build_detalle_transporte_xml(group_data))
        xlsx_rows.extend(build_xlsx_rows(group_data))

    # Si solo hay un transporte, raíz singular como tu ejemplo
    if len(detalle_nodes) == 1:
        root = detalle_nodes[0]
    else:
        root = Element("DetalleTransportes")
        for node in detalle_nodes:
            root.append(node)

    xml_bytes = tostring(root, encoding="utf-8")
    pretty_xml = minidom.parseString(xml_bytes).toprettyxml(indent="  ", encoding="utf-8")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    with open(output_file, "wb") as f:
        f.write(pretty_xml)

    print(f"XML generado en: {output_file}")

    output_xlsx_file = os.path.join(OUTPUT_DIR, OUTPUT_XLSX_FILE)
    write_output_xlsx(xlsx_rows, output_xlsx_file)
    print(f"XLSX generado en: {output_xlsx_file}")


default_args = {
    "owner": "axo",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2026, 4, 21),
    schedule=None,
    catchup=False,
    tags=["carta_porte", "merch", "sap"],
) as dag:

    process_task = PythonOperator(
        task_id="process_file",
        python_callable=process_file,
    )