from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os
import xml.etree.ElementTree as ET
from openpyxl import Workbook

# ===============================
# RUTAS
# ===============================

BASE_PATH = "/opt/airflow/data"
INPUT_PATH = f"{BASE_PATH}/input/122_origen_outbound.xml"
PROCESSED_PATH = f"{BASE_PATH}/processed"

# ===============================
# DICCIONARIO TRANSPORTISTAS (SOLO OUTBOUND)
# ===============================

TRANSPORTISTAS = {
    "3000001": "500163",
    "3000005": "500682",
    "3000010": "501529",
    "3000024": "502351",
    "3000029": "503052",
    "3000031": "503123",
    "3000041": "503804",
    "3000042": "503809",
    "3000050": "504056",
    "3000055": "504140",
    "3000074": "504584",
    "3000075": "504585",
    "3000089": "500163",
    "3000090": "500682",
    "3000091": "504739",
    "3000092": "502351",
    "3000093": "503052",
    "3000094": "503063",
    "3000095": "503804",
    "3000096": "503809",
    "3000097": "504056",
    "3000098": "504140",
    "3000099": "504488",
    "3000100": "504498",
    "3000101": "504584",
    "3000102": "504585",
    "3000103": "504591",
    "3000104": "504739",
    "3000105": "504757",
    "3000123": "503719",
}

# ===============================
# FUNCIONES AUXILIARES
# ===============================

def obtener_valor(root, tag):
    for elem in root.iter():
        if elem.tag.endswith(tag):
            return elem.text.strip() if elem.text else ""
    return ""

def formatear_fecha(fecha_iso):
    if not fecha_iso:
        return ""
    dt = datetime.strptime(fecha_iso, "%Y-%m-%dT%H:%M:%S")
    return dt.strftime("%d.%m.%Y %H:%M:%S")

# ===============================
# PROCESO PRINCIPAL
# ===============================

def procesar_cartaporte():

    print("==== INICIO PROCESO CARTAPORTE 122 ====")

    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"No existe archivo: {INPUT_PATH}")

    os.makedirs(PROCESSED_PATH, exist_ok=True)

    tree = ET.parse(INPUT_PATH)
    root = tree.getroot()

    # ===============================
    # CLASIFICACIÓN
    # ===============================

    clase = obtener_valor(root, "Clase_FO")

    if not clase:
        raise ValueError("Clase_FO vacío en XML")

    if "ZFIN" in clase:
        tipo_operacion = "INBOUND"
    elif clase in ["ZFON", "ZFOD"]:
        tipo_operacion = "OUTBOUND"
    else:
        raise ValueError(f"Clase no soportada: {clase}")

    print(f"Tipo operación: {tipo_operacion}")

    org = obtener_valor(root, "Org_Compra_Venta")
    numero_transporte = obtener_valor(root, "NumeroTransporte")
    transportista_origen = obtener_valor(root, "Transportista")

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    base_filename = f"{org}_{numero_transporte}_{timestamp}"

    # ===============================
    # VALIDACIÓN TRANSPORTISTA (SOLO OUTBOUND)
    # ===============================

    if tipo_operacion == "OUTBOUND":

        if not transportista_origen:
            raise ValueError("Transportista vacío en XML")

        transportista_mapeado = TRANSPORTISTAS.get(transportista_origen)

        if not transportista_mapeado:
            raise ValueError(f"Transportista no configurado: {transportista_origen}")

    else:
        transportista_mapeado = transportista_origen

    # ===============================
    # DATOS GENERALES
    # ===============================

    fecha_formateada = formatear_fecha(
        obtener_valor(root, "FechaHoraSalida")
    )

    datos_generales = {
        "NumeroTransporte": numero_transporte,
        "Idorigen": obtener_valor(root, "Idorigen"),
        "RFCRemitente": obtener_valor(root, "RFCRemitente"),
        "NombreRemitente": obtener_valor(root, "NombreRemitente"),
        "NumRegldTrib": obtener_valor(root, "NumRegldTrib"),
        "ResidenciaFiscal": obtener_valor(root, "ResidenciaFiscal"),
        "NumEstacion": obtener_valor(root, "NumEstacion"),
        "NombreEstacion": obtener_valor(root, "NombreEstacion"),
        "NavegacionTrafico": obtener_valor(root, "NavegacionTrafico"),
        "FechaHoraSalida": fecha_formateada,
        "IDDestino": obtener_valor(root, "IDDestino"),
        "RFCDestinatario": obtener_valor(root, "RFCDestinatario"),
        "NombreDestinatario": obtener_valor(root, "NombreDestinatario"),
        "Calle": obtener_valor(root, "Calle"),
        "NumeroExterior": obtener_valor(root, "NumeroExterior"),
        "NumeroInterior": obtener_valor(root, "NumeroInterior"),
        "Colonia": obtener_valor(root, "Colonia"),
        "Localidad": obtener_valor(root, "Localidad"),
        "Referencia": obtener_valor(root, "Referencia"),
        "Municipio": obtener_valor(root, "Municipio"),
        "Estado": obtener_valor(root, "Estado"),
        "Pais": obtener_valor(root, "Pais"),
        "CodigoPostal": obtener_valor(root, "CodigoPostal"),
    }

    datos_finales = {
        "CvesTransporte": obtener_valor(root, "CvesTransporte"),
        "Cantidad": obtener_valor(root, "Cantidad"),
        "ValorMercancia": obtener_valor(root, "ValorMercancia"),
        "Moneda": obtener_valor(root, "Moneda"),
        "Calle_Origen": obtener_valor(root, "Calle_Origen"),
        "NumeroExterior_Origen": obtener_valor(root, "NumeroExterior_Origen"),
        "NumeroInterior_Origen": obtener_valor(root, "NumeroInterior_Origen"),
        "Colonia_Origen": obtener_valor(root, "Colonia_Origen"),
        "Localidad_Origen": obtener_valor(root, "Localidad_Origen"),
        "Referencia_Origen": obtener_valor(root, "Referencia_Origen"),
        "Municipio_Origen": obtener_valor(root, "Municipio_Origen"),
        "Estado_Origen": obtener_valor(root, "Estado_Origen"),
        "Pais_Origen": obtener_valor(root, "Pais_Origen"),
        "CodigoPostal_Origen": obtener_valor(root, "CodigoPostal_Origen"),
    }

    # ===============================
    # EXTRAER BIENES
    # ===============================

    bienes_list = []

    for padre in root.iter():
        if padre.tag.endswith("BienesTransporte"):
            for hijo in padre:
                if hijo.tag.endswith("BienesTransporte"):

                    bien = {}
                    for campo in hijo:
                        tag = campo.tag.split("}")[-1]
                        bien[tag] = (campo.text or "").strip()

                    bienes_list.append(bien)
            break

    # ===============================
    # GENERAR XML (INBOUND Y OUTBOUND)
    # ===============================

    detalle_root = ET.Element("DetalleTransporte")

    for key, value in datos_generales.items():
        ET.SubElement(detalle_root, key).text = value

    bienes_padre = ET.SubElement(detalle_root, "BienesTransporte")

    for bien in bienes_list:
        bien_elem = ET.SubElement(bienes_padre, "BienesTransporte")
        for key, value in bien.items():
            ET.SubElement(bien_elem, key).text = value

    for key, value in datos_finales.items():
        ET.SubElement(detalle_root, key).text = value

    xml_path = os.path.join(PROCESSED_PATH, f"{base_filename}.xml")

    ET.ElementTree(detalle_root).write(
        xml_path,
        encoding="utf-8",
        xml_declaration=True
    )

    print(f"XML generado ✅ {xml_path}")

    # ===============================
    # EXCEL SOLO INBOUND
    # ===============================

    if tipo_operacion == "INBOUND":

        filas_excel = []

        for bien in bienes_list:
            fila = {**datos_generales, **bien, **datos_finales}
            filas_excel.append(fila)

        wb = Workbook()
        ws = wb.active
        ws.title = "Transporte"

        headers = list(filas_excel[0].keys())
        ws.append(headers)

        for fila in filas_excel:
            ws.append([fila.get(h, "") for h in headers])

        excel_path = os.path.join(PROCESSED_PATH, f"{base_filename}.xlsx")
        wb.save(excel_path)

        print(f"Excel generado ✅ {excel_path}")

    print("==== FIN PROCESO ✅ ====")

# ===============================
# DAG
# ===============================

with DAG(
    dag_id="cartaporte_122_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    task_procesar = PythonOperator(
        task_id="procesar_cartaporte",
        python_callable=procesar_cartaporte
    )

    start >> task_procesar