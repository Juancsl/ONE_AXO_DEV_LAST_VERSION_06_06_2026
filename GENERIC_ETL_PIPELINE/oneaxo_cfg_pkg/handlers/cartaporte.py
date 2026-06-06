import logging
from io import BytesIO
from typing import List, Dict, Any
from datetime import datetime
from openpyxl import Workbook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.transformers import transform_output_to_target_format

AWS_CONN_ID = "one_axo_s3"
OUT_BUCKET_NAME = "one-axo-out"

def _s3_put_bytes(s3_hook: S3Hook, bucket_name: str, key: str, payload: bytes, content_type: str | None = None):
    extra = {"ContentType": content_type} if content_type else {}
    s3_hook.get_conn().put_object(Bucket=bucket_name, Key=key, Body=payload, **extra)

def formatear_fecha(fecha_iso):
    if not fecha_iso:
        return ""
    dt = datetime.strptime(fecha_iso, "%Y-%m-%dT%H:%M:%S")
    return dt.strftime("%d.%m.%Y %H:%M:%S")

class CartaPortePayloadBuilder:
    """
    Construye el payload final para el endpoint cartaporte
    a partir de una lista de registros canónicos.
    """

    def build(self, canonical_records: List[Dict[str, Any]], integration_config: Dict[str, Any]) -> Dict[str, Any]:
        
        target_format = integration_config["target_format"]
        Clase_FO = canonical_records[0].get("Clase_FO")

        if not Clase_FO:
            raise ValueError("Clase_FO vacío en XML")

        if "ZFIN" in Clase_FO:
            tipo_operacion = "INBOUND"
        elif Clase_FO in ["ZFON", "ZFOD"]:
            tipo_operacion = "OUTBOUND"
        else:
            raise ValueError(f"Clase_FO no soportada: {Clase_FO}")

        org = canonical_records[0].get("Org_Compra_Venta")
        numero_transporte = canonical_records[0].get("NumeroTransporte")
        transportista = canonical_records[0].get("Transportista")
        transportista_outbound = canonical_records[0].get("NumeroTransporte_GD")

        if tipo_operacion== "OUTBOUND" and not transportista_outbound:
            raise ValueError(f"Transportista {transportista} no encontrado en catalogo transportistas")

        datos_generales = {
            "NumeroTransporte": numero_transporte,
            "Idorigen": canonical_records[0].get("Idorigen"),
            "RFCRemitente": canonical_records[0].get("RFCRemitente"),
            "NombreRemitente": canonical_records[0].get("NombreRemitente"),
            "NumRegldTrib": canonical_records[0].get("NumRegldTrib"),
            "ResidenciaFiscal": canonical_records[0].get("ResidenciaFiscal"),
            "NumEstacion": canonical_records[0].get("NumEstacion"),
            "NombreEstacion": canonical_records[0].get("NombreEstacion"),
            "NavegacionTrafico": canonical_records[0].get("NavegacionTrafico"),
            "FechaHoraSalida": canonical_records[0].get("FechaHoraSalida"),
            "IDDestino": canonical_records[0].get("IDDestino"),
            "RFCDestinatario": canonical_records[0].get("RFCDestinatario"),
            "NombreDestinatario": canonical_records[0].get("NombreDestinatario"),
            "Calle": canonical_records[0].get("Calle"),
            "NumeroExterior": canonical_records[0].get("NumeroExterior"),
            "NumeroInterior": canonical_records[0].get("NumeroInterior"),
            "Colonia": canonical_records[0].get("Colonia"),
            "Localidad": canonical_records[0].get("Localidad"),
            "Referencia": canonical_records[0].get("Referencia"),
            "Municipio": canonical_records[0].get("Municipio"),
            "Estado": canonical_records[0].get("Estado"),
            "Pais": canonical_records[0].get("Pais"),
            "CodigoPostal": canonical_records[0].get("CodigoPostal"),
        }
    
        datos_finales = {
            "CvesTransporte": canonical_records[0].get("CvesTransporte"),
            "Cantidad": canonical_records[0].get("Cantidad"),
            "ValorMercancia": canonical_records[0].get("ValorMercancia"),
            "Moneda": canonical_records[0].get("Moneda"),
            "Calle_Origen": canonical_records[0].get("Calle_Origen"),
            "NumeroExterior_Origen": canonical_records[0].get("NumeroExterior_Origen"),
            "NumeroInterior_Origen": canonical_records[0].get("NumeroInterior_Origen"),
            "Colonia_Origen": canonical_records[0].get("Colonia_Origen"),
            "Localidad_Origen": canonical_records[0].get("Localidad_Origen"),
            "Referencia_Origen": canonical_records[0].get("Referencia_Origen"),
            "Municipio_Origen": canonical_records[0].get("Municipio_Origen"),
            "Estado_Origen": canonical_records[0].get("Estado_Origen"),
            "Pais_Origen": canonical_records[0].get("Pais_Origen"),
            "CodigoPostal_Origen": canonical_records[0].get("CodigoPostal_Origen"),
        }

        bienes_list = []

        for x in canonical_records:
            bienes_list.append({
                "NumPedimento": x.get("NumPedimento"),
                "NumTotalMercancias": x.get("NumTotalMercancias"),
                "BienesTransp": x.get("BienesTransp"),
                "ClaveUnidad": x.get("ClaveUnidad"),
                "Unidad": x.get("Unidad"),
                "PesoEnKg": x.get("PesoEnKg"),
            })
        
        payload = {
            "DetalleTransporte":
            {
              **datos_generales,
              "BienesTransporte":{"BienesTransporte":bienes_list},
              **datos_finales
            }
        }

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        base_filename = f"{org}_{numero_transporte}_{timestamp}"
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        integration_id = integration_config.get("integration_id")
        
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

            buffer = BytesIO()
            wb.save(buffer)
            xlsx_bytes = buffer.getvalue()

            xlsx_out_s3_key = f"{integration_id}/out/xlsx/{base_filename}.xlsx"

            _s3_put_bytes(
                s3_hook=s3_hook,
                bucket_name=OUT_BUCKET_NAME,
                key=xlsx_out_s3_key,
                payload=xlsx_bytes,
                content_type="application/json",
            )
        else : 
            xlsx_out_s3_key= ""
        
        payload = transform_output_to_target_format( payload , target_format, {})
        xml_out_s3_key = f"{integration_id}/out/xml/{base_filename}.xml"
        
        _s3_put_bytes(
            s3_hook=s3_hook,
            bucket_name=OUT_BUCKET_NAME,
            key=xml_out_s3_key,
            payload=payload,
            content_type="application/json",
        )

        control_payload = {
            "numero_transporte": numero_transporte,
            "transportista": transportista,
            "tipo_operacion": tipo_operacion,
            "xml_out_s3_key": xml_out_s3_key, 
            "xlsx_out_s3_key": xlsx_out_s3_key
            }
        
        integration_config["target_format"] = "json"

        return transform_output_to_target_format(control_payload,"json", {})