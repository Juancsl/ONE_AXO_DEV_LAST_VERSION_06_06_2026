from typing import List, Dict, Any
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.transformers import transform_output_to_target_format

class RfcPayloadBuilder:
    """
    Construye el payload final para la RFC ZMXMMFM_PO_CONFIRMATION_ENT
    a partir de una lista de registros canónicos.
    """

    def _build_header(self, canonical_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Construye el 'headerTable' a partir del primer registro canónico."""
        
        # Si no hay registros, devolvemos una cabecera vacía.
        if not canonical_records:
            return {
                "name": "HEADER_TABLE",
                "fields": ["EBELN", "ZCONF_POS"],
                "records": []
            }
            
        # Asumimos que los datos de la cabecera son comunes y los tomamos del primer registro.
        first_record = canonical_records[0]
        
        header_record = {
            "EBELN": first_record.get("Pedido"),
            "ZCONF_POS": first_record.get("Posicion_de_confirmacion")
        }
        
        return {
            "name": "HEADER_TABLE",
            "fields": list(header_record.keys()),
            "records": [header_record]
        }

    def _build_body(self, canonical_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Construye el 'directTable' iterando sobre todos los registros canónicos."""

        body_records = []
        for record in canonical_records:
            body_record = {
                "EBELN": record.get("Pedido"),
                "EBELP": record.get("Posicion_Picking"),
                "ZZPROVEEDOR": record.get("Proveedor"),
                "MENGE": record.get("Cantidad_de_pedido"),
                "MATNR": record.get("Material"),
                "WERKS": record.get("Centro"),
                "ZLFIMG": record.get("Cantidad_de_entrega_real"),
                "ZCOUNT": record.get("Contador"),
                "ZCONF_POS": record.get("Posicion_de_confirmacion"),
                "EINDT": record.get("Fecha_Estimada_de_Entrega_Destino"),
                "EEINDT": record.get("Fecha_Estimada_de_Salida_Origen"),
                "TXZ01": record.get("Descripcion"),
                "ZRECHAZO": record.get("Rechazo_Total_de_la_Posicion_del_Pedido")
            }
            body_records.append(body_record)
            
        return {
            "name": "BODY_TABLE",
            "fields": list(body_records[0].keys()) if body_records else [],
            "records": body_records
        }

    def _build_direct(self, canonical_records: List[Dict[str, Any]], table_name: str) -> Dict[str, Any]:
        """Construye el 'directTable' iterando sobre todos los registros canónicos."""

        direct_records = []
        for record in canonical_records:
            direct_record = {
                "VGBEL": record.get("VGBEL"),
                "VGPOS": record.get("VGPOS"),
                "LIFNR": record.get("LIFNR"),
                "VBELN": record.get("VBELN"),
                "G_LFIMG": record.get("G_LFIMG"),
                "MATNR": record.get("MATNR"),
                "WERKS": record.get("WERKS"),
                "LFIMG": record.get("LFIMG"),
                "EXIDV": record.get("EXIDV"),
                "VHILM": record.get("VHILM"),
                "TMENG2": record.get("TMENG2"),
                "TMENG": record.get("TMENG"),
                "PLANTA": record.get("PLANTA"),
                "CONT": record.get("CONT"),
                "CONF_SER": record.get("CONF_SER"),
                "DELIV_DATE": record.get("DELIV_DATE"),
                "HANDOVER_DATE": record.get("HANDOVER_DATE"),
                "REFERENCE": record.get("REFERENCE"),
                "CANCELED": record.get("CANCELED"),
                "DELETE_IND": record.get("DELETE_IND"),
                "MAT_CAJ": record.get("MAT_CAJ"),
                "MAT_CAJ2": record.get("MAT_CAJ2"),
                "CONFIRMACION": record.get("CONFIRMACION"),
                "PEDIDO_PENDIENTE": record.get("PEDIDO_PENDIENTE"),
                "PEDIDO_COMPLETADO": record.get("PEDIDO_COMPLETADO"),
                "ERROR": record.get("ERROR"),
                "STATUS": record.get("STATUS")
            }
            direct_records.append(direct_record)
            
        return {
            "name": table_name,
            "fields": list(direct_records[0].keys()) if direct_records else [],
            "records": direct_records
        }

    def build(self, canonical_records: List[Dict[str, Any]], integration_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Punto de entrada principal. Orquesta la construcción del payload completo.
        
        Args:
            canonical_records: La lista de registros del mensaje canónico.
            integration_config: La configuración completa de la integración.
        
        Returns:
            El diccionario completo del payload de salida.
        """
        table_name = "I_PO_ENT"
        rfc_name = "ZMXMMFM_PO_CONFIRMATION_ENT"
        direct_table = self._build_direct(canonical_records, table_name)
        target_format = integration_config["target_format"]

        final_payload = {
            "table": table_name,
            "rfcName": rfc_name,
            "direct": True,
            "expectedResult": False,
            "resultTableName": "",
            "headerTable": {},
            "bodyTable": {},
            "directTable": direct_table
        }

        return transform_output_to_target_format(final_payload, target_format, {})
