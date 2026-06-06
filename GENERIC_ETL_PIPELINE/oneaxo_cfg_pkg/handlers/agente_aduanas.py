from typing import List, Dict, Any
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.transformers import transform_output_to_target_format

class AgenteAduanas:
    """
    Construye el payload final para el endpoint int_agente_aduanas de SAP
    a partir de una lista de registros canónicos.
    """
  
    def _build_item(self, canonical_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        
        body_records = []
        for record in canonical_records:
            body_record = {
                "SOCIEDAD": record.get("SOCIEDAD"),
                "ORG_COMPRA": record.get("ORG_COMPRA"),
                "ZPEDIMIENTO": record.get("ZPEDIMIENTO"),
                "ZCLAVE_PEDIMIENTO": record.get("ZCLAVE_PEDIMIENTO"),
                "ZFECHA_PEDIMIENTO": record.get("ZFECHA_PEDIMIENTO"),
                "ZAGENTE_ADUANA": record.get("ZAGENTE_ADUANA"),
                "ZPEDIMENTO_RECTIFICADO": record.get("ZPEDIMENTO_RECTIFICADO"),
                "ORIG_BTD_ID": record.get("ORIG_BTD_ID"),
                "BASE_BTD_ID": record.get("BASE_BTD_ID"),
                "ZFECHA_DE_ARRIBO": record.get("Z_FECHA_DE_ARRIBO"),
            }
            body_records.append(body_record)
            
        return {
            "item": body_records
        }
    
    def _build_header(self, canonical_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not canonical_records:
            return {
                "header": {}
            }
        
        first_record = canonical_records[0]

        header_record = {
            "TOR_ID": first_record.get("TOR_ID")
        }

        return {
            "header": header_record
        }

    def _soap_envelope(self, payload) -> Dict[str, Any]:

        envelope = {
            "soapenv:Envelope":{
                "@xmlns:soapenv" : "http://schemas.xmlsoap.org/soap/envelope/",
                "@xmlns:urn" : "urn:sap-com:document:sap:rfc:functions",
                "soapenv:Header": None,
                "soapenv:Body": { "urn:ZFTM_INT_AGENTE_ADUANAS": payload }
            }
        }

        return envelope

    
    def build(self, canonical_records: List[Dict[str, Any]], integration_config: Dict[str, Any]) -> Dict[str, Any]:
        
        target_format = integration_config["target_format"]
        header = self._build_header(canonical_records)
        items = self._build_item(canonical_records)

        payload = {
            "PI_INPUT":
            {
                "HEADER": header["header"],
                "ITEM": [items]
            }
        }
        
        return transform_output_to_target_format( self._soap_envelope(payload) , target_format, {})