import csv
import json
import xmltodict


def ensure_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]

def transform_to_xml(output_dict: dict, transformer_config: dict):
    xml_bytes = xmltodict.unparse(output_dict, pretty=True)
    return xml_bytes

def transform_to_json(output_dict: dict, transformer_config: dict):
    pretty_json = json.dumps(output_dict,indent=4,ensure_ascii=False).encode("utf-8")
    return pretty_json

def transform_to_mnt(output_dict: dict, transformer_config: dict):
    field_order = list(output_dict[0].keys())

    resultado = []
    resultado.append('<Header target_org_node="PAIS:MX"/>')

    for registro in output_dict:
        fila = []

        for campo in field_order:
            valor = registro.get(campo, "")

            if isinstance(valor, bool):
                if valor == True:
                    valor = "1" 
                else:
                    valor = "0"
            elif isinstance(valor, str):
                valor.strip().lower()
                if valor == "si":
                    valor = "1" 
                else:
                    valor = "0"    
            else:
                valor = ""


            fila.append(valor)

        resultado.append("|".join(fila))

    return "\n".join(resultado)

def transform_to_mnt(output_data: dict, transformer_config: dict):
    field_order = list(output_data[0].keys())
 
    result = []
    result.append('<Header target_org_node="PAIS-MX"/>')
 
    for record in output_data:
        row = []
 
        for field in field_order:
            value = record.get(field, "")
 
            if isinstance(value, bool):
                value = "1" if value else "0"
 
            elif isinstance(value, str):
                value_lower = value.strip().lower()
 
                if value_lower in ["yes", "true", "si", "sí"]:
                    value = "1"
                elif value_lower in ["no", "false"]:
                    value = "0"
            
            else:
                value = ""
 
            row.append(value)
 
        result.append("|".join(row))
 
    return "\n".join(result)

def transform_output_to_target_format(output_dict: dict, target_format: str, parser_config: dict | None):
    target_format = target_format.lower()

    if target_format == "xml":
        return transform_to_xml(output_dict, parser_config)

    if target_format == "json":
        return transform_to_json(output_dict, parser_config)

    if target_format == "mnt":
        return transform_to_mnt(output_dict, parser_config)

    raise ValueError(f"Formato no soportado: {target_format}")