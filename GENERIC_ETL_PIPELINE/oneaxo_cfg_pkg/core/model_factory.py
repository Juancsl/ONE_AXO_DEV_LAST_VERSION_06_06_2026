from typing import Optional

from pydantic import create_model


TYPE_MAP = {
    "decimal":float,
    "string":str,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
}


def _parse_default(field_type: str, default_value):
    if default_value is None:
        return None

    if field_type == "int":
        return int(default_value)

    if field_type == "float":
        return float(default_value)

    if field_type == "bool":
        return str(default_value).lower() in ("true", "1", "yes", "y")

    return str(default_value)


def build_dynamic_model_from_models_config(
    integration_id: str,
    model_key: str,
    models_config: dict,
):
    model_def = models_config.get(model_key)

    if not model_def:
        raise ValueError(
            f"No existe definición de modelo para integration_id={integration_id}, model_key={model_key}"
        )

    field_defs = model_def.get("fields", [])
    if not field_defs:
        raise ValueError(
            f"El modelo {model_key} no tiene campos en integration_id={integration_id}"
        )

    fields = {}

    for field in field_defs:
        field_name = field["name"]
        field_type = field["type"]
        required = field.get("required", True)
        default_value = field.get("default")

        py_type = TYPE_MAP.get(field_type)
        if py_type is None:
            raise ValueError(
                f"Tipo no soportado: {field_type} en integration_id={integration_id}, model_key={model_key}, field_name={field_name}"
            )

        if required:
            fields[field_name] = (py_type, ...)
        else:
            parsed_default = _parse_default(field_type, default_value)
            fields[field_name] = (Optional[py_type], parsed_default)

    model_name = f"{integration_id}_{model_key}_Model"
    return create_model(model_name, **fields)