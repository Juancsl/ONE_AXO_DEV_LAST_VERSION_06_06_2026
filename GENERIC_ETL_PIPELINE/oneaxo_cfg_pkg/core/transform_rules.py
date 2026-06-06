# -*- coding: utf-8 -*-
import re
import logging
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateutil_parser
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.parsers import get_path_value

def _evaluate_condition(source_context: dict, condition: dict) -> bool:
    """Función auxiliar para evaluar una única condición."""
    path = condition.get("source_path")
    operator = condition.get("operator", "equals")
    expected_value = condition.get("value")

    actual_value = get_path_value(source_context, path)

    if operator == "equals":
        return actual_value == expected_value
    if operator == "not_equals":
        return actual_value != expected_value
    if operator == "in":
        return actual_value in (expected_value or [])
    if operator == "is_not_null":
        return actual_value is not None and actual_value != ""
    # Se pueden añadir más operadores como 'greater_than', 'contains', etc.
    
    raise ValueError(f"Operador de condición no soportado: {operator}")

def _resolve_value(source_context: dict, value_or_path: any):
    """
    Resuelve un valor. Si es un diccionario con 'from_path', extrae el valor
    del contexto. Si no, devuelve el valor tal cual.
    """
    if isinstance(value_or_path, dict) and "from_path" in value_or_path:
        path = value_or_path["from_path"]
        return get_path_value(source_context, path)
    
    # Si no es una referencia, es un valor estático
    return value_or_path

def rule_identity(value, default_value, **kwargs):
    """Devuelve el valor si no es None, de lo contrario devuelve el valor por defecto."""
    return value if value is not None else default_value

def rule_default_only(default_value, **kwargs):
    """Siempre devuelve el valor por defecto, ignorando el valor de entrada."""
    return default_value

def rule_to_str(value, default_value, **kwargs):
    """Convierte el valor a string. Usa el valor por defecto si el valor de entrada es nulo o vacío."""
    if value in (None, ""):
        return str(default_value or "")
    return str(value)

def rule_to_int(value, default_value, **kwargs):
    """Convierte el valor a int. Usa el valor por defecto si el valor de entrada es nulo o vacío."""
    if value in (None, ""):
        return int(default_value or 0)
    return int(value)

def rule_to_float(value, default_value, **kwargs):
    """Convierte el valor a float. Usa el valor por defecto si el valor de entrada es nulo o vacío."""
    if value in (None, ""):
        return float(default_value or 0)
    return float(value)

def rule_regex_extract(value, default_value, params, **kwargs):
    """Extrae un valor usando una expresión regular y opcionalmente lo convierte a un tipo."""
    pattern = params.get("pattern")
    if not pattern:
        raise ValueError("regex_extract requiere transform_params.pattern")

    cast_to = params.get("cast_to")
    # Función interna para obtener el valor por defecto con el tipo correcto
    def _get_default():
        if cast_to == "int":
            return int(default_value or 0)
        if cast_to == "float":
            return float(default_value or 0)
        return default_value

    if value in (None, ""):
        return _get_default()

    group = int(params.get("group", 1))
    match = re.search(pattern, str(value))
    
    if not match:
        return _get_default()
    
    extracted = match.group(group)
    
    if cast_to == "int":
        return int(extracted)
    if cast_to == "float":
        return float(extracted)
    return extracted

def rule_concat(value, default_value, params, **kwargs):
    """Concatena una lista de valores con un delimitador."""
    delimiter = params.get("delimiter", "")
    value_param = params.get("fields")
    values = value_param if isinstance(value_param, list) else [value_param]
    values_to_join = [str(v) for v in values if v not in (None, "")]
    
    if not values_to_join:
        return default_value
    
    return delimiter.join(values_to_join)

def rule_coalesce(value, default_value, **kwargs):
    """Devuelve el primer valor no nulo ni vacío de una lista."""
    values = value if isinstance(value, list) else [value]
    for v in values:
        if v not in (None, ""):
            return v
    return default_value

def rule_format_date(value, default_value, params, **kwargs):
    """
    Parsea un string de fecha y lo formatea, con la opción de convertirlo a UTC.
    """
    if value in (None, ""):
        return default_value

    output_format = params.get("output_format", "%Y-%m-%dT%H:%M:%S")
    # Nuevo parámetro para controlar la conversión a UTC
    convert_to_utc = params.get("to_utc", False)

    try:
        # 1. Parsear el string de fecha. dateutil creará un objeto datetime "aware"
        #    si el string contiene información de zona horaria.
        datetime_obj = dateutil_parser.parse(str(value))

        # 2. Convertir a UTC si se solicita y si la fecha es "aware"
        if convert_to_utc and datetime_obj.tzinfo is not None:
            datetime_obj = datetime_obj.astimezone(timezone.utc)
        elif convert_to_utc and datetime_obj.tzinfo is None:
            # Buena práctica: advertir si se intenta convertir una fecha "naive" (sin zona horaria)
            logging.warning(
                f"Se intentó convertir a UTC la fecha '{value}', pero no tiene información de zona horaria. "
                "La conversión no se realizará para evitar asumir una zona horaria incorrecta."
            )
            
        # 3. Formatear la fecha (ya sea la original o la convertida a UTC)
        return datetime_obj.strftime(output_format)
        
    except (ValueError, TypeError) as e:
        logging.warning(f"No se pudo parsear la fecha: '{value}'. Error: {e}. Se usará el valor por defecto.")
        return default_value

def format_customs_request_number(value, default_value, params, **kwargs):
    try:
        customs_request_number = "  ".join(value.split())
        return customs_request_number
    except (ValueError, TypeError) as e:
        logging.warning(f"No se pudo normalizar el campo ZPEDIMENTO. Se retornara el mismo valor.")
        return value

def rule_case_when(source_context: dict, params: dict, default_value, **kwargs):
    """
    Simula una declaración CASE de SQL, permitiendo que el valor de 'then'
    sea estático o una referencia a otro campo del contexto.
    """
    conditions = params.get("conditions", [])
    else_value = params.get("else", default_value)

    for case in conditions:
        when_block = case.get("when", [])
        
        # Todas las condiciones del bloque 'when' deben cumplirse (lógica AND)
        if all(_evaluate_condition(source_context, cond) for cond in when_block):
            then_value_or_path = case.get("then")
            # Devolvemos el valor resuelto (ya sea estático o extraído de una ruta)
            return _resolve_value(source_context, then_value_or_path)
            
    # Si ningún 'when' se cumplió, resolvemos y devolvemos el valor 'else'.
    return _resolve_value(source_context, else_value)

def rule_substring(value, default_value, params, **kwargs):
    """
    Extrae una subcadena de un valor de entrada.
    Utiliza los parámetros 'start' (índice inicial, 0-based) y 'length' (opcional).
    """
    # Si el valor de entrada es nulo o vacío, devolvemos el valor por defecto.
    if value in (None, ""):
        return default_value

    try:
        start = int(params.get("start"))
        length = params.get("length")

        text = str(value)

        if length is not None:
            length = int(length)
            return text[start : start + length]
        else:
            # Si no se especifica 'length', se extrae desde 'start' hasta el final.
            return text[start:]
            
    except (ValueError, TypeError, KeyError):
        # Si 'start' no es un número o falta, o si hay otro error, devolvemos el default.
        return default_value

def rule_current_datetime(params: dict, **kwargs):
    """
    Genera la fecha y hora actual. Permite especificar una zona horaria
    como un offset de UTC y un formato de salida.
    """
    # 1. Obtenemos los parámetros de la configuración
    output_format = params.get("output_format", "%Y-%m-%dT%H:%M:%S")
    offset_hours = params.get("timezone_offset") # ej. -6

    # 2. Obtenemos la hora actual en UTC. Este es el punto de referencia más fiable.
    now_utc = datetime.now(timezone.utc)
    
    target_datetime = now_utc

    # 3. Si se especifica un offset, convertimos la hora
    if offset_hours is not None:
        try:
            # Creamos un objeto de zona horaria con el offset proporcionado
            target_tz = timezone(timedelta(hours=int(offset_hours)))
            # Convertimos la hora UTC a esa nueva zona horaria
            target_datetime = now_utc.astimezone(target_tz)
        except (ValueError, TypeError):
            logging.warning(
                f"El valor de 'timezone_offset' ({offset_hours}) no es un número válido. Se usará UTC por defecto."
            )
            # Si el offset no es válido, por seguridad, nos quedamos con UTC

    # 4. Formateamos la fecha (ya sea la UTC o la convertida)
    return target_datetime.strftime(output_format)


def rule_sum_fields(params: dict, source_context: dict, default_value, **kwargs):
    """
    Suma los valores de una lista de campos especificada. Ignora el valor de entrada
    y trabaja sobre el source_context.
    - Intenta convertir los valores a float para la suma.
    - Si un valor no se puede convertir a número, se trata como 0 y se loguea una advertencia.
    """
    # Obtenemos la lista de rutas de los campos a sumar desde los parámetros.
    fields_to_sum = params.get("fields_to_sum", [])
    if not fields_to_sum:
        logging.warning("La regla 'sum_fields' fue llamada sin 'fields_to_sum' en los parámetros. Devolviendo default.")
        return default_value

    total_sum = 0.0

    for path in fields_to_sum:
        # Extraemos el valor de cada campo del contexto completo
        raw_value = get_path_value(source_context, path)

        # Si el valor es nulo o vacío, lo tratamos como 0
        if raw_value in (None, ""):
            continue

        # Intentamos convertir el valor a float para la suma
        try:
            total_sum += float(raw_value)
        except (ValueError, TypeError):
            # Si la conversión falla, logueamos una advertencia y continuamos (tratando el valor como 0)
            logging.warning(
                f"En la regla 'sum_fields', no se pudo convertir el valor '{raw_value}' del campo en la ruta '{path}' a un número. Se ignorará en la suma."
            )
            continue
    
    return str(int(total_sum))

# --- Registro Central de Reglas de Transformación ---

TRANSFORM_RULE_REGISTRY = {
    "identity": rule_identity,
    "default_only": rule_default_only,
    "to_str": rule_to_str,
    "to_int": rule_to_int,
    "to_float": rule_to_float,
    "regex_extract": rule_regex_extract,
    "concat": rule_concat,
    "coalesce": rule_coalesce,
    "format_date": rule_format_date,
    "format_customs": format_customs_request_number,
    "case_when": rule_case_when,
    "substring": rule_substring,
    "current_datetime": rule_current_datetime,
    "sum_fields": rule_sum_fields,
}

# --- Función Despachadora (Dispatcher) ---

def apply_transform_rule(value, rule: str, default_value=None, params=None, source_context=None):
    """
    Busca una regla en el registro y la ejecuta con los parámetros proporcionados.
    """
    rule = rule or "identity"
    params = params or {}
    
    # Busca la función de la regla en el registro
    rule_func = TRANSFORM_RULE_REGISTRY.get(rule or "identity")

    if not rule_func:
        raise ValueError(f"Regla de transformación no soportada: {rule}")

    # Llama a la función encontrada, pasando los argumentos.
    # El **kwargs en la firma de las funciones de regla ignorará los parámetros que no necesite.
    return rule_func(value=value, default_value=default_value, params=params, source_context=source_context)
