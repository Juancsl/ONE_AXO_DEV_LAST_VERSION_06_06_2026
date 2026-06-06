import re


def _ensure_meta(record: dict):
    if "_meta" not in record:
        record["_meta"] = {
            "validation_errors": [],
            "validation_warnings": [],
            "enrichments_applied": [],
            "notification_issues": [],
        }
    return record["_meta"]


def add_validation_message(record: dict, severity: str, message: str):
    meta = _ensure_meta(record)

    if severity == "warning":
        meta["validation_warnings"].append(message)
    else:
        meta["validation_errors"].append(message)


def add_notification_issue(
    record: dict,
    issue_code: str,
    severity: str,
    field_name: str,
    message: str,
):
    meta = _ensure_meta(record)
    meta["notification_issues"].append(
        {
            "issue_code": issue_code,
            "severity": severity,
            "field_name": field_name,
            "message": message,
        }
    )


def rule_non_positive_number_notify(record: dict, params: dict, severity: str = "error"):
    field = params.get("field")
    issue_code = params.get("issue_code", "non_positive_number")
    message = params.get("message", f"El campo '{field}' debe ser mayor que 0")

    value = record.get(field)
    if value is None:
        return record

    try:
        if float(value) <= 0:
            add_validation_message(record, severity, message)
            add_notification_issue(
                record=record,
                issue_code=issue_code,
                severity=severity,
                field_name=field,
                message=message,
            )
    except Exception:
        add_validation_message(record, severity, f"El campo '{field}' no es numérico")
        add_notification_issue(
            record=record,
            issue_code=issue_code,
            severity=severity,
            field_name=field,
            message=f"El campo '{field}' no es numérico",
        )

    return record


def rule_required_fields(record: dict, params: dict, severity: str = "error"):
    fields = params.get("fields", [])
    issue_code = params.get("issue_code", "required_fields")

    for field in fields:
        value = record.get(field)
        if value in (None, "", []):
            message = params.get("message", f"El campo '{field}' es requerido")
            add_validation_message(record, severity, message)
            add_notification_issue(
                record=record,
                issue_code=issue_code,
                severity=severity,
                field_name=field,
                message=message,
            )

    return record


def rule_positive_number(record: dict, params: dict, severity: str = "error"):
    field = params.get("field")
    value = record.get(field)

    if value is None:
        return record

    try:
        if float(value) <= 0:
            add_validation_message(record, severity, f"El campo '{field}' debe ser mayor que 0")
    except Exception:
        add_validation_message(record, severity, f"El campo '{field}' no es numérico")

    return record


def rule_allowed_values(record: dict, params: dict, severity: str = "error"):
    field = params.get("field")
    allowed = params.get("allowed_values", [])
    value = record.get(field)

    if value in (None, ""):
        return record

    if value not in allowed:
        add_validation_message(
            record,
            severity,
            f"El campo '{field}' tiene un valor no permitido: {value}",
        )

    return record


def validate_regex_match(record: dict, params: dict, severity: str = "error"):
    field = params.get("field")
    pattern = params.get("pattern")
    issue_code = params.get("issue_code", "regex_match")
    skip_empty = params.get("skip_empty", False)
    message = params.get("message", f"El campo '{field}' no cumple el formato esperado.")

    value = record.get(field)

    if skip_empty and (value is None or str(value).strip() == ""):
        return record

    if value is None:
        return record

    value_str = str(value).strip()

    try:
        if re.fullmatch(pattern, value_str) is None:
            add_validation_message(record, severity, message)

            # Para notificación guardamos el valor real inválido,
            # no el mensaje genérico.
            add_notification_issue(
                record=record,
                issue_code=issue_code,
                severity=severity,
                field_name=field,
                message=value_str,
            )
    except Exception:
        add_validation_message(record, severity, f"El campo '{field}' no cumple el formato esperado")
        add_notification_issue(
            record=record,
            issue_code=issue_code,
            severity=severity,
            field_name=field,
            message=value_str,
        )

    return record


VALIDATION_REGISTRY = {
    "required_fields": rule_required_fields,
    "positive_number": rule_positive_number,
    "allowed_values": rule_allowed_values,
    "non_positive_number": rule_non_positive_number_notify,
    "regex_match_notify": validate_regex_match,
}


def apply_validations(record: dict, validations: list[dict]) -> dict:
    for validation in validations:
        if not validation.get("enabled", True):
            continue

        name = validation["name"]
        params = validation.get("params", {})
        severity = validation.get("severity", "error")

        rule_fn = VALIDATION_REGISTRY.get(name)
        if not rule_fn:
            raise ValueError(f"Validación no soportada: {name}")

        record = rule_fn(record, params, severity)

    return record


def record_has_errors(record: dict) -> bool:
    meta = record.get("_meta", {})
    return len(meta.get("validation_errors", [])) > 0


def record_has_warnings(record: dict) -> bool:
    meta = record.get("_meta", {})
    return len(meta.get("validation_warnings", [])) > 0