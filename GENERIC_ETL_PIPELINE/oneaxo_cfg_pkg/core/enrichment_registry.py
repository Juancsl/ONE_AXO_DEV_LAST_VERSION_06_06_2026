import json
from airflow.models import Variable
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.api_auth_client import ApiClient

API_VAR_NAME = "sap_connector_config"


def _ensure_meta(record: dict):
    if "_meta" not in record:
        record["_meta"] = {
            "validation_errors": [],
            "validation_warnings": [],
            "enrichments_applied": [],
            "notification_issues": [],
        }
    return record["_meta"]


def _add_validation_message(record: dict, severity: str, message: str):
    meta = _ensure_meta(record)

    if severity == "warning":
        meta["validation_warnings"].append(message)
    else:
        meta["validation_errors"].append(message)


def _add_notification_issue(
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


def enrichment_noop(record: dict, params: dict, services: dict | None = None) -> dict:
    return record


def sap_purchaseorderotemapi01(record: dict, params: dict, services: dict | None = None) -> dict:
    services = services or {}
    meta = _ensure_meta(record)
    purchase_order = params.get("lookup_field")

    query = """
    query sapQuery($tableName: String, $fields: [String], $filters: [FilterInput]) {
        sapQuery(tableName: $tableName, fields: $fields, filters: $filters) {
            name,
            value
        }
    }
    """

    variables = {
        "tableName": "I_PURCHASEORDERITEMAPI01",
        "fields": [
            "PURCHASEORDER",
            "PURCHASEORDERITEM",
            "MATERIAL",
            "PLANT",
            "ORDERQUANTITY",
            "PURCHASINGDOCUMENTDELETIONCODE",
            "ISCOMPLETELYDELIVERED",
        ],
        "filters": [
            {
                "key": "PURCHASEORDER",
                "value": f"{purchase_order}",
            }
        ],
    }

    json_payload = {
        "query": query,
        "variables": variables,
    }

    client = services.get("sap_api_client")

    if not client:
        raise ValueError("El cliente SAP API no fue inicializado en el diccionario de servicios.")

    response = client.post(endpoint="/graphql", payload=json_payload)
    response = response.json()
    data = response["data"]["sapQuery"]

    data_cleaned = []

    for x in data:
        elem = {}
        for y in x:
            elem[y["name"]] = y["value"]
        data_cleaned.append(elem)

    for x in data_cleaned:
        if x["MATERIAL"] == record["MATNR"] and x["PURCHASEORDERITEM"] == str(record["VGPOS"]):
            record["WERKS"] = x["PLANT"]
            record["TMENG2"] = float(x["ORDERQUANTITY"])
            record["TMENG1"] = float(x["ORDERQUANTITY"])

    meta["enrichments_applied"].append("sap_delivery_enrichment")

    return record


def global_dict_query(record: dict, params: dict, services: dict | None = None) -> dict:
    services = services or {}
    meta = _ensure_meta(record)

    module = params["module"]
    catalog = params["catalog"]
    lookup_field = params["lookup_field"]
    target_field = params["target_field"]

    lookup_value = str(record.get(lookup_field, "")).strip()
    lookupkey = f"{module}-{catalog}-{lookup_value}"

    sql = """
        SELECT target_value
        FROM ctrlplane.tbl_cat_gd
        WHERE lookupkey = %s
    """

    hook = services["postgres_conn"]

    if not hook:
        raise ValueError("La conexion a la base de datos no fue inicializada en el diccionario de servicios.")

    result = hook.get_first(sql, parameters=(lookupkey,))

    if result and result[0]:
        record[target_field] = f"{result[0]}"
    else:
        record[target_field] = ""

        if catalog == "transportista":
            _add_validation_message(
                record,
                "error",
                f"Transportista no encontrado en diccionario global: {lookup_value}",
            )

            _add_notification_issue(
                record=record,
                issue_code="carrier_not_found",
                severity="error",
                field_name=lookup_field,
                message=lookup_value,
            )

    meta["enrichments_applied"].append(f"global_dict_query:{lookupkey}")

    return record


ENRICHMENT_REGISTRY = {
    "noop": enrichment_noop,
    "sap_purchaseorderotemapi01": sap_purchaseorderotemapi01,
    "global_dict_query": global_dict_query,
}


def apply_enrichments(record: dict, enrichments: list[dict], services: dict | None = None) -> dict:
    services = services or {}

    for enrichment in enrichments:
        if not enrichment.get("enabled", True):
            continue

        name = enrichment["name"]
        params = enrichment.get("params", {})

        fn = ENRICHMENT_REGISTRY.get(name)

        if not fn:
            raise ValueError(f"Enrichment no soportado: {name}")

        record = fn(record, params, services)

    return record