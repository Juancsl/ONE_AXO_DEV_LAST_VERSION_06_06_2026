from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json

POSTGRES_CONN_ID = "gobierno_central_postgres"
INTEGRATION_ID = "9999D"
DEFAULT_OUTPUT_DIR = "/opt/airflow/data/output/9999D"
DEFAULT_NOM2001_CSV_PATH = "/opt/airflow/data/input/nom_2001_completo.csv"

CONTROL_TABLE = "finz.cntrl_nom2001_to_sap_empleados"
SOCIEDADES_TABLE = "finz.ctl_compania_nom200_to_sap"
LOG_TABLE = "ctrlplane.tbl_airflow_integration_log_finz"
ENDPOINTS_TABLE = "ctrlplane.tbl_cfg_endpoints_airflow"


def clean_text(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    if value == "" or value.lower() in ("nan", "none", "null"):
        return None
    return value.upper()


def clean_key(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    if value == "" or value.lower() in ("nan", "none", "null"):
        return None
    return value.zfill(7)


def clean_cvecia(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    if value == "" or value.lower() in ("nan", "none", "null"):
        return None
    return value.zfill(3)


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    return str(value)


def safe_json_load(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, payload: Any) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            payload,
            f,
            ensure_ascii=False,
            indent=2,
            default=json_default,
        )
    return path


def insert_integration_log(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    phase: str,
    status: str,
    message: str,
    source_file: Optional[str] = None,
    raw_s3_key: Optional[str] = None,
    canonical_s3_key: Optional[str] = None,
    out_s3_key: Optional[str] = None,
    error_message: Optional[str] = None,
    send_details: Optional[Dict[str, Any]] = None,
    postgres_conn_id: str = POSTGRES_CONN_ID,
) -> None:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    sql = f"""
        INSERT INTO {LOG_TABLE}
        (
            dag_id,
            run_id,
            task_id,
            integration_id,
            phase,
            status,
            source_file,
            raw_s3_key,
            canonical_s3_key,
            out_s3_key,
            message,
            error_message,
            created_at,
            send_details
        )
        VALUES
        (
            %(dag_id)s,
            %(run_id)s,
            %(task_id)s,
            %(integration_id)s,
            %(phase)s,
            %(status)s,
            %(source_file)s,
            %(raw_s3_key)s,
            %(canonical_s3_key)s,
            %(out_s3_key)s,
            %(message)s,
            %(error_message)s,
            NOW(),
            %(send_details)s
        )
    """
    hook.run(
        sql,
        parameters={
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "integration_id": INTEGRATION_ID,
            "phase": phase,
            "status": status,
            "source_file": source_file,
            "raw_s3_key": raw_s3_key,
            "canonical_s3_key": canonical_s3_key,
            "out_s3_key": out_s3_key,
            "message": message,
            "error_message": error_message,
            "send_details": Json(send_details or {}),
        },
    )


def load_nom2001_csv_to_json(
    csv_path: str = DEFAULT_NOM2001_CSV_PATH,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    only_active: bool = True,
) -> str:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"No existe archivo NOM2001: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]

    df = df.rename(
        columns={
            "fecalt": "fecha_alt",
            "nombretra": "nombre_tra",
            "apepat": "apellido_pat",
            "apemat": "apellido_mat",
            "fecnac": "fecha_nac",
            "desciu": "desc_ciudad",
            "numrfc": "rfc",
            "status": "estatus",
        }
    )

    required = [
        "cvetra",
        "fecha_alt",
        "nombre_tra",
        "apellido_pat",
        "apellido_mat",
        "fecha_nac",
        "sexo",
        "cvecia",
        "desc_ciudad",
        "rfc",
        "estatus",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas en NOM2001 CSV: {missing}")

    df["estatus"] = df["estatus"].astype(str).str.strip().str.upper()
    if only_active:
        df = df[df["estatus"] == "A"].copy()

    df["cvetra"] = df["cvetra"].apply(clean_key)
    df["cvecia"] = df["cvecia"].apply(clean_cvecia)

    df = df.astype(object).where(pd.notnull(df), None)

    path = os.path.join(output_dir, "nom2001_records.json")
    write_json(path, df.to_dict("records"))

    print(f"NOM2001 registros cargados: {len(df)}")
    return path


def load_control_table_to_json(
    output_dir: str = DEFAULT_OUTPUT_DIR,
    postgres_conn_id: str = POSTGRES_CONN_ID,
) -> str:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    sql = f"""
        SELECT
            cvetra,
            apepat,
            apemat,
            nombretra,
            cveare,
            fecnac,
            cvedep,
            cvecia,
            desciu,
            subcta,
            numrfc,
            clabe,
            contrlstatus,
            sapstatus,
            no_de_acreedor,
            fecha_baja,
            code_response,
            rfc
        FROM {CONTROL_TABLE}
    """

    df = hook.get_pandas_df(sql)
    df.columns = [c.strip().lower() for c in df.columns]

    df["cvetra"] = df["cvetra"].apply(clean_key)
    df["cvecia"] = df["cvecia"].apply(clean_cvecia)

    df = df.astype(object).where(pd.notnull(df), None)

    path = os.path.join(output_dir, "control_records.json")
    write_json(path, df.to_dict("records"))

    print(f"Control FINZ registros cargados: {len(df)}")
    return path


def load_sociedades_to_json(
    output_dir: str = DEFAULT_OUTPUT_DIR,
    postgres_conn_id: str = POSTGRES_CONN_ID,
) -> str:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    sql = f"""
        SELECT
            id,
            cvecia_nom200,
            cvecia_sap,
            descripcion,
            activo
        FROM {SOCIEDADES_TABLE}
        WHERE activo = 1
    """

    df = hook.get_pandas_df(sql)
    df.columns = [c.strip().lower() for c in df.columns]
    df["cvecia_nom200"] = df["cvecia_nom200"].apply(clean_cvecia)

    df = df.astype(object).where(pd.notnull(df), None)

    path = os.path.join(output_dir, "sociedades_records.json")
    write_json(path, df.to_dict("records"))

    print(f"Sociedades activas cargadas: {len(df)}")
    return path


def load_s4h_endpoint_config(
    endpoint_name: str = "NOM2001_9999D_S4H_TARGET",
    postgres_conn_id: str = POSTGRES_CONN_ID,
) -> Dict[str, Any]:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    sql = f"""
        SELECT config
        FROM {ENDPOINTS_TABLE}
        WHERE endpoint_name = %(endpoint_name)s
          AND active = true
        LIMIT 1
    """

    row = hook.get_first(sql, parameters={"endpoint_name": endpoint_name})

    if not row:
        raise ValueError(f"No existe endpoint activo: {endpoint_name}")

    config = row[0]

    if isinstance(config, str):
        return json.loads(config)

    return config


def build_operation(nom: Dict[str, Any], ctrl: Optional[Dict[str, Any]]) -> str:
    if not ctrl:
        return "A"

    ctrl_apepat = clean_text(ctrl.get("apepat"))
    ctrl_apemat = clean_text(ctrl.get("apemat"))
    ctrl_nombre = clean_text(ctrl.get("nombretra"))
    ctrl_rfc = clean_text(ctrl.get("numrfc") or ctrl.get("rfc"))

    if ctrl_apepat in (None, "$$$$$$"):
        return "A"

    nom_apepat = clean_text(nom.get("apellido_pat"))
    nom_apemat = clean_text(nom.get("apellido_mat"))
    nom_nombre = clean_text(nom.get("nombre_tra"))
    nom_rfc = clean_text(nom.get("rfc"))

    cambio_nombre = (
        nom_apepat != ctrl_apepat
        or nom_apemat != ctrl_apemat
        or nom_nombre != ctrl_nombre
    )
    cambio_rfc = nom_rfc != ctrl_rfc

    if cambio_nombre and cambio_rfc:
        return "CNR"
    if cambio_nombre:
        return "CN"
    if cambio_rfc:
        return "CR"

    return "I"


def build_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    operation = row["operation"]
    business_partner = f"AE0{row['cvetra']}"

    if operation == "A":
        return {
            "BusinessPartner": business_partner,
            "BusinessPartnerGrouping": "Z010",
            "BusinessPartnerCategory": "1",
            "FirstName": row.get("nombre_tra"),
            "LastName": f"{row.get('apellido_pat') or ''} {row.get('apellido_mat') or ''}".strip(),
            "CorrespondenceLanguage": "ES",
            "SearchTerm1": row.get("nombre_tra"),
            "SearchTerm2": f"{row.get('apellido_pat') or ''} {row.get('apellido_mat') or ''}".strip(),
            "IsNaturalPerson": "X",
            "to_BusinessPartnerTax": [
                {
                    "BPTaxType": "MX1",
                    "BPTaxNumber": row.get("rfc"),
                }
            ],
            "to_BusinessPartnerAddress": [
                {
                    "Country": "MX",
                    "HouseNumber": "5",
                    "StreetName": "Blvd Manuel Avila Camacho",
                    "PostalCode": "53390",
                    "CityName": "Estado de Mexico",
                    "Region": "MEX",
                    "District": "",
                    "Language": "ES",
                }
            ],
            "to_BusinessPartnerBank": [
                {
                    "BankCountryKey": "MX",
                    "BankNumber": "072",
                    "BankAccount": "111111111",
                }
            ],
            "to_BusinessPartnerRole": [
                {
                    "BusinessPartnerRole": "FLVN00",
                }
            ],
            "to_Supplier": {
                "PaymentIsBlockedForSupplier": False,
                "PostingIsBlocked": False,
                "PurchasingIsBlocked": False,
                "DeletionIndicator": False,
                "to_SupplierCompany": [
                    {
                        "CompanyCode": row.get("cvecia_sap"),
                        "ReconciliationAccount": "20500010",
                        "PaymentMethodsList": "H",
                        "PaymentTerms": "NT00",
                        "LayoutSortingRule": "009",
                        "IsToBeCheckedForDuplicates": True,
                        "CashPlanningGroup": "A14",
                        "DeletionIndicator": False,
                        "SupplierIsBlockedForPosting": False,
                    }
                ],
            },
        }

    if operation in ("CN", "CNR"):
        return {
            "BusinessPartner": business_partner,
            "BusinessPartnerGrouping": "Z010",
            "BusinessPartnerCategory": "1",
            "FirstName": row.get("nombre_tra"),
            "LastName": f"{row.get('apellido_pat') or ''} {row.get('apellido_mat') or ''}".strip(),
            "SearchTerm1": row.get("nombre_tra"),
            "SearchTerm2": f"{row.get('apellido_pat') or ''} {row.get('apellido_mat') or ''}".strip(),
        }

    if operation == "CR":
        return {
            "BPTaxNumber": row.get("rfc"),
        }

    if operation == "B":
        return {
            "PostingIsBlocked": True,
            "PaymentIsBlockedForSupplier": True,
            "PurchasingIsBlocked": True,
        }

    return {}


def classify_records_to_json(
    nom_records_path: str,
    control_records_path: str,
    sociedades_path: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> str:
    nom_records = safe_json_load(nom_records_path)
    control_records = safe_json_load(control_records_path)
    sociedades = safe_json_load(sociedades_path)

    sociedad_by_nom = {
        clean_cvecia(s["cvecia_nom200"]): s
        for s in sociedades
    }

    control_by_key = {
        (clean_key(c.get("cvetra")), clean_cvecia(c.get("cvecia"))): c
        for c in control_records
    }

    result: List[Dict[str, Any]] = []
    skipped_no_sociedad = 0
    skipped_no_cvetra = 0

    for nom in nom_records:
        cvetra = clean_key(nom.get("cvetra"))
        cvecia = clean_cvecia(nom.get("cvecia"))

        if not cvetra:
            skipped_no_cvetra += 1
            continue

        sociedad = sociedad_by_nom.get(cvecia)

        if not sociedad:
            skipped_no_sociedad += 1
            continue

        row = {
            "cvetra": cvetra,
            "fecha_alt": nom.get("fecha_alt"),
            "nombre_tra": clean_text(nom.get("nombre_tra")),
            "apellido_pat": clean_text(nom.get("apellido_pat")),
            "apellido_mat": clean_text(nom.get("apellido_mat")),
            "fecha_nac": nom.get("fecha_nac"),
            "sexo": clean_text(nom.get("sexo")),
            "cveare": nom.get("cveare"),
            "cvedep": nom.get("cvedep"),
            "cvecia": cvecia,
            "cvecia_sap": sociedad.get("cvecia_sap"),
            "sociedad_activa": sociedad.get("activo"),
            "desc_ciudad": clean_text(nom.get("desc_ciudad")),
            "rfc": clean_text(nom.get("rfc")),
            "sub_cta": nom.get("subcta") or nom.get("sub_cta"),
            "estatus": clean_text(nom.get("estatus")),
        }

        ctrl = control_by_key.get((row["cvetra"], row["cvecia"]))
        operation = build_operation(row, ctrl)

        row["operation"] = operation
        row["operation_desc"] = {
            "A": "Alta",
            "CN": "Cambio nombre",
            "CR": "Cambio RFC",
            "CNR": "Cambio nombre y RFC",
            "I": "Igual",
        }.get(operation, operation)
        row["exists_in_control"] = bool(ctrl)
        row["business_partner"] = f"AE0{row['cvetra']}"
        row["payload"] = build_payload(row)

        result.append(row)

    df = pd.DataFrame(result)
    summary = df["operation"].value_counts(dropna=False).to_dict() if not df.empty else {}

    classification = {
        "records": result,
        "summary": summary,
        "skipped_no_sociedad": skipped_no_sociedad,
        "skipped_no_cvetra": skipped_no_cvetra,
        "total_nom2001": len(nom_records),
        "total_after_sociedad_filter": len(result),
    }

    path = os.path.join(output_dir, "classification_result.json")
    write_json(path, classification)

    print(f"Clasificación: {summary}")
    return path


def detect_bajas_to_json(
    control_records_path: str,
    nom_records_path: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> str:
    control_records = safe_json_load(control_records_path)
    nom_records = safe_json_load(nom_records_path)

    nom_active_keys = {
        clean_key(n.get("cvetra"))
        for n in nom_records
        if clean_key(n.get("cvetra"))
    }

    bajas: List[Dict[str, Any]] = []

    for ctrl in control_records:
        cvetra = clean_key(ctrl.get("cvetra"))
        sapstatus = clean_text(ctrl.get("sapstatus"))
        contrlstatus = clean_text(ctrl.get("contrlstatus"))

        if sapstatus == "A" and contrlstatus != "B" and cvetra not in nom_active_keys:
            cvecia = clean_cvecia(ctrl.get("cvecia"))
            row = {
                "cvetra": cvetra,
                "cvecia": cvecia,
                "cvecia_sap": None,
                "business_partner": f"AE0{cvetra}",
                "operation": "B",
                "operation_desc": "Baja",
                "payload": {
                    "PostingIsBlocked": True,
                    "PaymentIsBlockedForSupplier": True,
                    "PurchasingIsBlocked": True,
                },
            }
            bajas.append(row)

    path = os.path.join(output_dir, "bajas_result.json")
    write_json(path, bajas)

    print(f"Bajas detectadas: {len(bajas)}")
    return path


def build_final_payloads_to_json(
    classification_path: str,
    bajas_path: str,
    sociedades_path: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> str:
    classification = safe_json_load(classification_path)
    bajas = safe_json_load(bajas_path)
    sociedades = safe_json_load(sociedades_path)

    sociedad_by_nom = {
        clean_cvecia(s["cvecia_nom200"]): s
        for s in sociedades
    }

    payloads: List[Dict[str, Any]] = []

    for row in classification["records"]:
        if row.get("operation") == "I":
            continue
        payloads.append(row)

    for baja in bajas:
        sociedad = sociedad_by_nom.get(clean_cvecia(baja.get("cvecia")))
        if sociedad:
            baja["cvecia_sap"] = sociedad.get("cvecia_sap")
        payloads.append(baja)

    path = os.path.join(output_dir, "9999D_payloads_to_send.json")
    write_json(path, payloads)

    print(f"Payloads a enviar S4H: {len(payloads)}")
    return path


def write_report(
    classification_path: str,
    bajas_path: str,
    payloads_path: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Dict[str, str]:
    classification = safe_json_load(classification_path)
    bajas = safe_json_load(bajas_path)
    payloads = safe_json_load(payloads_path)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    classified_path = os.path.join(output_dir, f"9999D_clasificacion_{timestamp}.csv")
    summary_path = os.path.join(output_dir, f"9999D_summary_{timestamp}.json")
    payload_path = os.path.join(output_dir, f"9999D_payloads_{timestamp}.json")

    df = pd.DataFrame(classification.get("records", []))

    if not df.empty:
        df.drop(columns=["payload"], errors="ignore").to_csv(
            classified_path,
            index=False,
            encoding="utf-8-sig",
        )
    else:
        pd.DataFrame().to_csv(classified_path, index=False)

    write_json(payload_path, payloads)

    summary = {
        "total_nom2001": classification.get("total_nom2001", 0),
        "total_after_sociedad_filter": classification.get("total_after_sociedad_filter", 0),
        "skipped_no_sociedad": classification.get("skipped_no_sociedad", 0),
        "skipped_no_cvetra": classification.get("skipped_no_cvetra", 0),
        "operations": classification.get("summary", {}),
        "bajas_detectadas": len(bajas),
        "payloads_to_send": len(payloads),
        "classified_path": classified_path,
        "payload_path": payload_path,
    }

    write_json(summary_path, summary)

    print(json.dumps(summary, ensure_ascii=False, indent=2, default=json_default))

    return {
        "classified_path": classified_path,
        "payload_path": payload_path,
        "summary_path": summary_path,
    }


def update_control_after_success(
    row: Dict[str, Any],
    response_summary: Dict[str, Any],
    postgres_conn_id: str = POSTGRES_CONN_ID,
) -> None:
    operation = row.get("operation")
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    code_response = json.dumps( {
        "ok": response_summary.get("ok"),
        "operation": response_summary.get("operation"),
        "status": "S4H_SUCCESS",
    },
    ensure_ascii=False,
    default=json_default,)[:500]

    if operation == "A":
        sql = f"""
            INSERT INTO {CONTROL_TABLE}
            (
                cvetra,
                apepat,
                apemat,
                nombretra,
                cveare,
                fecnac,
                cvedep,
                cvecia,
                desciu,
                subcta,
                numrfc,
                contrlstatus,
                sapstatus,
                rfc,
                code_response
            )
            VALUES
            (
                %(cvetra)s,
                %(apepat)s,
                %(apemat)s,
                %(nombretra)s,
                %(cveare)s,
                %(fecnac)s,
                %(cvedep)s,
                %(cvecia)s,
                %(desciu)s,
                %(subcta)s,
                %(numrfc)s,
                'Y',
                'A',
                %(rfc)s,
                %(code_response)s
            )
            ON CONFLICT DO NOTHING
        """

        hook.run(
            sql,
            parameters={
                "cvetra": row.get("cvetra"),
                "apepat": row.get("apellido_pat"),
                "apemat": row.get("apellido_mat"),
                "nombretra": row.get("nombre_tra"),
                "cveare": row.get("cveare"),
                "fecnac": row.get("fecha_nac"),
                "cvedep": row.get("cvedep"),
                "cvecia": row.get("cvecia"),
                "desciu": row.get("desc_ciudad"),
                "subcta": row.get("sub_cta"),
                "numrfc": row.get("rfc"),
                "rfc": row.get("rfc"),
                "code_response": code_response,
            },
        )
        return

    if operation in ("CN", "CR", "CNR"):
        sql = f"""
            UPDATE {CONTROL_TABLE}
            SET apepat = %(apepat)s,
                apemat = %(apemat)s,
                nombretra = %(nombretra)s,
                numrfc = %(numrfc)s,
                rfc = %(rfc)s,
                contrlstatus = 'Y',
                sapstatus = 'A',
                code_response = %(code_response)s
            WHERE cvetra = %(cvetra)s
              AND cvecia = %(cvecia)s
        """

        hook.run(
            sql,
            parameters={
                "apepat": row.get("apellido_pat"),
                "apemat": row.get("apellido_mat"),
                "nombretra": row.get("nombre_tra"),
                "numrfc": row.get("rfc"),
                "rfc": row.get("rfc"),
                "code_response": code_response,
                "cvetra": row.get("cvetra"),
                "cvecia": row.get("cvecia"),
            },
        )
        return

    if operation == "B":
        sql = f"""
            UPDATE {CONTROL_TABLE}
            SET contrlstatus = 'Y',
                sapstatus = 'B',
                fecha_baja = NOW(),
                code_response = %(code_response)s
            WHERE cvetra = %(cvetra)s
              AND cvecia = %(cvecia)s
        """

        hook.run(
            sql,
            parameters={
                "code_response": code_response,
                "cvetra": row.get("cvetra"),
                "cvecia": row.get("cvecia"),
            },
        )

def send_to_s4h(
    payloads_path: str,
    endpoint_name: str = "NOM2001_9999D_S4H_TARGET",
    dag_id: str = "NOM2001_9999D",
    run_id: str = "",
    task_id: str = "send_to_s4h",
    postgres_conn_id: str = POSTGRES_CONN_ID,
) -> Dict[str, Any]:
    from collections import Counter

    from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.senders.nom2001_9999d_s4h_sender import (
        Nom2001S4HSender,
    )

    all_payloads = safe_json_load(payloads_path)

    # ======================================================================
    # ENVÍO FINAL
    # Envía todas las operaciones válidas a S4H.
    # No envía operación I porque significa "sin cambios".
    #
    # Si en algún momento necesitas excluir temporalmente un empleado puntual,
    # puedes descomentar la línea marcada con 0041509.
    # ======================================================================
    payloads = [
        p for p in all_payloads
        if p.get("operation") in {"A", "CN", "CR", "CNR", "B"}
        # and p.get("cvetra") not in {"0041509"}
    ]

    # ======================================================================
    # OPCIONES DE PRUEBA CONTROLADA
    # Para probar solamente una operación, comenta el bloque de ENVÍO FINAL
    # de arriba y descomenta una de estas líneas.
    # ======================================================================
    # payloads = [p for p in all_payloads if p.get("operation") == "A"][:1]
    # payloads = [p for p in all_payloads if p.get("operation") == "CN"][:5]
    # payloads = [p for p in all_payloads if p.get("operation") == "CR"][:5]
    # payloads = [p for p in all_payloads if p.get("operation") == "CNR"][:5]
    # payloads = [p for p in all_payloads if p.get("operation") == "B"][:5]

    operation_counter_all = Counter(
        p.get("operation", "UNKNOWN")
        for p in all_payloads
    )
    operation_counter_send = Counter(
        p.get("operation", "UNKNOWN")
        for p in payloads
    )

    print("=" * 80)
    print("RESUMEN GENERAL DEL ARCHIVO DE PAYLOADS")
    print("=" * 80)
    for operation, qty in sorted(operation_counter_all.items()):
        print(f"{operation}: {qty}")
    print(f"TOTAL GENERAL: {len(all_payloads)}")
    print("=" * 80)

    print("=" * 80)
    print("RESUMEN DE REGISTROS A ENVIAR EN ESTA CORRIDA")
    print("=" * 80)
    for operation, qty in sorted(operation_counter_send.items()):
        print(f"{operation}: {qty}")
    print(f"TOTAL A ENVIAR: {len(payloads)}")
    print("=" * 80)

    config = load_s4h_endpoint_config(
        endpoint_name=endpoint_name,
        postgres_conn_id=postgres_conn_id,
    )

    total = len(payloads)
    success = 0
    failed = 0
    skipped = 0

    sender = Nom2001S4HSender(config=config)

    try:
        token_info = sender.fetch_csrf_token()
        token = token_info["token"]
    except Exception as exc:
        failed = total
        summary = {
            "total": total,
            "success": 0,
            "failed": failed,
            "skipped": 0,
            "error_stage": "FETCH_CSRF_TOKEN",
            "error_message": str(exc),
            "operation_counter_send": dict(operation_counter_send),
        }

        insert_integration_log(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            phase="SEND_S4H",
            status="FAILED",
            message="Error obteniendo CSRF token de S4H",
            error_message=str(exc),
            send_details=summary,
            postgres_conn_id=postgres_conn_id,
        )

        print(json.dumps(summary, ensure_ascii=False, indent=2, default=json_default))
        print("=" * 80)
        print("RESUMEN ENVÍO S4H")
        print("=" * 80)
        print(f"Total a enviar : {summary['total']}")
        print(f"Exitosos       : {summary['success']}")
        print(f"Fallidos       : {summary['failed']}")
        print(f"Omitidos       : {summary['skipped']}")
        print(f"Etapa error    : {summary['error_stage']}")
        print(f"Error          : {summary['error_message']}")
        print("=" * 80)

        # No levantamos excepción: el DAG queda verde y el error se registra en BD.
        return summary

    insert_integration_log(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        phase="SEND_S4H",
        status="STARTED",
        message=f"Iniciando envío S4H para {total} movimientos",
        send_details={
            "total": total,
            "operation_counter_send": dict(operation_counter_send),
            "operation_counter_all": dict(operation_counter_all),
        },
        postgres_conn_id=postgres_conn_id,
    )

    for row in payloads:
        operation = row.get("operation")
        cvetra = row.get("cvetra")
        cvecia = row.get("cvecia")

        if operation == "I":
            skipped += 1
            continue

        try:
            result = sender.send_row(row, token)

            if result.get("ok"):
                success += 1

                update_control_after_success(
                    row=row,
                    response_summary=result,
                    postgres_conn_id=postgres_conn_id,
                )
            else:
                failed += 1

                insert_integration_log(
                    dag_id=dag_id,
                    run_id=run_id,
                    task_id=task_id,
                    phase="SEND_S4H",
                    status="FAILED",
                    source_file=f"{cvetra}-{cvecia}",
                    message=f"Error enviando empleado {cvetra} operación {operation}",
                    error_message=str(
                        result.get("error")
                        or result.get("response")
                        or result.get("steps")
                    ),
                    send_details={
                        "cvetra": cvetra,
                        "cvecia": cvecia,
                        "operation": operation,
                        "result": result,
                    },
                    postgres_conn_id=postgres_conn_id,
                )

        except Exception as exc:
            failed += 1

            insert_integration_log(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                phase="SEND_S4H",
                status="FAILED",
                source_file=f"{cvetra}-{cvecia}",
                message=f"Excepción enviando empleado {cvetra} operación {operation}",
                error_message=str(exc),
                send_details={
                    "cvetra": cvetra,
                    "cvecia": cvecia,
                    "operation": operation,
                    "payload": row.get("payload"),
                },
                postgres_conn_id=postgres_conn_id,
            )

    final_status = "SUCCESS" if failed == 0 else "PARTIAL_FAILED"

    summary = {
        "total": total,
        "success": success,
        "failed": failed,
        "skipped": skipped,
        "operation_counter_send": dict(operation_counter_send),
    }

    insert_integration_log(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        phase="SEND_S4H",
        status=final_status,
        message="Envío S4H finalizado",
        send_details=summary,
        postgres_conn_id=postgres_conn_id,
    )

    print(json.dumps(summary, ensure_ascii=False, indent=2, default=json_default))

    print("=" * 80)
    print("RESUMEN ENVÍO S4H")
    print("=" * 80)
    print(f"Total enviados : {total}")
    print(f"Exitosos       : {success}")
    print(f"Fallidos       : {failed}")
    print(f"Omitidos       : {skipped}")
    print("=" * 80)

    # Importante: no levantamos excepción aunque haya fallidos.
    # Los errores por empleado quedan registrados en ctrlplane.tbl_airflow_integration_log_finz.
    return summary
