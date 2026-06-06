from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from pathlib import Path
import json
import shutil
import uuid


POSTGRES_CONN_ID = "gobierno_central_postgres"

INPUT_DIR = Path("/opt/airflow/data/input/212")
WORK_DIR = Path("/opt/airflow/data/processing/212")
ECC_OUT_DIR = Path("/opt/airflow/data/output/212/ecc")
S4H_OUT_DIR = Path("/opt/airflow/data/output/212/s4h")
S3_BACKUP_DIR = Path("/opt/airflow/data/output/212/s3_backup")


def lookup_global_dictionary(key: str) -> str | None:
    """
    Consulta real a Global Dictionary.
    Ejemplo:
    FINANZAS-sociedades-M003
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql = """
        SELECT target_value
        FROM ctrlplane.tbl_cat_gd
        WHERE lookupkey = %s
        LIMIT 1
    """

    row = hook.get_first(sql, parameters=(key,))

    if row:
        return str(row[0]).strip()

    return None


@dag(
    dag_id="TEST_212_POLIZA_NOMINA",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["TEST", "212", "POLIZA_NOMINA"],
)
def test_212_poliza_nomina():

    @task
    def prepare_dirs():
        for folder in [
            INPUT_DIR,
            WORK_DIR,
            ECC_OUT_DIR,
            S4H_OUT_DIR,
            S3_BACKUP_DIR,
        ]:
            folder.mkdir(parents=True, exist_ok=True)

        return True

    @task
    def list_files(_):
        files = sorted(INPUT_DIR.glob("*.TXT")) + sorted(INPUT_DIR.glob("*.txt"))

        if not files:
            raise AirflowFailException(
                f"No hay archivos TXT en {INPUT_DIR}"
            )

        return [str(f) for f in files]

    @task
    def process_file(file_path: str):

        source_path = Path(file_path)
        filename = source_path.name

        run_uuid = str(uuid.uuid4())

        work_path = WORK_DIR / filename

        shutil.copy2(source_path, work_path)

        backup_path = S3_BACKUP_DIR / filename
        shutil.copy2(work_path, backup_path)

        with open(work_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [
                line.rstrip("\n\r")
                for line in f
                if line.strip()
            ]

        if not lines:
            raise AirflowFailException(
                f"Archivo vacío: {filename}"
            )

        first_line = lines[0]

        # NiFi:
        # .{34}(.{4})
        sociedad = first_line[34:38].strip()

        gd_key = f"FINANZAS-sociedades-{sociedad}"

        society_flag = lookup_global_dictionary(gd_key)

        print("=" * 80)
        print(f"Archivo: {filename}")
        print(f"Sociedad detectada: {sociedad}")
        print(f"Global Dictionary Key: {gd_key}")
        print(f"Global Dictionary Value: {society_flag}")
        print("=" * 80)

        # ============================================================
        # ECC
        # ============================================================
        if society_flag != "1":

            ecc_target = ECC_OUT_DIR / filename

            shutil.copy2(work_path, ecc_target)

            print(f"Archivo enviado a ECC: {ecc_target}")

            return {
                "filename": filename,
                "sociedad": sociedad,
                "route": "ECC",
                "status": "SUCCEEDED",
                "global_dictionary_key": gd_key,
                "global_dictionary_value": society_flag,
                "output": str(ecc_target),
                "backup": str(backup_path),
            }

        # ============================================================
        # S4H
        # ============================================================

        polizas = {}

        for line in lines:

            item_type = line[0:1]
            idext = line[1:13].strip()

            if not idext:
                raise AirflowFailException(
                    f"Línea sin IDEXT en {filename}: {line}"
                )

            if idext not in polizas:
                polizas[idext] = {
                    "IDEXT": idext,
                    "TRANS_UUID": run_uuid,
                    "SOURCEID": "NOMINA",
                    "filename": filename,
                    "sociedad": sociedad,
                    "header": None,
                    "details": [],
                }

            # ========================================================
            # HEADER
            # ========================================================
            if item_type == "H":

                polizas[idext]["header"] = parse_header(line)

            # ========================================================
            # DETAIL
            # ========================================================
            elif item_type == "D":

                polizas[idext]["details"].append(
                    parse_detail(line)
                )

            else:

                raise AirflowFailException(
                    f"Tipo de línea no reconocido '{item_type}' "
                    f"en {filename}: {line}"
                )

        output_files = []

        for idext, payload in polizas.items():

            if payload["header"] is None:
                raise AirflowFailException(
                    f"Póliza {idext} no tiene cabecera H"
                )

            expected_details = safe_int(
                payload["header"].get("TotalNumberOfLineItem")
            )

            if (
                expected_details
                and expected_details != len(payload["details"])
            ):

                print(
                    f"WARNING: IDEXT {idext}: "
                    f"header dice {expected_details} detalles, "
                    f"pero se leyeron {len(payload['details'])}"
                )

            json_path = (
                S4H_OUT_DIR /
                f"{source_path.stem}_{idext}.json"
            )

            with open(json_path, "w", encoding="utf-8") as jf:
                json.dump(
                    payload,
                    jf,
                    ensure_ascii=False,
                    indent=2
                )

            output_files.append(str(json_path))

            print(f"JSON S4H generado: {json_path}")

        return {
            "filename": filename,
            "sociedad": sociedad,
            "route": "S4H",
            "status": "SUCCEEDED",
            "polizas": len(polizas),
            "global_dictionary_key": gd_key,
            "global_dictionary_value": society_flag,
            "output_files": output_files,
            "backup": str(backup_path),
        }

    # ================================================================
    # PARSE HEADER
    # ================================================================
    def parse_header(line: str) -> dict:

        return {
            "ItemType": line[0:1].strip(),
            "IDEXT_H": line[1:13].strip(),
            "Head_1": line[13:25].strip(),
            "DocumentDate": line[25:33].strip(),
            "PostingDate": line[33:41].strip(),
            "DocType": line[41:43].strip(),
            "Sociedad": line[43:47].strip(),
            "Reference": line[47:59].strip(),
            "Description": line[59:84].strip(),
            "Currency": line[84:89].strip(),
            "TotalNumberOfLineItem": line[89:99].strip(),
            "raw": line,
        }

    # ================================================================
    # PARSE DETAIL
    # ================================================================
    def parse_detail(line: str) -> dict:

        return {
            "ItemType": line[0:1].strip(),
            "IDEXT_D": line[1:13].strip(),
            "LineNumber": line[13:16].strip(),
            "GLAccount": line[16:26].strip(),
            "Amount": line[26:41].strip(),
            "Reference": line[41:59].strip(),
            "CostCenterName": line[59:109].strip(),
            "CostCenter": line[109:130].strip(),
            "Description": line[130:].strip(),
            "raw": line,
        }

    def safe_int(value):

        try:

            value = str(value).strip()

            if not value:
                return None

            return int(value)

        except Exception:
            return None

    ready = prepare_dirs()

    files = list_files(ready)

    process_file.expand(file_path=files)


test_212_poliza_nomina()