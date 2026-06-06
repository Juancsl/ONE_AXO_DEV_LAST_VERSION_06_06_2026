from __future__ import annotations

import os
import re
import posixpath
import tempfile
import shutil
from datetime import datetime

import boto3
import paramiko
from pypdf import PdfReader

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


USE_LOCAL_TEST_FILES = True
LOCAL_INPUT_DIR = "/opt/airflow/data/input/1045"

SOURCE_SFTP_HOST = "qa.filegateway.us.hsbc.com"
SOURCE_SFTP_PORT = 22
SOURCE_SFTP_USERNAME = "HFG01994"
SOURCE_SFTP_KEY_PATH = "/data/nifi/sftp_keys/hsbc_sftp_openssh.ppk"
SOURCE_REMOTE_PATH = "/DEV/Inbox"

S3_BUCKET = "one-axo-finance"
S3_STAGE_PATH = "bancos/hsbc/1045C/"

TARGET_SFTP_HOST = "10.10.4.186"
TARGET_SFTP_PORT = 22
TARGET_SFTP_USERNAME = "ubuntu"
TARGET_SFTP_KEY_PATH = "/data/nifi/sftp_keys/shared_pk.pem"
TARGET_REMOTE_BASE_PATH = "/mnt/axoetor0401/BANCOS_QAS/021 HSBC/PAGOS/COMPROBANTES"

PDF_EXTENSIONS = (".pdf", ".PDF")


def _connect_sftp(host: str, port: int, username: str, private_key_path: str):
    key = paramiko.RSAKey.from_private_key_file(private_key_path)
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, pkey=key)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def _mkdir_p_sftp(sftp, remote_directory: str):
    parts = remote_directory.strip("/").split("/")
    current = ""

    for part in parts:
        current += f"/{part}"
        try:
            sftp.stat(current)
        except FileNotFoundError:
            sftp.mkdir(current)


def extract_pdf_text(local_pdf_path: str) -> str:
    reader = PdfReader(local_pdf_path)
    text_parts = []

    for page in reader.pages:
        text_parts.append(page.extract_text() or "")

    return "\n".join(text_parts)


def extract_cuenta_ordenante(text: str) -> str | None:
    patterns = [
        r"Cuenta Ordenante\s+(.+?)\s+Detalles",
        r"Cuenta Ordenante\s+(.+?)\s+DETALLES",
        r"Cuenta de débito\s+(.+?)\s+Nombre",
        r"Cuenta Ordenante:\s+(.+?)\s+DETALLES DEL CARGO",
        r"Cuenta de débito\s+(.+?)\s+Reporte de Pago de Servicios",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.DOTALL)
        if match:
            value = match.group(1).strip()
            if value:
                return value

    return None


def parse_date_from_filename(filename: str) -> dict:
    parts = filename.split(".")
    if len(parts) < 3:
        raise ValueError(f"Nombre de archivo inválido: {filename}")

    date_token = parts[2]

    year = date_token[1:3]
    month = date_token[3:5]
    day = date_token[5:7]

    if not year or not month or not day:
        raise ValueError(f"No se pudo extraer fecha desde filename: {filename}")

    return {"year": year, "month": month, "day": day}


def get_tipo_operacion(filename: str) -> str:
    parts = filename.split(".")
    if len(parts) < 2:
        raise ValueError(f"Nombre de archivo inválido: {filename}")

    operation = parts[1].upper()

    mapping = {
        "BILL_PAY": "PAGO_DE_SERVICIOS",
        "SPEI_PAY": "PAGOS_SPEI_ENVIADOS",
        "MX_SPID": "PAGOS_SPID_EMITIDOS",
        "PRIORITY": "TRANSFERENCIAS",
    }

    if operation not in mapping:
        raise ValueError(f"Tipo de operación no soportado: {operation}")

    return mapping[operation]


def get_mes_nombre(month: str) -> str:
    mapping = {
        "01": "ENE",
        "02": "FEB",
        "03": "MAR",
        "04": "ABR",
        "05": "MAY",
        "06": "JUN",
        "07": "JUL",
        "08": "AGO",
        "09": "SEP",
        "10": "OCT",
        "11": "NOV",
        "12": "DIC",
    }

    if month not in mapping:
        raise ValueError(f"Mes inválido: {month}")

    return mapping[month]


def lookup_sociedad_global_dictionary(cuenta_ordenante: str) -> str | None:
    lookup_key = f"021:{cuenta_ordenante.strip()}"

    sql = """
        SELECT target_value
        FROM finz.tbl_bank_accounts_cache
        WHERE lookupkey = %s
        LIMIT 1
    """

    hook = PostgresHook(postgres_conn_id="gobierno_central_postgres")
    result = hook.get_first(sql, parameters=(lookup_key,))

    if not result:
        print(f"No se encontró sociedad para lookup_key={lookup_key}")
        return None

    raw_value = result[0]  # Ejemplo: M033|S4 o MULT|NOSAP
    sociedad = raw_value.split("|")[0].strip()

    print(f"Sociedad encontrada para {lookup_key}: {sociedad}")
    return sociedad


def build_remote_path(
    filename: str,
    text_pdf: str,
    cuenta_ordenante: str | None,
    sociedad: str | None,
) -> str:
    date_data = parse_date_from_filename(filename)
    tipo_oper = get_tipo_operacion(filename)
    mesnom = get_mes_nombre(date_data["month"])

    year = date_data["year"]
    month = date_data["month"]
    day = date_data["day"]

    if sociedad == "MULT" or "MULTIBRAND" in text_pdf:
        return f"MULTI/20{year}{month}/{day}{mesnom}{year}/{tipo_oper}"

    if cuenta_ordenante and sociedad and sociedad.lower() != "null":
        return f"{sociedad}/20{year}{month}/{day}{mesnom}{year}/{tipo_oper}"

    return f"20{year}{month}/{day}{mesnom}{year}/{tipo_oper}/NO_IDENTIFICADOS"


@dag(
    dag_id="HSBC_1045C_R150_COMPROBANTES",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["HSBC", "1045C", "R150", "SFTP", "PDF"],
)
def hsbc_1045c_r150_comprobantes():

    @task
    def list_sftp_files() -> list[dict]:
        if USE_LOCAL_TEST_FILES:
            files = []

            for filename in os.listdir(LOCAL_INPUT_DIR):
                if not filename.endswith(PDF_EXTENSIONS):
                    continue

                full_path = os.path.join(LOCAL_INPUT_DIR, filename)

                files.append({
                    "filename": filename,
                    "remote_path": full_path,
                    "size": os.path.getsize(full_path),
                    "modified_time": os.path.getmtime(full_path),
                })

            print(f"Archivos encontrados localmente: {files}")
            return files

        sftp, transport = _connect_sftp(
            SOURCE_SFTP_HOST,
            SOURCE_SFTP_PORT,
            SOURCE_SFTP_USERNAME,
            SOURCE_SFTP_KEY_PATH,
        )

        try:
            files = []

            for attr in sftp.listdir_attr(SOURCE_REMOTE_PATH):
                filename = attr.filename

                if not filename.endswith(PDF_EXTENSIONS):
                    continue

                files.append({
                    "filename": filename,
                    "remote_path": posixpath.join(SOURCE_REMOTE_PATH, filename),
                    "size": attr.st_size,
                    "modified_time": attr.st_mtime,
                })

            print(f"Archivos encontrados en SFTP: {files}")
            return files

        finally:
            sftp.close()
            transport.close()

    @task
    def process_file(file_info: dict) -> dict:
        filename = file_info["filename"]
        source_remote_file = file_info["remote_path"]

        print(f"Procesando archivo: {filename}")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_pdf_path = os.path.join(tmpdir, filename)

            if USE_LOCAL_TEST_FILES:
                shutil.copy2(source_remote_file, local_pdf_path)
                print(f"Archivo copiado localmente: {source_remote_file}")
            else:
                source_sftp, source_transport = _connect_sftp(
                    SOURCE_SFTP_HOST,
                    SOURCE_SFTP_PORT,
                    SOURCE_SFTP_USERNAME,
                    SOURCE_SFTP_KEY_PATH,
                )

                try:
                    source_sftp.get(source_remote_file, local_pdf_path)
                    print(f"Archivo descargado: {source_remote_file}")

                    source_sftp.remove(source_remote_file)
                    print(f"Archivo eliminado de origen: {source_remote_file}")

                finally:
                    source_sftp.close()
                    source_transport.close()

            try:
                s3_key = f"{S3_STAGE_PATH}{filename}"

                try:
                    boto3.client("s3").upload_file(local_pdf_path, S3_BUCKET, s3_key)
                    print(f"Archivo enviado a S3: s3://{S3_BUCKET}/{s3_key}")
                except Exception as s3_exc:
                    print(f"WARNING: No se pudo subir a S3 en prueba local: {s3_exc}")
                    s3_key = None

                text_pdf = extract_pdf_text(local_pdf_path)
                print("Texto PDF extraído correctamente")

                cuenta_ordenante = extract_cuenta_ordenante(text_pdf)

                if cuenta_ordenante:
                    print(f"Cuenta Ordenante encontrada: {cuenta_ordenante}")
                else:
                    print("No se encontró Cuenta Ordenante")

                sociedad = lookup_sociedad_global_dictionary(cuenta_ordenante) if cuenta_ordenante else None
                print(f"Sociedad encontrada: {sociedad}")

                remote_path = build_remote_path(
                    filename=filename,
                    text_pdf=text_pdf,
                    cuenta_ordenante=cuenta_ordenante,
                    sociedad=sociedad,
                )

                target_remote_dir = posixpath.join(TARGET_REMOTE_BASE_PATH, remote_path)
                target_remote_file = posixpath.join(target_remote_dir, filename)

                print(f"Ruta destino calculada: {target_remote_file}")

                if USE_LOCAL_TEST_FILES:
                    local_output_dir = "/opt/airflow/data/output/1045"
                    local_final_dir = os.path.join(local_output_dir, remote_path.replace("/", os.sep))
                    os.makedirs(local_final_dir, exist_ok=True)

                    local_final_file = os.path.join(local_final_dir, filename)
                    shutil.copy2(local_pdf_path, local_final_file)

                    print(f"Archivo copiado a salida local: {local_final_file}")
                    final_file = local_final_file

                else:
                    target_sftp, target_transport = _connect_sftp(
                        TARGET_SFTP_HOST,
                        TARGET_SFTP_PORT,
                        TARGET_SFTP_USERNAME,
                        TARGET_SFTP_KEY_PATH,
                    )

                    try:
                        _mkdir_p_sftp(target_sftp, target_remote_dir)
                        target_sftp.put(local_pdf_path, target_remote_file)
                        print(f"Archivo enviado a destino: {target_remote_file}")

                    finally:
                        target_sftp.close()
                        target_transport.close()

                    final_file = target_remote_file

                return {
                    "filename": filename,
                    "status": "SUCCEEDED",
                    "cuenta_ordenante": cuenta_ordenante,
                    "sociedad": sociedad,
                    "remote_file": final_file,
                    "s3_key": s3_key,
                }

            except Exception as exc:
                print(f"Error procesando {filename}: {exc}")
                raise

    files = list_sftp_files()
    process_file.expand(file_info=files)


hsbc_1045c_r150_comprobantes()