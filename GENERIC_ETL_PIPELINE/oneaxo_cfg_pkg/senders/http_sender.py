# Archivo: GENERIC_ETL_PIPELINE/oneaxo_cfg_pkg/senders/http_sender.py

import json
import logging

import requests
from airflow.models import Variable

from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.api_auth_client import ApiClient
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.exceptions import HttpSenderError


class HttpApiSender:

    def send(self, payload: bytes, config: dict, **kwargs) -> dict:
        """
        Envía un payload a un endpoint HTTP.

        Estrategias soportadas:
        - none
        - basic
        - bearer
        - bearer_refresh
        - sap_csrf_basic
        """

        auth_config = config.get("auth", {})
        auth_type = auth_config.get("type", "none")

        endpoint = config.get("http_endpoint")

        # ==========================================================
        # ESTRATEGIA 1
        # bearer_refresh usando ApiClient
        # ==========================================================

        if auth_type == "bearer_refresh":

            variable_name = auth_config.get("airflow_variable_name")

            if not variable_name:
                raise ValueError(
                    "La auth 'bearer_refresh' requiere 'airflow_variable_name'."
                )

            try:
                creds = Variable.get(variable_name, deserialize_json=True)

                client = ApiClient(**creds)

                json_payload = json.loads(payload.decode("utf-8"))

                response = client.post(
                    endpoint=endpoint,
                    payload=json_payload,
                )

                logging.info(
                    f"Petición (ApiClient) exitosa con status: {response.status_code}"
                )

                return {
                    "status_code": response.status_code,
                    "response_text": response.text,
                }

            except Exception as e:

                raise HttpSenderError(
                    message=f"{e}",
                    status_code=getattr(e, "status", None),
                    response_text=getattr(e, "error_text", None),
                    target=getattr(e, "target", None),
                ) from e

        # ==========================================================
        # ESTRATEGIA 2
        # SAP CSRF BASIC AUTH
        # ==========================================================

        if auth_type == "sap_csrf_basic":

            base_url = config.get("http_base_url")
            post_endpoint = config.get("http_endpoint")
            token_endpoint = auth_config.get("token_endpoint")

            if not base_url:
                raise ValueError("Falta config.http_base_url")

            if not post_endpoint:
                raise ValueError("Falta config.http_endpoint")

            if not token_endpoint:
                raise ValueError(
                    "La auth sap_csrf_basic requiere auth.token_endpoint"
                )

            variable_name = auth_config.get("airflow_variable_name")

            if not variable_name:
                raise ValueError(
                    "La auth sap_csrf_basic requiere airflow_variable_name"
                )

            creds = Variable.get(variable_name, deserialize_json=True)

            username = creds.get("username")
            password = creds.get("password")

            if not username or not password:
                raise ValueError(
                    "La variable Airflow no contiene username/password"
                )

            token_url = (
                f"{base_url.rstrip('/')}/{token_endpoint.lstrip('/')}"
            )

            post_url = (
                f"{base_url.rstrip('/')}/{post_endpoint.lstrip('/')}"
            )

            session = requests.Session()
            session.auth = (username, password)

            try:

                logging.info(
                    f"Solicitando CSRF token SAP a {token_url}"
                )

                token_response = session.get(
                    token_url,
                    headers={
                        "Accept": "application/json",
                        "x-csrf-token": "fetch",
                    },
                    timeout=180,
                )

                token_response.raise_for_status()

                csrf_token = token_response.headers.get("x-csrf-token")

                if not csrf_token:
                    raise ValueError(
                        "SAP no devolvió x-csrf-token"
                    )

                logging.info("CSRF token SAP obtenido correctamente.")

                headers = config.get("headers", {}).copy()

                headers["x-csrf-token"] = csrf_token

                if "Content-Type" not in headers:
                    headers["Content-Type"] = "application/json"

                if "Accept" not in headers:
                    headers["Accept"] = "application/json"

                logging.info(
                    f"Enviando POST SAP a {post_url}"
                )

                response = session.post(
                    url=post_url,
                    data=payload,
                    headers=headers,
                    timeout=180,
                )

                response.raise_for_status()

                logging.info(
                    f"POST SAP exitoso. Status={response.status_code}"
                )

                return {
                    "url": post_url,
                    "status_code": response.status_code,
                    "response_text": response.text,
                }

            except requests.exceptions.HTTPError as http_err:

                raise HttpSenderError(
                    message=f"Error HTTP SAP {http_err.response.status_code}",
                    status_code=http_err.response.status_code,
                    response_text=http_err.response.text,
                    target=post_url,
                ) from http_err

            except requests.exceptions.RequestException as e:

                raise HttpSenderError(
                    message=f"Error de red SAP hacia {post_url}: {e}",
                    target=post_url,
                ) from e

        # ==========================================================
        # ESTRATEGIA 3
        # requests simple
        # ==========================================================

        base_url = config.get("http_base_url")

        if not base_url or not endpoint:
            raise ValueError(
                "La configuración para auth simple debe incluir "
                "'http_base_url' y 'http_endpoint'."
            )

        full_url = (
            f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        )

        method = config.get("http_method", "POST")

        headers = config.get("headers", {}).copy()

        auth_param = None

        variable_name = auth_config.get("airflow_variable_name")

        if variable_name:

            creds = Variable.get(variable_name, deserialize_json=True)

            if auth_type == "basic":

                auth_param = (
                    creds.get("username"),
                    creds.get("password"),
                )

                logging.info(
                    f"Usando autenticación Básica para "
                    f"el usuario: {creds.get('username')}"
                )

            elif auth_type == "bearer":

                headers["Authorization"] = (
                    f"Bearer {creds.get('token')}"
                )

                logging.info(
                    "Usando autenticación por Token Bearer estático."
                )

        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

        try:

            logging.info(
                f"Enviando petición {method} a {full_url}"
            )

            response = requests.request(
                method=method,
                url=full_url,
                data=payload,
                headers=headers,
                auth=auth_param,
                timeout=180,
            )

            response.raise_for_status()

            logging.info(
                f"Petición HTTP exitosa con status: {response.status_code}"
            )

            return {
                "url": full_url,
                "status_code": response.status_code,
                "response_text": response.text,
            }

        except requests.exceptions.HTTPError as http_err:

            raise HttpSenderError(
                message=f"Error HTTP {http_err.response.status_code}",
                status_code=http_err.response.status_code,
                response_text=http_err.response.text,
                target=full_url,
            ) from http_err

        except requests.exceptions.RequestException as e:

            raise HttpSenderError(
                message=f"Error de red al enviar a {full_url}: {e}"
            ) from e