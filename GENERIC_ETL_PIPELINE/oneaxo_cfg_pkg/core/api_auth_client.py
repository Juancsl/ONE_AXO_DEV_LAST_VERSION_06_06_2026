# Fichero: dags/include/clients/api_client.py

import requests
import logging
from datetime import datetime, timedelta
from typing import Any, Dict
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.exceptions import RecoverableError

class ApiClient:
    """
    Cliente para una API con token de refresco, que recibe las credenciales
    directamente en lugar de usar un Hook de Airflow.
    """
    def __init__(self, api_base_url: str, auth_url: str, client_id: str, client_secret: str, username: str, password: str):
        self.api_base_url = api_base_url
        self.auth_url = auth_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password

        self._session = requests.Session()
        self._token = None
        self._token_expiry_time = datetime.utcnow()
    
    def _get_new_token(self):
        logging.info(f"Solicitando nuevo token de acceso desde {self.auth_url}")
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'username': self.username,
            'password': self.password
        }
        response = requests.post(self.auth_url, data=payload, timeout=15)
        response.raise_for_status()
        token_data = response.json()
        
        self._token = token_data['access_token']
        expires_in = token_data.get('expires_in', 3600)
        self._token_expiry_time = datetime.utcnow() + timedelta(seconds=expires_in)
        
        self._session.headers.update({
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        })
        logging.info("Token renovado y sesión actualizada.")
        
    def _ensure_valid_token(self):
        if datetime.utcnow() >= self._token_expiry_time - timedelta(seconds=60):
            self._get_new_token()
    
    def get(self, endpoint: str, params: Dict[str, Any] = None) -> requests.Response:
        self._ensure_valid_token()
        full_url = f"{self.api_base_url}{endpoint}"
        logging.info(f"Ejecutando petición a: {full_url} con params: {params}")
        response = self._session.get(full_url, params=params, timeout=45)
        response.raise_for_status()
        return response

    def post(self, endpoint: str, payload: Dict[str, Any]) -> requests.Response:
        self._ensure_valid_token()
        full_url = f"{self.api_base_url}{endpoint}"
        logging.info(f"Ejecutando petición a: {full_url} con payload {payload}" )
        
        try:
            response = self._session.post(full_url, json=payload, timeout=180)
            # Esta línea ya lanza un HTTPError para status >= 400
            response.raise_for_status()
            return response
            
        except requests.exceptions.HTTPError as http_err:

            if http_err.response.status_code >= 500:
                raise RecoverableError(
                    message=f"Fallo recuperable de API: Servidor respondió con {http_err.response.status_code}",
                    status=http_err.response.status_code,
                    error_text=http_err.response.text,
                    target=full_url
                ) from http_err
            raise 
            
        except requests.exceptions.RequestException as req_err:
            raise RecoverableError(f"Fallo recuperable de red: {req_err}") from req_err