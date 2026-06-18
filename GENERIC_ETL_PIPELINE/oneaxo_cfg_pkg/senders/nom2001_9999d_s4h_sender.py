from __future__ import annotations

from typing import Any, Dict

import requests
from airflow.models import Variable


class Nom2001S4HSender:
    DEFAULT_ENDPOINTS = {
        "ALTA": "/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner?sap-client=110",
        "CN": "/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner('{{business_partner}}')?sap-client=110",
        "CR": "/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartnerTaxNumber(BusinessPartner='{{business_partner}}',BPTaxType='MX1')?sap-client=110",
        "BAJA": "/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_Supplier('{{business_partner}}')?sap-client=110",
    }

    DEFAULT_METHODS = {
        "ALTA": "POST",
        "CN": "PATCH",
        "CR": "PATCH",
        "BAJA": "PATCH",
    }

    def __init__(self, config: Dict[str, Any], timeout: int = 60) -> None:
        self.config = config
        self.timeout = timeout
        self.base_url = config.get("http_base_url", "").rstrip("/")

        if not self.base_url:
            raise ValueError("Config S4H sin http_base_url")

        self.auth_config = config.get("auth", {})
        self.headers = dict(config.get("headers", {}))
        self.endpoints = {**self.DEFAULT_ENDPOINTS, **config.get("endpoints", {})}
        self.methods = {**self.DEFAULT_METHODS, **config.get("methods", {})}
        self.session = requests.Session()

        variable_name = self.auth_config.get(
            "airflow_variable_name",
            "sap_s4h_9999d_credentials",
        )

        credentials = Variable.get(variable_name, deserialize_json=True)
        self.username = credentials["username"]
        self.password = credentials["password"]

    def _full_url(self, endpoint: str) -> str:
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        if endpoint.startswith("/"):
            return f"{self.base_url}{endpoint}"
        return f"{self.base_url}/{endpoint}"

    def fetch_csrf_token(self) -> Dict[str, Any]:
        token_endpoint = self.auth_config.get("token_endpoint")
        if not token_endpoint:
            raise ValueError("Config S4H sin auth.token_endpoint")

        response = self.session.get(
            self._full_url(token_endpoint),
            auth=(self.username, self.password),
            headers={"x-csrf-token": "Fetch", "Accept": "application/json"},
            timeout=self.timeout,
            verify=False,
        )

        response.raise_for_status()

        token = response.headers.get("x-csrf-token")
        if not token:
            raise RuntimeError("SAP no regresó x-csrf-token")

        return {"token": token, "cookies": self.session.cookies.get_dict()}

    def _render_endpoint(self, endpoint_template: str, row: Dict[str, Any]) -> str:
        business_partner = row.get("business_partner") or f"AE0{row.get('cvetra')}"
        company_code = row.get("cvecia_sap") or row.get("company_code") or ""

        return (
            endpoint_template
            .replace("{{business_partner}}", str(business_partner))
            .replace("{{company_code}}", str(company_code))
        )

    def _request(
        self,
        method: str,
        endpoint: str,
        payload: Dict[str, Any],
        token: str,
    ) -> Dict[str, Any]:
        headers = dict(self.headers)
        headers["x-csrf-token"] = token

        if method.upper() in ("PATCH", "MERGE", "PUT"):
            headers["If-Match"] = "*"

        headers.setdefault("Accept", "application/json")
        headers.setdefault("Content-Type", "application/json")

        response = self.session.request(
            method=method.upper(),
            url=self._full_url(endpoint),
            auth=(self.username, self.password),
            headers=headers,
            json=payload,
            timeout=self.timeout,
            verify=False,
        )

        text = response.text or ""

        try:
            body = response.json() if text else {}
        except Exception:
            body = {"raw_response": text[:2000]}

        result = {
            "method": method.upper(),
            "url": response.url,
            "status_code": response.status_code,
            "ok": 200 <= response.status_code <= 299,
            "response": body,
        }

        if not result["ok"]:
            result["error"] = text[:2000]

        return result

    def send_row(self, row: Dict[str, Any], token: str) -> Dict[str, Any]:
        operation = row.get("operation")
        payload = row.get("payload") or {}

        if operation == "A":
            endpoint = self._render_endpoint(self.endpoints["ALTA"], row)
            return self._request(self.methods["ALTA"], endpoint, payload, token)

        if operation == "CN":
            endpoint = self._render_endpoint(self.endpoints["CN"], row)
            payload["SearchTerm1"] = str(payload.get("SearchTerm1") or "")[:20]
            payload["SearchTerm2"] = str(payload.get("SearchTerm2") or "")[:20]
            return self._request(self.methods["CN"], endpoint, payload, token)

        if operation == "CR":
            endpoint = self._render_endpoint(self.endpoints["CR"], row)
            cr_payload = {"BPTaxNumber": row.get("rfc")}
            return self._request(self.methods["CR"], endpoint, cr_payload, token)

        if operation == "CNR":
            cn_payload = {
                "BusinessPartner": payload.get("BusinessPartner"),
                "BusinessPartnerGrouping": payload.get("BusinessPartnerGrouping"),
                "BusinessPartnerCategory": payload.get("BusinessPartnerCategory"),
                "FirstName": payload.get("FirstName"),
                "LastName": payload.get("LastName"),
                "SearchTerm1": str(payload.get("SearchTerm1") or "")[:20],
                "SearchTerm2": str(payload.get("SearchTerm2") or "")[:20],
            }

            cn_endpoint = self._render_endpoint(self.endpoints["CN"], row)
            cn_result = self._request(self.methods["CN"], cn_endpoint, cn_payload, token)

            if not cn_result["ok"]:
                return {
                    "ok": False,
                    "operation": "CNR",
                    "steps": {"CN": cn_result},
                }

            cr_endpoint = self._render_endpoint(self.endpoints["CR"], row)
            cr_payload = {"BPTaxNumber": row.get("rfc")}
            cr_result = self._request(self.methods["CR"], cr_endpoint, cr_payload, token)

            return {
                "ok": bool(cr_result["ok"]),
                "operation": "CNR",
                "steps": {
                    "CN": cn_result,
                    "CR": cr_result,
                },
            }

        if operation == "B":
            endpoint = self._render_endpoint(self.endpoints["BAJA"], row)
            return self._request(self.methods["BAJA"], endpoint, payload, token)

        return {
            "ok": True,
            "skipped": True,
            "message": f"Operación no enviable: {operation}",
        }