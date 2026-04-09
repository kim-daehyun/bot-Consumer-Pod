from __future__ import annotations

from typing import Any, Dict, Optional

import requests


class InferenceClient:
    def __init__(
        self,
        base_url: str,
        timeout_sec: float = 3.0,
        max_retries: int = 1,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.session = requests.Session()

    def predict_fe(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("/predict/fe", payload)

    def predict_be(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("/predict/be", payload)

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        last_error: Optional[Exception] = None

        for _ in range(self.max_retries + 1):
            try:
                response = self.session.post(
                    f"{self.base_url}{path}",
                    json=payload,
                    timeout=self.timeout_sec,
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                last_error = e

        raise RuntimeError(f"Inference request failed: path={path}, error={last_error}")