from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

import requests


logger = logging.getLogger(__name__)


class InferenceClient:
    """
    Consumer Pod에서 FastAPI Inference Pod를 호출하는 클라이언트.

    역할:
    - FE feature -> /predict/fe 호출
    - BE feature -> /predict/be 호출
    - timeout / retry / 실패 로그 처리
    """

    def __init__(
        self,
        base_url: str,
        timeout_sec: float = 0.5,
        max_retries: int = 1,
        retry_backoff_sec: float = 0.2,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.retry_backoff_sec = retry_backoff_sec

    # ------------------------------------------------------------------
    # public api
    # ------------------------------------------------------------------
    def call_fe_inference(self, feature_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        FE feature payload를 FastAPI /predict/fe 로 전달한다.
        """
        return self._post_with_retry(
            endpoint="/predict/fe",
            payload=feature_payload,
            model_type="fe",
        )

    def call_be_inference(self, feature_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        BE feature payload를 FastAPI /predict/be 로 전달한다.
        """
        return self._post_with_retry(
            endpoint="/predict/be",
            payload=feature_payload,
            model_type="be",
        )

    # ------------------------------------------------------------------
    # internal
    # ------------------------------------------------------------------
    def _post_with_retry(
        self,
        endpoint: str,
        payload: Dict[str, Any],
        model_type: str,
    ) -> Dict[str, Any]:
        """
        POST 요청을 보내고, 실패 시 재시도한다.

        재시도 정책:
        - 최초 1회 + max_retries 만큼 재시도
        - timeout / connection error / 5xx 계열 예외 시 재시도
        - 4xx는 보통 payload 문제이므로 바로 실패 처리
        """
        url = f"{self.base_url}{endpoint}"
        last_error: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                logger.info(
                    "[INFERENCE REQUEST] model_type=%s url=%s attempt=%s payload=%s",
                    model_type,
                    url,
                    attempt + 1,
                    payload,
                )

                response = requests.post(
                    url,
                    json=payload,
                    timeout=self.timeout_sec,
                )

                # 4xx / 5xx 처리
                if 400 <= response.status_code < 500:
                    logger.error(
                        "[INFERENCE CLIENT ERROR] model_type=%s status=%s body=%s payload=%s",
                        model_type,
                        response.status_code,
                        response.text,
                        payload,
                    )
                    response.raise_for_status()

                if response.status_code >= 500:
                    logger.warning(
                        "[INFERENCE SERVER ERROR] model_type=%s status=%s body=%s attempt=%s/%s",
                        model_type,
                        response.status_code,
                        response.text,
                        attempt + 1,
                        self.max_retries + 1,
                    )
                    response.raise_for_status()

                result = response.json()

                logger.info(
                    "[INFERENCE RESPONSE] model_type=%s result=%s",
                    model_type,
                    result,
                )
                return result

            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = exc
                logger.warning(
                    "[INFERENCE NETWORK ERROR] model_type=%s url=%s attempt=%s/%s error=%s",
                    model_type,
                    url,
                    attempt + 1,
                    self.max_retries + 1,
                    repr(exc),
                )

            except requests.HTTPError as exc:
                last_error = exc
                status_code = exc.response.status_code if exc.response is not None else None

                # 4xx는 payload 문제일 가능성이 커서 재시도하지 않음
                if status_code is not None and 400 <= status_code < 500:
                    logger.error(
                        "[INFERENCE NON-RETRYABLE ERROR] model_type=%s status=%s payload=%s",
                        model_type,
                        status_code,
                        payload,
                    )
                    raise

                logger.warning(
                    "[INFERENCE RETRYABLE HTTP ERROR] model_type=%s attempt=%s/%s error=%s",
                    model_type,
                    attempt + 1,
                    self.max_retries + 1,
                    repr(exc),
                )

            except ValueError as exc:
                # response.json() 파싱 실패 등
                last_error = exc
                logger.error(
                    "[INFERENCE RESPONSE PARSE ERROR] model_type=%s url=%s error=%s",
                    model_type,
                    url,
                    repr(exc),
                )

            # 마지막 시도 아니면 잠깐 대기 후 재시도
            if attempt < self.max_retries:
                time.sleep(self.retry_backoff_sec)

        raise RuntimeError(
            f"Inference request failed after retries. model_type={model_type}, "
            f"url={url}, last_error={repr(last_error)}"
        )