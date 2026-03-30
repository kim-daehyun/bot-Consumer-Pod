from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


class ResultHandler:
    """
    FastAPI inference 응답을 받아 결과를 정리하고,
    로그 저장 또는 result topic 발행용 payload를 만든다.

    현재 단계에서는:
    - 직접 차단하지 않음
    - 결과를 기록하고 다음 계층으로 넘길 수 있는 형태로 정리만 함
    """

    def __init__(
        self,
        result_topic: Optional[str] = None,
        enable_log: bool = True,
        enable_result_topic_payload: bool = True,
    ) -> None:
        self.result_topic = result_topic
        self.enable_log = enable_log
        self.enable_result_topic_payload = enable_result_topic_payload

    # ------------------------------------------------------------------
    # public api
    # ------------------------------------------------------------------
    def handle_result(
        self,
        inference_result: Dict[str, Any],
        raw_feature_payload: Dict[str, Any],
        source_type: str,
    ) -> Dict[str, Any]:
        """
        inference 결과를 받아 표준 result record 로 정리한다.

        Parameters
        ----------
        inference_result:
            FastAPI /predict/fe 또는 /predict/be 응답 JSON
        raw_feature_payload:
            FastAPI에 보냈던 feature payload
        source_type:
            "fe" 또는 "be"

        Returns
        -------
        Dict[str, Any]
            result topic 발행 또는 후속 저장에 쓸 수 있는 표준화 결과
        """
        result_record = self._build_result_record(
            inference_result=inference_result,
            raw_feature_payload=raw_feature_payload,
            source_type=source_type,
        )

        if self.enable_log:
            self._log_result(result_record)

        return result_record

    def build_result_topic_message(
        self,
        result_record: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Kafka result topic 발행용 메시지 포맷으로 변환한다.
        현재는 result_record 그대로 쓰되, topic 메타정보만 추가한다.
        """
        if not self.enable_result_topic_payload:
            raise RuntimeError("Result topic payload generation is disabled.")

        return {
            "topic": self.result_topic,
            "value": result_record,
        }

    # ------------------------------------------------------------------
    # internal
    # ------------------------------------------------------------------
    def _build_result_record(
        self,
        inference_result: Dict[str, Any],
        raw_feature_payload: Dict[str, Any],
        source_type: str,
    ) -> Dict[str, Any]:
        session_id = inference_result.get("session_id") or raw_feature_payload.get("session_id")
        model_type = inference_result.get("model_type") or source_type
        label = inference_result.get("label")
        bot_score = inference_result.get("bot_score")
        threshold = inference_result.get("threshold")
        model_name = inference_result.get("model_name")

        result_record = {
            "ts_utc": self._utc_now_iso(),
            "session_id": session_id,
            "source_type": source_type,
            "model_type": model_type,
            "label": label,
            "bot_score": bot_score,
            "threshold": threshold,
            "model_name": model_name,
            "feature_payload": raw_feature_payload,
        }

        return result_record

    def _log_result(self, result_record: Dict[str, Any]) -> None:
        logger.info(
            "[INFERENCE RESULT] %s",
            json.dumps(result_record, ensure_ascii=False),
        )

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()