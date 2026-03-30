from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional

from kafka import KafkaConsumer

from state_store import StateStore
from fe_processor import FEProcessor
from be_processor import BEProcessor
from inference_client import InferenceClient
from result_handler import ResultHandler


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ConsumerApp:
    """
    Kafka raw 메시지를 consume 하여
    FE/BE feature 생성 -> FastAPI inference 호출 -> 결과 처리
    전체 흐름을 담당하는 앱
    """

    def __init__(self) -> None:
        # ------------------------------------------------------------------
        # environment
        # ------------------------------------------------------------------
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.group_id = os.getenv("KAFKA_GROUP_ID", "bot-consumer-group")

        self.fe_topic = os.getenv("FE_TOPIC", "client_telemetry_log")
        self.be_request_topic = os.getenv("BE_REQUEST_TOPIC", "server_request_log")
        self.be_event_topic = os.getenv("BE_EVENT_TOPIC", "domain_event_log")

        self.inference_base_url = os.getenv("INFERENCE_BASE_URL", "http://localhost:8000")
        self.result_topic = os.getenv("RESULT_TOPIC", "bot_inference_result")

        self.cleanup_interval_sec = int(os.getenv("STATE_CLEANUP_INTERVAL_SEC", "60"))
        self.max_idle_ms = int(os.getenv("STATE_MAX_IDLE_MS", "600000"))  # 10분

        # ------------------------------------------------------------------
        # components
        # ------------------------------------------------------------------
        self.state_store = StateStore()
        self.fe_processor = FEProcessor()
        self.be_processor = BEProcessor(self.state_store)
        self.inference_client = InferenceClient(
            base_url=self.inference_base_url,
            timeout_sec=0.5,
            max_retries=1,
            retry_backoff_sec=0.2,
        )
        self.result_handler = ResultHandler(
            result_topic=self.result_topic,
            enable_log=True,
            enable_result_topic_payload=True,
        )

        self._last_cleanup_ts = time.time()

    # ------------------------------------------------------------------
    # run
    # ------------------------------------------------------------------
    def run(self) -> None:
        logger.info(
            "[BOOT] Consumer starting | bootstrap=%s group_id=%s topics=[%s, %s, %s] inference_base_url=%s",
            self.bootstrap_servers,
            self.group_id,
            self.fe_topic,
            self.be_request_topic,
            self.be_event_topic,
            self.inference_base_url,
        )

        consumer = KafkaConsumer(
            self.fe_topic,
            self.be_request_topic,
            self.be_event_topic,
            bootstrap_servers=[self.bootstrap_servers],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=self.group_id,
        )

        for msg in consumer:
            topic = msg.topic
            raw = msg.value

            logger.info("[RAW RECEIVED] topic=%s raw=%s", topic, raw)

            try:
                if topic == self.fe_topic:
                    self._handle_fe_raw(raw)

                elif topic == self.be_request_topic:
                    self._handle_be_request_raw(raw)

                elif topic == self.be_event_topic:
                    self._handle_be_event_raw(raw)

                else:
                    logger.warning("[UNKNOWN TOPIC] topic=%s raw=%s", topic, raw)

            except Exception as exc:
                logger.exception(
                    "[PROCESSING ERROR] topic=%s error=%s raw=%s",
                    topic,
                    repr(exc),
                    raw,
                )

            self._maybe_cleanup_states()

    # ------------------------------------------------------------------
    # handlers
    # ------------------------------------------------------------------
    def _handle_fe_raw(self, raw: Dict[str, Any]) -> None:
        """
        FE raw 처리 흐름
        - FE는 거의 즉시 변환형이므로 raw 1건 기준으로 feature 생성
        - feature 완성 시 /predict/fe 호출
        - 결과 처리
        """
        feature_payload = self.fe_processor.process(raw)
        logger.info("[FE FEATURE READY] payload=%s", feature_payload)

        inference_result = self.inference_client.call_fe_inference(feature_payload)
        logger.info("[FE INFERENCE DONE] result=%s", inference_result)

        result_record = self.result_handler.handle_result(
            inference_result=inference_result,
            raw_feature_payload=feature_payload,
            source_type="fe",
        )
        logger.info("[FE RESULT HANDLED] record=%s", result_record)

    def _handle_be_request_raw(self, raw: Dict[str, Any]) -> None:
        """
        BE request raw 처리 흐름
        - state_store 에 누적
        - feature 4개가 아직 안 모였으면 None
        - domain_event_log 쪽과 조인이 완료되면 /predict/be 호출
        """
        feature_payload = self.be_processor.process_server_request_log(raw)

        if feature_payload is None:
            logger.info("[BE REQUEST STORED] waiting for more BE data")
            return

        logger.info("[BE FEATURE READY FROM REQUEST] payload=%s", feature_payload)

        inference_result = self.inference_client.call_be_inference(feature_payload)
        logger.info("[BE INFERENCE DONE] result=%s", inference_result)

        result_record = self.result_handler.handle_result(
            inference_result=inference_result,
            raw_feature_payload=feature_payload,
            source_type="be",
        )
        logger.info("[BE RESULT HANDLED] record=%s", result_record)

    def _handle_be_event_raw(self, raw: Dict[str, Any]) -> None:
        """
        BE domain event raw 처리 흐름
        - request state 와 조인
        - feature 4개 완성 시 /predict/be 호출
        """
        feature_payload = self.be_processor.process_domain_event_log(raw)

        if feature_payload is None:
            logger.info("[BE EVENT STORED] waiting for more BE data")
            return

        logger.info("[BE FEATURE READY FROM EVENT] payload=%s", feature_payload)

        inference_result = self.inference_client.call_be_inference(feature_payload)
        logger.info("[BE INFERENCE DONE] result=%s", inference_result)

        result_record = self.result_handler.handle_result(
            inference_result=inference_result,
            raw_feature_payload=feature_payload,
            source_type="be",
        )
        logger.info("[BE RESULT HANDLED] record=%s", result_record)

    # ------------------------------------------------------------------
    # maintenance
    # ------------------------------------------------------------------
    def _maybe_cleanup_states(self) -> None:
        now_sec = time.time()
        if now_sec - self._last_cleanup_ts < self.cleanup_interval_sec:
            return

        now_ms = int(now_sec * 1000)
        removed_fe, removed_be = self.state_store.cleanup_stale_states(
            now_ms=now_ms,
            max_idle_ms=self.max_idle_ms,
        )

        if removed_fe or removed_be:
            logger.warning(
                "[STATE CLEANUP] removed_fe=%s removed_be=%s",
                removed_fe,
                removed_be,
            )

        self._last_cleanup_ts = now_sec


if __name__ == "__main__":
    app = ConsumerApp()
    app.run()