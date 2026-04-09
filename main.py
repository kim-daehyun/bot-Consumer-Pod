from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Dict, Optional

import redis
import requests

from be_processor import BEProcessor
from fe_processor import FEProcessor
from inference_client import InferenceClient
from kafka_consumer import RawKafkaConsumer
from result_handler import ResultHandler
from state_store import StateStore

# =========================
# Environment Variables
# =========================
RUN_MODE = os.getenv("RUN_MODE", "kafka").strip().lower()  # kafka | file
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

INFERENCE_BASE_URL = os.getenv("INFERENCE_BASE_URL", "http://localhost:8000").rstrip("/")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

TOPIC_FE = os.getenv("TOPIC_FE", "client_telemetry_log")
TOPIC_REQ = os.getenv("TOPIC_REQ", "server_request_log")
TOPIC_EVT = os.getenv("TOPIC_EVT", "domain_event_log")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "bot-consumer-local")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

RISK_USER_API_URL = os.getenv("RISK_USER_API_URL", "")
RISK_USER_API_TIMEOUT_SEC = float(os.getenv("RISK_USER_API_TIMEOUT_SEC", "3.0"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("bot-consumer")


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _check_inference_health(base_url: str, timeout_sec: float = 3.0) -> None:
    url = f"{base_url}/health"
    try:
        resp = requests.get(url, timeout=timeout_sec)
        resp.raise_for_status()
        data = resp.json()
        logger.info("Inference health check OK: %s", data)
    except Exception as e:
        raise RuntimeError(f"Inference health check failed: {url} / {e}") from e


class ConsumerApp:
    def __init__(self) -> None:
        logger.info(
            "Initializing ConsumerApp | INFERENCE_BASE_URL=%s REDIS=%s:%s/%s",
            INFERENCE_BASE_URL,
            REDIS_HOST,
            REDIS_PORT,
            REDIS_DB,
        )

        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=False,
        )

        # Redis 연결 확인
        self.redis_client.ping()
        logger.info("Redis connection OK")

        # inference health 확인
        _check_inference_health(INFERENCE_BASE_URL)

        self.state_store = StateStore(self.redis_client)
        self.fe_processor = FEProcessor(self.state_store)
        self.be_processor = BEProcessor(self.state_store)
        self.inference_client = InferenceClient(INFERENCE_BASE_URL)
        self.result_handler = ResultHandler(
            state_store=self.state_store,
            risk_user_api_url=RISK_USER_API_URL or None,
            risk_user_api_timeout_sec=RISK_USER_API_TIMEOUT_SEC,
        )

    def process_message(self, log_type: str, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        logger.info("Received message | topic=%s", log_type)

        if log_type == TOPIC_FE:
            fe_payload = self.fe_processor.process(raw)
            if fe_payload is None:
                logger.info("FE payload not ready yet")
                return None

            logger.info("FE payload built: %s", fe_payload)
            fe_result = self.inference_client.predict_fe(fe_payload)
            logger.info("FE inference result: %s", fe_result)
            handled = self.result_handler.handle_fe_result(fe_result)
            logger.info("FE handled result: %s", handled)
            return handled

        if log_type == TOPIC_REQ:
            be_payload = self.be_processor.process_server_request_log(raw)
            if be_payload is None:
                logger.info("BE payload not ready yet (server_request_log)")
                return None

            logger.info("BE payload built from request log: %s", be_payload)
            be_result = self.inference_client.predict_be(be_payload)
            logger.info("BE inference result: %s", be_result)
            handled = self.result_handler.handle_be_result(be_result)
            logger.info("BE handled result: %s", handled)
            return handled

        if log_type == TOPIC_EVT:
            be_payload = self.be_processor.process_domain_event_log(raw)
            if be_payload is None:
                logger.info("BE payload not ready yet (domain_event_log)")
                return None

            logger.info("BE payload built from event log: %s", be_payload)
            be_result = self.inference_client.predict_be(be_payload)
            logger.info("BE inference result: %s", be_result)
            handled = self.result_handler.handle_be_result(be_result)
            logger.info("BE handled result: %s", handled)
            return handled

        logger.warning("Unknown topic received: %s", log_type)
        return None


def run_file_mode() -> int:
    """
    Example
    python main.py client_telemetry_log sample_fe.json
    python main.py server_request_log sample_req.json
    python main.py domain_event_log sample_evt.json
    """
    if len(sys.argv) != 3:
        print("Usage: python main.py <log_type> <json_file>")
        return 1

    log_type = sys.argv[1]
    json_file = sys.argv[2]

    logger.info("Running FILE mode | topic=%s file=%s", log_type, json_file)

    app = ConsumerApp()
    raw = _load_json(json_file)
    result = app.process_message(log_type, raw)

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def run_kafka_mode() -> int:
    logger.info(
        "Running KAFKA mode | bootstrap=%s group_id=%s auto_offset_reset=%s topics=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_GROUP_ID,
        KAFKA_AUTO_OFFSET_RESET,
        [TOPIC_FE, TOPIC_REQ, TOPIC_EVT],
    )

    app = ConsumerApp()

    consumer = RawKafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topics=[TOPIC_FE, TOPIC_REQ, TOPIC_EVT],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
    )

    logger.info("Kafka consumer started")

    for topic, raw in consumer.iter_messages():
        try:
            result = app.process_message(topic, raw)
            if result is not None:
                logger.info("Final handled result: %s", result)
        except Exception:
            logger.exception("Failed to process message | topic=%s raw=%s", topic, raw)

    return 0


if __name__ == "__main__":
    if RUN_MODE == "file":
        raise SystemExit(run_file_mode())
    if RUN_MODE == "kafka":
        raise SystemExit(run_kafka_mode())

    print("RUN_MODE must be one of: file, kafka")
    raise SystemExit(1)