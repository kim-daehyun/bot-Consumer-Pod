from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import redis

from be_processor import BEProcessor
from fe_processor import FEProcessor
from inference_client import InferenceClient
from kafka_consumer import RawKafkaConsumer
from result_handler import ResultHandler
from state_store import StateStore


INFERENCE_BASE_URL = os.getenv("INFERENCE_BASE_URL", "http://localhost:8000")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "bot-consumer-pod")
TOPIC_FE = os.getenv("TOPIC_FE", "client_telemetry_log")
TOPIC_REQ = os.getenv("TOPIC_REQ", "server_request_log")
TOPIC_EVT = os.getenv("TOPIC_EVT", "domain_event_log")


class ConsumerApp:
    def __init__(self) -> None:
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=False,
        )

        self.state_store = StateStore(self.redis_client)
        self.fe_processor = FEProcessor(self.state_store)
        self.be_processor = BEProcessor(self.state_store)
        self.inference_client = InferenceClient(INFERENCE_BASE_URL)
        self.result_handler = ResultHandler(self.state_store)

    def process_message(self, log_type: str, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if log_type == TOPIC_FE:
            fe_payload = self.fe_processor.process(raw)
            if fe_payload is None:
                return None

            fe_result = self.inference_client.predict_fe(fe_payload)
            return self.result_handler.handle_fe_result(fe_result)

        if log_type == TOPIC_REQ:
            be_payload = self.be_processor.process_server_request_log(raw)
            if be_payload is None:
                return None

            be_result = self.inference_client.predict_be(be_payload)
            return self.result_handler.handle_be_result(be_result)

        if log_type == TOPIC_EVT:
            be_payload = self.be_processor.process_domain_event_log(raw)
            if be_payload is None:
                return None

            be_result = self.inference_client.predict_be(be_payload)
            return self.result_handler.handle_be_result(be_result)

        return None


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_local_once(log_type: str, json_file: str) -> None:
    app = ConsumerApp()
    raw = _load_json(json_file)
    result = app.process_message(log_type, raw)
    print(json.dumps(result, ensure_ascii=False, indent=2))


def run_kafka_forever() -> None:
    app = ConsumerApp()
    consumer = RawKafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topics=[TOPIC_FE, TOPIC_REQ, TOPIC_EVT],
        group_id=KAFKA_GROUP_ID,
    )

    for topic, raw in consumer.iter_messages():
        try:
            result = app.process_message(topic, raw)
            if result is not None:
                print(json.dumps(result, ensure_ascii=False))
        except Exception as e:
            print(f"[ERROR] topic={topic}, error={e}")


if __name__ == "__main__":
    """
    로컬 1건 검증:
      python main.py local client_telemetry_log sample_fe.json
      python main.py local server_request_log sample_req.json
      python main.py local domain_event_log sample_evt.json

    Kafka 실행:
      python main.py kafka
    """
    import sys

    if len(sys.argv) >= 2 and sys.argv[1] == "local":
        if len(sys.argv) != 4:
            print("Usage: python main.py local <log_type> <json_file>")
            raise SystemExit(1)
        run_local_once(sys.argv[2], sys.argv[3])
    else:
        run_kafka_forever()