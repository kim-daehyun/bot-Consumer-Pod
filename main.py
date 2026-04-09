from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import redis

from be_processor import BEProcessor
from fe_processor import FEProcessor
from inference_client import InferenceClient
from result_handler import ResultHandler
from state_store import StateStore


INFERENCE_BASE_URL = os.getenv("INFERENCE_BASE_URL", "http://localhost:8000")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

TOPIC_FE = os.getenv("TOPIC_FE", "client_telemetry_log")
TOPIC_REQ = os.getenv("TOPIC_REQ", "server_request_log")
TOPIC_EVT = os.getenv("TOPIC_EVT", "domain_event_log")

RISK_USER_API_URL = os.getenv("RISK_USER_API_URL", "")
RISK_USER_API_TIMEOUT_SEC = float(os.getenv("RISK_USER_API_TIMEOUT_SEC", "3.0"))


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
        self.result_handler = ResultHandler(
            state_store=self.state_store,
            risk_user_api_url=RISK_USER_API_URL or None,
            risk_user_api_timeout_sec=RISK_USER_API_TIMEOUT_SEC,
        )

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


if __name__ == "__main__":
    """
    로컬 테스트:
      python main.py client_telemetry_log sample_fe.json
      python main.py server_request_log sample_req.json
      python main.py domain_event_log sample_evt.json
    """
    import sys

    if len(sys.argv) != 3:
        print("Usage: python main.py <log_type> <json_file>")
        raise SystemExit(1)

    log_type = sys.argv[1]
    json_file = sys.argv[2]

    app = ConsumerApp()
    raw = _load_json(json_file)
    result = app.process_message(log_type, raw)
    print(json.dumps(result, ensure_ascii=False, indent=2))