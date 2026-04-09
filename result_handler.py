from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from state_store import StateStore


class ResultHandler:
    def __init__(
        self,
        state_store: StateStore,
        fe_block_ttl_sec: int = 600,
        risk_user_api_url: Optional[str] = None,
        risk_user_api_timeout_sec: float = 3.0,
    ) -> None:
        self.state_store = state_store
        self.fe_block_ttl_sec = fe_block_ttl_sec
        self.risk_user_api_url = risk_user_api_url.rstrip("/") if risk_user_api_url else None
        self.risk_user_api_timeout_sec = risk_user_api_timeout_sec
        self.http = requests.Session()

    def handle_fe_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        label = str(result.get("label", "")).lower()
        x_session_ticket = result.get("X-Session-Ticket")

        action = {
            "model_type": "fe",
            "label": label,
            "blocked": False,
        }

        if label == "bot" and x_session_ticket:
            self.state_store.set_blocked_ticket(
                x_session_ticket=x_session_ticket,
                ttl_sec=self.fe_block_ttl_sec,
            )
            action["blocked"] = True
            action["block_key"] = f"blocked_ticket:{x_session_ticket}"

        return action

    def handle_be_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        label = str(result.get("label", "")).lower()
        x_user_id = result.get("X-User-Id")
        order_id = result.get("orderId")
        bot_score = result.get("bot_score")
        model_name = result.get("model_name")

        action = {
            "model_type": "be",
            "label": label,
            "risk_count": 0,
            "policy_action": "none",
            "risk_api_called": False,
            "risk_api_status": None,
        }

        if label != "bot":
            return action

        if order_id:
            self.state_store.set_risk_order(order_id, result)

        if not x_user_id:
            action["policy_action"] = "store_order_only"
        else:
            count = self.state_store.incr_be_bot_count(x_user_id)
            action["risk_count"] = count

            if count == 1:
                action["policy_action"] = "store_and_review"
            elif count == 2:
                action["policy_action"] = "raise_risk_and_monitor"
            elif count == 3:
                action["policy_action"] = "extra_auth_or_stricter_threshold"
            elif 4 <= count <= 5:
                action["policy_action"] = "temporary_restriction"
            else:
                action["policy_action"] = "consider_permanent_ban"

        if self.risk_user_api_url:
            payload = {
                "X-User-Id": x_user_id,
                "orderId": order_id,
                "label": label,
                "bot_score": bot_score,
                "model_type": "be",
                "model_name": model_name,
                "policy_action": action["policy_action"],
                "risk_count": action["risk_count"],
            }

            try:
                resp = self.http.post(
                    self.risk_user_api_url,
                    json=payload,
                    timeout=self.risk_user_api_timeout_sec,
                )
                action["risk_api_called"] = True
                action["risk_api_status"] = resp.status_code
            except Exception as e:
                action["risk_api_called"] = True
                action["risk_api_status"] = f"error:{e}"

        return action