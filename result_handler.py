from __future__ import annotations

from typing import Any, Dict

from state_store import StateStore


class ResultHandler:
    def __init__(self, state_store: StateStore, fe_block_ttl_sec: int = 600) -> None:
        self.state_store = state_store
        self.fe_block_ttl_sec = fe_block_ttl_sec

    def handle_fe_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        label = str(result.get("label", "")).lower()
        x_session_ticket = result.get("X-Session-Ticket")
        session_id = result.get("session_id")

        action = {
            "model_type": "fe",
            "session_id": session_id,
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
        session_id = result.get("session_id")

        action = {
            "model_type": "be",
            "session_id": session_id,
            "label": label,
            "risk_count": 0,
            "policy_action": "none",
        }

        if label != "bot":
            return action

        if order_id:
            self.state_store.set_risk_order(order_id, result)

        if not x_user_id:
            action["policy_action"] = "store_order_only"
            return action

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

        return action