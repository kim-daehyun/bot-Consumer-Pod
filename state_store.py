from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import redis


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


def _extract_path(raw: Dict[str, Any]) -> Optional[str]:
    path = raw.get("path")
    if isinstance(path, str) and path.strip():
        return path.strip()
    return None


def _extract_request_body(raw: Dict[str, Any]) -> Dict[str, Any]:
    body = raw.get("requestBody")
    return body if isinstance(body, dict) else {}


def _extract_session_key(raw: Dict[str, Any]) -> Optional[str]:
    return (
        _safe_str(raw.get("UUID"))
        or _safe_str(raw.get("uuid"))
        or _safe_str(raw.get("sessionId"))
        or _safe_str(raw.get("session_id"))
    )


def _extract_x_session_ticket(raw: Dict[str, Any]) -> Optional[str]:
    return (
        _safe_str(raw.get("X-Session-Ticket"))
        or _safe_str(raw.get("x_session_ticket"))
        or _safe_str(raw.get("sessionTicket"))
    )


def _extract_show_schedule_id(raw: Dict[str, Any]) -> Optional[int]:
    direct = (
        _safe_int(raw.get("showScheduleId"))
        or _safe_int(raw.get("show_schedule_id"))
        or _safe_int(raw.get("showId"))
    )
    if direct is not None:
        return direct

    path = _extract_path(raw)
    if not path:
        return None

    match = re.search(r"/api/ticketing/(\d+)", path)
    if match:
        return int(match.group(1))
    return None


def _extract_x_user_id(raw: Dict[str, Any]) -> Optional[str]:
    return (
        _safe_str(raw.get("X-User-Id"))
        or _safe_str(raw.get("X-User-ID"))
        or _safe_str(raw.get("x_user_id"))
        or _safe_str(raw.get("userId"))
    )


def _extract_order_id(raw: Dict[str, Any]) -> Optional[str]:
    body = _extract_request_body(raw)
    query = raw.get("queryParams") if isinstance(raw.get("queryParams"), dict) else {}

    order_id = (
        _safe_str(raw.get("orderId"))
        or _safe_str(body.get("orderId"))
        or _safe_str(query.get("orderId"))
    )
    if order_id:
        return order_id

    path = _extract_path(raw)
    if not path:
        return None

    match = re.search(r"/api/payments/([^/]+)(?:/cancel)?$", path)
    if match:
        candidate = match.group(1)
        if candidate not in {"confirm", "fail"}:
            return candidate

    return None


def _extract_reservation_number(raw: Dict[str, Any]) -> Optional[str]:
    body = _extract_request_body(raw)

    reservation_number = (
        _safe_str(raw.get("reservationNumber"))
        or _safe_str(body.get("reservationNumber"))
    )
    if reservation_number:
        return reservation_number

    path = _extract_path(raw)
    if not path:
        return None

    match = re.search(r"/api/bookings/([^/]+)/payment-ready$", path)
    if match:
        return match.group(1)

    return None


@dataclass
class BEState:
    join_key: str
    session_key: Optional[str] = None
    x_user_id: Optional[str] = None
    order_id: Optional[str] = None
    reservation_number: Optional[str] = None

    request_records: List[Dict[str, Any]] = field(default_factory=list)

    ts_payment_ready_start: Optional[int] = None
    ts_terminal: Optional[int] = None


class StateStore:
    def __init__(
        self,
        redis_client: redis.Redis,
        fe_state_prefix: str = "fe_state",
        blocked_ticket_prefix: str = "blocked_ticket",
        be_risk_user_prefix: str = "be_risk_user",
        be_risk_order_prefix: str = "be_risk_order",
        fe_state_ttl_sec: int = 1800,
        blocked_ticket_ttl_sec: int = 600,
        be_order_ttl_sec: int = 30 * 24 * 60 * 60,
    ) -> None:
        self.redis_client = redis_client

        self.fe_state_prefix = fe_state_prefix
        self.blocked_ticket_prefix = blocked_ticket_prefix
        self.be_risk_user_prefix = be_risk_user_prefix
        self.be_risk_order_prefix = be_risk_order_prefix

        self.fe_state_ttl_sec = fe_state_ttl_sec
        self.blocked_ticket_ttl_sec = blocked_ticket_ttl_sec
        self.be_order_ttl_sec = be_order_ttl_sec

        self._be_states: Dict[str, BEState] = {}

    # ------------------------------------------------------------------
    # FE redis state
    # ------------------------------------------------------------------
    def _fe_state_key(self, session_key: str) -> str:
        return f"{self.fe_state_prefix}:{session_key}"

    def load_fe_state(self, session_key: str) -> Dict[str, Any]:
        raw_value = self.redis_client.get(self._fe_state_key(session_key))
        if raw_value is None:
            return {
                "session_key": session_key,
                "X-Session-Ticket": None,
                "showScheduleId": None,
                "page_stage": None,
                "page_enter_ts": None,
                "page_leave_ts": None,
                "mousemove_count": 0,
                "viewport_width": None,
                "viewport_height": None,
                "mousemove": [],
            }

        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8")
        return json.loads(raw_value)

    def save_fe_state(self, session_key: str, state: Dict[str, Any]) -> None:
        self.redis_client.setex(
            self._fe_state_key(session_key),
            self.fe_state_ttl_sec,
            json.dumps(state, ensure_ascii=False),
        )

    def delete_fe_state(self, session_key: str) -> None:
        self.redis_client.delete(self._fe_state_key(session_key))

    # ------------------------------------------------------------------
    # FE blocked ticket
    # ------------------------------------------------------------------
    def _blocked_ticket_key(self, x_session_ticket: str) -> str:
        return f"{self.blocked_ticket_prefix}:{x_session_ticket}"

    def set_blocked_ticket(self, x_session_ticket: str, ttl_sec: Optional[int] = None) -> None:
        ttl = ttl_sec if ttl_sec is not None else self.blocked_ticket_ttl_sec
        self.redis_client.setex(self._blocked_ticket_key(x_session_ticket), ttl, "1")

    def is_blocked_ticket(self, x_session_ticket: str) -> bool:
        return bool(self.redis_client.exists(self._blocked_ticket_key(x_session_ticket)))

    # ------------------------------------------------------------------
    # BE risk info
    # ------------------------------------------------------------------
    def _be_risk_user_key(self, x_user_id: str) -> str:
        return f"{self.be_risk_user_prefix}:{x_user_id}"

    def _be_risk_order_key(self, order_id: str) -> str:
        return f"{self.be_risk_order_prefix}:{order_id}"

    def incr_be_bot_count(self, x_user_id: str) -> int:
        return int(self.redis_client.incr(self._be_risk_user_key(x_user_id)))

    def set_risk_order(self, order_id: str, payload: Dict[str, Any]) -> None:
        self.redis_client.setex(
            self._be_risk_order_key(order_id),
            self.be_order_ttl_sec,
            json.dumps(payload, ensure_ascii=False),
        )

    # ------------------------------------------------------------------
    # BE join state
    # ------------------------------------------------------------------
    def _make_be_join_key(self, raw: Dict[str, Any]) -> str:
        order_id = _extract_order_id(raw)
        reservation_number = _extract_reservation_number(raw)
        session_key = _extract_session_key(raw)

        if order_id:
            return f"order:{order_id}"
        if reservation_number:
            return f"reservation:{reservation_number}"
        if session_key:
            return f"session:{session_key}"

        raise ValueError("BE raw must include one of orderId / reservationNumber / session key")

    def get_or_create_be_state(self, raw: Dict[str, Any]) -> BEState:
        join_key = self._make_be_join_key(raw)
        state = self._be_states.get(join_key)

        if state is None:
            state = BEState(join_key=join_key)
            self._be_states[join_key] = state

        session_key = _extract_session_key(raw)
        x_user_id = _extract_x_user_id(raw)
        order_id = _extract_order_id(raw)
        reservation_number = _extract_reservation_number(raw)

        if session_key:
            state.session_key = session_key
        if x_user_id:
            state.x_user_id = x_user_id
        if order_id:
            state.order_id = order_id
        if reservation_number:
            state.reservation_number = reservation_number

        return state

    def pop_be_state(self, join_key: str) -> None:
        self._be_states.pop(join_key, None)

    # ------------------------------------------------------------------
    # helper exposure
    # ------------------------------------------------------------------
    @staticmethod
    def extract_session_key(raw: Dict[str, Any]) -> Optional[str]:
        return _extract_session_key(raw)

    @staticmethod
    def extract_x_session_ticket(raw: Dict[str, Any]) -> Optional[str]:
        return _extract_x_session_ticket(raw)

    @staticmethod
    def extract_show_schedule_id(raw: Dict[str, Any]) -> Optional[int]:
        return _extract_show_schedule_id(raw)

    @staticmethod
    def extract_x_user_id(raw: Dict[str, Any]) -> Optional[str]:
        return _extract_x_user_id(raw)

    @staticmethod
    def extract_order_id(raw: Dict[str, Any]) -> Optional[str]:
        return _extract_order_id(raw)

    @staticmethod
    def extract_reservation_number(raw: Dict[str, Any]) -> Optional[str]:
        return _extract_reservation_number(raw)

    @staticmethod
    def extract_path(raw: Dict[str, Any]) -> Optional[str]:
        return _extract_path(raw)

    @staticmethod
    def extract_request_body(raw: Dict[str, Any]) -> Dict[str, Any]:
        return _extract_request_body(raw)