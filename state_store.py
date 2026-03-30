from __future__ import annotations

from dataclasses import dataclass, field, asdict
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple


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


@dataclass
class FEState:
    session_id: str
    user_id: Optional[str] = None
    event_type: Optional[str] = None

    page_enter_ts: Optional[int] = None
    page_leave_ts: Optional[int] = None

    mousemove_count: int = 0
    viewport_width: Optional[int] = None
    viewport_height: Optional[int] = None

    # 원시 mousemove event 저장이 꼭 필요할 때만 사용
    mousemove_events: List[Dict[str, Any]] = field(default_factory=list)

    created_at_ms: Optional[int] = None
    last_updated_ms: Optional[int] = None

    def touch(self, ts_ms: Optional[int]) -> None:
        if ts_ms is None:
            return
        if self.created_at_ms is None:
            self.created_at_ms = ts_ms
        self.last_updated_ms = ts_ms


@dataclass
class BEState:
    join_key: str

    uuid: Optional[str] = None
    x_user_id: Optional[str] = None
    x_session_ticket: Optional[str] = None

    show_scheduled_id: Optional[int] = None
    seat_ids: List[int] = field(default_factory=list)
    order_id: Optional[str] = None
    reservation_number: Optional[str] = None

    # server_request_log 쪽 누적 데이터
    request_timestamps: List[int] = field(default_factory=list)
    endpoint_timestamps: Dict[str, List[int]] = field(default_factory=dict)
    target_counts: Dict[str, int] = field(default_factory=dict)

    # domain_event_log 쪽 시각
    ts_payment_ready: Optional[int] = None
    ts_approved_at: Optional[int] = None
    ts_fail: Optional[int] = None

    created_at_ms: Optional[int] = None
    last_updated_ms: Optional[int] = None

    def touch(self, ts_ms: Optional[int]) -> None:
        if ts_ms is None:
            return
        if self.created_at_ms is None:
            self.created_at_ms = ts_ms
        self.last_updated_ms = ts_ms

    @property
    def terminal_ts(self) -> Optional[int]:
        # 승인 시각 우선, 없으면 fail 시각
        return self.ts_approved_at if self.ts_approved_at is not None else self.ts_fail


class StateStore:
    """
    Consumer Pod 내부에서 FE/BE raw를 잠시 저장하는 메모리 스토어.

    - FE: session_id 기준 저장
    - BE: orderId 우선, 없으면 reservationNumber, 없으면 UUID 기준 저장
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._fe_states: Dict[str, FEState] = {}
        self._be_states: Dict[str, BEState] = {}

    # ------------------------------------------------------------------
    # FE
    # ------------------------------------------------------------------
    def get_or_create_fe_state(self, payload: Dict[str, Any]) -> FEState:
        session_id = _safe_str(payload.get("session_id"))
        if not session_id:
            raise ValueError("FE payload must include session_id")

        ts_candidates = [
            _safe_int(payload.get("page_enter_ts")),
            _safe_int(payload.get("page_leave_ts")),
        ]
        ts_ms = next((x for x in ts_candidates if x is not None), None)

        with self._lock:
            state = self._fe_states.get(session_id)
            if state is None:
                state = FEState(session_id=session_id)
                self._fe_states[session_id] = state

            state.user_id = _safe_str(payload.get("user_id")) or state.user_id
            state.event_type = _safe_str(payload.get("event_type")) or state.event_type

            page_enter_ts = _safe_int(payload.get("page_enter_ts"))
            if page_enter_ts is not None:
                state.page_enter_ts = page_enter_ts

            page_leave_ts = _safe_int(payload.get("page_leave_ts"))
            if page_leave_ts is not None:
                state.page_leave_ts = page_leave_ts

            mousemove_count = _safe_int(payload.get("mousemove_count"))
            if mousemove_count is not None:
                state.mousemove_count = mousemove_count

            viewport_width = _safe_int(payload.get("viewport_width"))
            if viewport_width is not None:
                state.viewport_width = viewport_width

            viewport_height = _safe_int(payload.get("viewport_height"))
            if viewport_height is not None:
                state.viewport_height = viewport_height

            # mousemove_events 자체를 raw에 넣어줄 수도 있고 안 줄 수도 있음
            events = payload.get("mousemove_events")
            if isinstance(events, list):
                state.mousemove_events = events

            state.touch(ts_ms)
            return state

    def get_fe_state(self, session_id: str) -> Optional[FEState]:
        with self._lock:
            return self._fe_states.get(session_id)

    def pop_fe_state(self, session_id: str) -> Optional[FEState]:
        with self._lock:
            return self._fe_states.pop(session_id, None)

    # ------------------------------------------------------------------
    # BE
    # ------------------------------------------------------------------
    def make_be_join_key(self, payload: Dict[str, Any]) -> str:
        order_id = _safe_str(payload.get("orderId") or payload.get("order_id"))
        reservation_number = _safe_str(
            payload.get("reservationNumber") or payload.get("reservation_number")
        )
        uuid = _safe_str(payload.get("UUID") or payload.get("uuid"))

        if order_id:
            return f"order:{order_id}"
        if reservation_number:
            return f"reservation:{reservation_number}"
        if uuid:
            return f"uuid:{uuid}"

        raise ValueError(
            "BE payload must include one of orderId/order_id, "
            "reservationNumber/reservation_number, UUID/uuid"
        )

    def get_or_create_be_state(self, payload: Dict[str, Any]) -> BEState:
        join_key = self.make_be_join_key(payload)

        ts_candidates = [
            _safe_int(payload.get("ts_server")),
            _safe_int(payload.get("ts_payment_ready")),
            _safe_int(payload.get("ts_approvedAt")),
            _safe_int(payload.get("ts_fail")),
        ]
        ts_ms = next((x for x in ts_candidates if x is not None), None)

        with self._lock:
            state = self._be_states.get(join_key)
            if state is None:
                state = BEState(join_key=join_key)
                self._be_states[join_key] = state

            state.uuid = _safe_str(payload.get("UUID") or payload.get("uuid")) or state.uuid
            state.x_user_id = _safe_str(payload.get("X-User-ID") or payload.get("x_user_id")) or state.x_user_id
            state.x_session_ticket = _safe_str(
                payload.get("X-Session-Ticket") or payload.get("x_session_ticket")
            ) or state.x_session_ticket

            show_scheduled_id = _safe_int(
                payload.get("showScheduleId") or payload.get("show_scheduled_id")
            )
            if show_scheduled_id is not None:
                state.show_scheduled_id = show_scheduled_id

            state.order_id = _safe_str(payload.get("orderId") or payload.get("order_id")) or state.order_id
            state.reservation_number = _safe_str(
                payload.get("reservationNumber") or payload.get("reservation_number")
            ) or state.reservation_number

            # seatIds
            seat_ids = payload.get("seatIds") or payload.get("seat_ids")
            if isinstance(seat_ids, list):
                parsed = []
                for seat_id in seat_ids:
                    value = _safe_int(seat_id)
                    if value is not None:
                        parsed.append(value)
                if parsed:
                    state.seat_ids = parsed

            # server_request_log 누적
            ts_server = _safe_int(payload.get("ts_server"))
            if ts_server is not None:
                state.request_timestamps.append(ts_server)

                endpoint = _safe_str(payload.get("endpoint"))
                if endpoint:
                    state.endpoint_timestamps.setdefault(endpoint, []).append(ts_server)

                target_key = self._extract_target_key(payload)
                if target_key:
                    state.target_counts[target_key] = state.target_counts.get(target_key, 0) + 1

            # domain_event_log 누적
            ts_payment_ready = _safe_int(payload.get("ts_payment_ready"))
            if ts_payment_ready is not None:
                state.ts_payment_ready = ts_payment_ready

            ts_approved_at = _safe_int(payload.get("ts_approvedAt"))
            if ts_approved_at is not None:
                state.ts_approved_at = ts_approved_at

            ts_fail = _safe_int(payload.get("ts_fail"))
            if ts_fail is not None:
                state.ts_fail = ts_fail

            state.touch(ts_ms)
            return state

    def get_be_state(self, payload_or_join_key: Any) -> Optional[BEState]:
        if isinstance(payload_or_join_key, dict):
            join_key = self.make_be_join_key(payload_or_join_key)
        else:
            join_key = str(payload_or_join_key)

        with self._lock:
            return self._be_states.get(join_key)

    def pop_be_state(self, payload_or_join_key: Any) -> Optional[BEState]:
        if isinstance(payload_or_join_key, dict):
            join_key = self.make_be_join_key(payload_or_join_key)
        else:
            join_key = str(payload_or_join_key)

        with self._lock:
            return self._be_states.pop(join_key, None)

    # ------------------------------------------------------------------
    # 공통
    # ------------------------------------------------------------------
    def cleanup_stale_states(self, now_ms: int, max_idle_ms: int) -> Tuple[List[str], List[str]]:
        """
        오래된 FE/BE state 제거.
        반환값:
        - removed_fe_session_ids
        - removed_be_join_keys
        """
        removed_fe: List[str] = []
        removed_be: List[str] = []

        with self._lock:
            for session_id, state in list(self._fe_states.items()):
                if state.last_updated_ms is None:
                    continue
                if now_ms - state.last_updated_ms > max_idle_ms:
                    removed_fe.append(session_id)
                    del self._fe_states[session_id]

            for join_key, state in list(self._be_states.items()):
                if state.last_updated_ms is None:
                    continue
                if now_ms - state.last_updated_ms > max_idle_ms:
                    removed_be.append(join_key)
                    del self._be_states[join_key]

        return removed_fe, removed_be

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "fe_states": {k: asdict(v) for k, v in self._fe_states.items()},
                "be_states": {k: asdict(v) for k, v in self._be_states.items()},
            }

    # ------------------------------------------------------------------
    # 내부 헬퍼
    # ------------------------------------------------------------------
    def _extract_target_key(self, payload: Dict[str, Any]) -> Optional[str]:
        """
        target_retry_count 계산을 위해 '같은 목표'를 식별하는 키를 만든다.
        우선순위:
        1) seatIds
        2) showScheduleId
        3) reservationNumber
        """
        seat_ids = payload.get("seatIds") or payload.get("seat_ids")
        if isinstance(seat_ids, list) and seat_ids:
            parsed = []
            for seat_id in seat_ids:
                value = _safe_int(seat_id)
                if value is not None:
                    parsed.append(str(value))
            if parsed:
                return f"seat:{','.join(parsed)}"

        show_scheduled_id = _safe_int(
            payload.get("showScheduleId") or payload.get("show_scheduled_id")
        )
        if show_scheduled_id is not None:
            return f"showSchedule:{show_scheduled_id}"

        reservation_number = _safe_str(
            payload.get("reservationNumber") or payload.get("reservation_number")
        )
        if reservation_number:
            return f"reservation:{reservation_number}"

        return None