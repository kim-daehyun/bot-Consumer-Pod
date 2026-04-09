from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from state_store import StateStore


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


class FEProcessor:
    """
    최신 FE 기준:
    - duration_ms
    - mousemove_teleport_count
    - mousemove_count

    식별자:
    - X-Session-Ticket
    - showScheduleId

    Redis에 session/window 상태를 누적하고,
    seatmap 종료 시 feature payload를 만든다.
    """

    def __init__(
        self,
        state_store: StateStore,
        teleport_dt_ms_threshold: int = 20,
        teleport_norm_dist_threshold: float = 0.003,
        teleport_norm_speed_threshold: float = 0.002,
    ) -> None:
        self.state_store = state_store
        self.teleport_dt_ms_threshold = teleport_dt_ms_threshold
        self.teleport_norm_dist_threshold = teleport_norm_dist_threshold
        self.teleport_norm_speed_threshold = teleport_norm_speed_threshold

    def process(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        session_key = self.state_store.extract_session_key(raw)
        if not session_key:
            raise ValueError("FE raw must include UUID/sessionId/session_id")

        body = self.state_store.extract_request_body(raw)
        state = self.state_store.load_fe_state(session_key)

        self._update_common_state(state, raw, body)
        self._merge_mousemove(state, body)
        self._merge_mousemove_count(state, body)

        self.state_store.save_fe_state(session_key, state)

        if not self._is_finalize(body):
            return None

        payload = self._build_feature_payload(state)
        self.state_store.delete_fe_state(session_key)
        return payload

    def _update_common_state(self, state: Dict[str, Any], raw: Dict[str, Any], body: Dict[str, Any]) -> None:
        x_session_ticket = self.state_store.extract_x_session_ticket(raw)
        show_schedule_id = self.state_store.extract_show_schedule_id(raw)

        page_stage = _safe_str(body.get("page_stage"))
        page_enter_ts = _safe_int(body.get("page_enter_ts"))
        page_leave_ts = _safe_int(body.get("page_leave_ts"))
        viewport_width = _safe_int(body.get("viewport_width"))
        viewport_height = _safe_int(body.get("viewport_height"))

        if x_session_ticket is not None:
            state["X-Session-Ticket"] = x_session_ticket
        if show_schedule_id is not None:
            state["showScheduleId"] = show_schedule_id
        if page_stage is not None:
            state["page_stage"] = page_stage

        if page_enter_ts is not None:
            if state["page_enter_ts"] is None:
                state["page_enter_ts"] = page_enter_ts
            else:
                state["page_enter_ts"] = min(state["page_enter_ts"], page_enter_ts)

        if page_leave_ts is not None:
            if state["page_leave_ts"] is None:
                state["page_leave_ts"] = page_leave_ts
            else:
                state["page_leave_ts"] = max(state["page_leave_ts"], page_leave_ts)

        if viewport_width is not None:
            state["viewport_width"] = viewport_width
        if viewport_height is not None:
            state["viewport_height"] = viewport_height

    def _merge_mousemove(self, state: Dict[str, Any], body: Dict[str, Any]) -> None:
        events = body.get("mousemove")
        if not isinstance(events, list) or not events:
            return

        merged = state.get("mousemove", [])

        for event in events:
            if not isinstance(event, dict):
                continue
            ts = _safe_int(event.get("timestamp"))
            x = _safe_int(event.get("x"))
            y = _safe_int(event.get("y"))
            if ts is None or x is None or y is None:
                continue
            merged.append({"timestamp": ts, "x": x, "y": y})

        merged.sort(key=lambda item: item["timestamp"])

        deduped: List[Dict[str, int]] = []
        seen = set()
        for item in merged:
            key = (item["timestamp"], item["x"], item["y"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)

        state["mousemove"] = deduped

    def _merge_mousemove_count(self, state: Dict[str, Any], body: Dict[str, Any]) -> None:
        mousemove_count = _safe_int(body.get("mousemove_count"))
        if mousemove_count is not None and mousemove_count >= 0:
            state["mousemove_count"] = max(state.get("mousemove_count", 0), mousemove_count)

        if state.get("mousemove_count", 0) <= 0:
            state["mousemove_count"] = len(state.get("mousemove", []))

    def _is_finalize(self, body: Dict[str, Any]) -> bool:
        return _safe_int(body.get("page_leave_ts")) is not None

    def _build_feature_payload(self, state: Dict[str, Any]) -> Dict[str, Any]:
        page_enter_ts = _safe_int(state.get("page_enter_ts"))
        page_leave_ts = _safe_int(state.get("page_leave_ts"))
        if page_enter_ts is None or page_leave_ts is None:
            raise ValueError("FE finalize requires page_enter_ts and page_leave_ts")

        duration_ms = page_leave_ts - page_enter_ts
        if duration_ms <= 0:
            raise ValueError("Invalid FE duration_ms")

        mousemove_count = _safe_int(state.get("mousemove_count")) or 0
        mousemove_teleport_count = self._calc_mousemove_teleport_count(state)

        x_session_ticket = _safe_str(state.get("X-Session-Ticket"))
        show_schedule_id = _safe_int(state.get("showScheduleId"))

        if not x_session_ticket:
            raise ValueError("FE finalize requires X-Session-Ticket")
        if show_schedule_id is None:
            raise ValueError("FE finalize requires showScheduleId")

        return {
            "X-Session-Ticket": x_session_ticket,
            "showScheduleId": show_schedule_id,
            "duration_ms": float(duration_ms),
            "mousemove_teleport_count": float(mousemove_teleport_count),
            "mousemove_count": float(mousemove_count),
        }

    def _calc_mousemove_teleport_count(self, state: Dict[str, Any]) -> int:
        events = state.get("mousemove", [])
        if not isinstance(events, list) or len(events) < 2:
            return 0

        viewport_width = _safe_float(state.get("viewport_width"))
        viewport_height = _safe_float(state.get("viewport_height"))
        if viewport_width is None or viewport_height is None or viewport_width <= 0 or viewport_height <= 0:
            return 0

        teleport_count = 0

        for i in range(1, len(events)):
            prev_evt = events[i - 1]
            curr_evt = events[i]

            prev_ts = _safe_float(prev_evt.get("timestamp"))
            curr_ts = _safe_float(curr_evt.get("timestamp"))
            prev_x = _safe_float(prev_evt.get("x"))
            prev_y = _safe_float(prev_evt.get("y"))
            curr_x = _safe_float(curr_evt.get("x"))
            curr_y = _safe_float(curr_evt.get("y"))

            if None in (prev_ts, curr_ts, prev_x, prev_y, curr_x, curr_y):
                continue

            dt = curr_ts - prev_ts
            if dt <= 0:
                continue

            dx = curr_x - prev_x
            dy = curr_y - prev_y

            norm_dx = dx / viewport_width
            norm_dy = dy / viewport_height
            norm_dist = math.sqrt(norm_dx ** 2 + norm_dy ** 2)
            norm_speed = norm_dist / dt

            if (
                dt < self.teleport_dt_ms_threshold and norm_dist > self.teleport_norm_dist_threshold
            ) or (
                norm_speed > self.teleport_norm_speed_threshold
            ):
                teleport_count += 1

        return teleport_count