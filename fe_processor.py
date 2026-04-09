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
    bot-detection/scripts/preprocess_fe.py 기준으로
    FE feature 3개를 동일하게 만든다.

    feature:
      - duration_ms
      - mouse_teleport_rate
      - mousemove_count

    식별자:
      - session_id
      - X-Session-Ticket
      - showScheduleId
    """

    def __init__(
        self,
        state_store: StateStore,
        teleport_dt_ms_threshold: int = 20,
        teleport_norm_dist_threshold: float = 0.12,
        teleport_norm_speed_threshold: float = 0.006,
    ) -> None:
        self.state_store = state_store
        self.teleport_dt_ms_threshold = teleport_dt_ms_threshold
        self.teleport_norm_dist_threshold = teleport_norm_dist_threshold
        self.teleport_norm_speed_threshold = teleport_norm_speed_threshold

    def process(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        session_id = self.state_store.extract_session_id(raw)
        if not session_id:
            raise ValueError("FE raw must include session_id/UUID/sessionId")

        state = self.state_store.load_fe_state(session_id)

        self._update_common_state(state, raw)
        self._merge_mousemove(state, raw)
        self._merge_mousemove_count(state, raw)

        self.state_store.save_fe_state(session_id, state)

        if not self._is_finalize(raw, state):
            return None

        payload = self._build_feature_payload(session_id, state)
        self.state_store.delete_fe_state(session_id)
        return payload

    def _extract_request_body(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        request_body = raw.get("requestBody")
        return request_body if isinstance(request_body, dict) else raw

    def _update_common_state(self, state: Dict[str, Any], raw: Dict[str, Any]) -> None:
        body = self._extract_request_body(raw)

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

    def _merge_mousemove_count(self, state: Dict[str, Any], raw: Dict[str, Any]) -> None:
        body = self._extract_request_body(raw)
        mousemove_count = _safe_int(body.get("mousemove_count"))
        if mousemove_count is not None and mousemove_count >= 0:
            state["mousemove_count"] = max(state.get("mousemove_count", 0), mousemove_count)

    def _merge_mousemove(self, state: Dict[str, Any], raw: Dict[str, Any]) -> None:
        body = self._extract_request_body(raw)
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
        if state.get("mousemove_count", 0) <= 0:
            state["mousemove_count"] = len(deduped)

    def _is_finalize(self, raw: Dict[str, Any], state: Dict[str, Any]) -> bool:
        body = self._extract_request_body(raw)

        if _safe_int(body.get("page_leave_ts")) is not None:
            return True

        is_stage_end = raw.get("is_stage_end")
        if isinstance(is_stage_end, bool) and is_stage_end:
            return True

        return False

    def _build_feature_payload(self, session_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
        duration_ms = self._calc_duration_ms(
            page_enter_ts=_safe_int(state.get("page_enter_ts")),
            page_leave_ts=_safe_int(state.get("page_leave_ts")),
        )
        mousemove_count = _safe_int(state.get("mousemove_count")) or 0
        mouse_teleport_rate = self._calc_mouse_teleport_rate(state)

        x_session_ticket = _safe_str(state.get("X-Session-Ticket"))
        show_schedule_id = _safe_int(state.get("showScheduleId"))

        if not x_session_ticket:
            raise ValueError("FE finalize requires X-Session-Ticket")
        if show_schedule_id is None:
            raise ValueError("FE finalize requires showScheduleId")

        return {
            "session_id": session_id,
            "X-Session-Ticket": x_session_ticket,
            "showScheduleId": show_schedule_id,
            "duration_ms": float(duration_ms),
            "mouse_teleport_rate": float(round(mouse_teleport_rate, 6)),
            "mousemove_count": float(mousemove_count),
        }

    def _calc_duration_ms(self, page_enter_ts: Optional[int], page_leave_ts: Optional[int]) -> int:
        if page_enter_ts is None or page_leave_ts is None:
            raise ValueError("FE finalize requires page_enter_ts and page_leave_ts")
        duration_ms = page_leave_ts - page_enter_ts
        return max(0, int(duration_ms))

    def _calc_mouse_teleport_rate(self, state: Dict[str, Any]) -> float:
        events = state.get("mousemove", [])
        if not isinstance(events, list) or len(events) < 2:
            return 0.0

        mousemove_count = _safe_int(state.get("mousemove_count")) or 0
        viewport_width = _safe_float(state.get("viewport_width"))
        viewport_height = _safe_float(state.get("viewport_height"))

        if mousemove_count <= 0 or viewport_width is None or viewport_height is None:
            return 0.0
        if viewport_width <= 0 or viewport_height <= 0:
            return 0.0

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

            norm_dx = dx / viewport_width if viewport_width > 0 else 0.0
            norm_dy = dy / viewport_height if viewport_height > 0 else 0.0
            norm_dist = math.sqrt(norm_dx ** 2 + norm_dy ** 2)
            norm_speed = norm_dist / dt

            is_teleport = (
                (dt < self.teleport_dt_ms_threshold and norm_dist > self.teleport_norm_dist_threshold)
                or (norm_speed > self.teleport_norm_speed_threshold)
            )

            if is_teleport:
                teleport_count += 1

        return teleport_count / max(mousemove_count, 1)