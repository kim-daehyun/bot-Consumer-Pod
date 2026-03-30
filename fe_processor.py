from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Optional

import redis


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
    Redis를 활용해 FE(client_telemetry_log) raw를 세션별로 누적하고,
    stage 종료 시 FE feature 3개를 계산한다.

    계산 feature:
    - duration_ms
    - mouse_activity_rate
    - mouse_teleport_rate
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        redis_prefix: str = "fe_state",
        state_ttl_sec: int = 1800,
        teleport_norm_dist_threshold: float = 0.12,
        teleport_norm_speed_threshold: float = 0.006,
        teleport_dt_threshold_ms: int = 20,
    ) -> None:
        self.redis_client = redis_client
        self.redis_prefix = redis_prefix
        self.state_ttl_sec = state_ttl_sec

        # 공유한 판정 기준 반영
        self.teleport_norm_dist_threshold = teleport_norm_dist_threshold
        self.teleport_norm_speed_threshold = teleport_norm_speed_threshold
        self.teleport_dt_threshold_ms = teleport_dt_threshold_ms

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------
    def process(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        FE raw 1건을 처리한다.

        동작:
        1) Redis에 세션 상태 누적
        2) 종료 조건이 아니면 None 반환
        3) 종료 조건이면 feature 계산 후 payload 반환
        """
        session_id = _safe_str(raw.get("session_id"))
        if not session_id:
            raise ValueError("FE raw must include session_id")

        event_type = _safe_str(raw.get("event_type"))
        if not event_type:
            raise ValueError("FE raw must include event_type")

        state = self._load_state(session_id)

        # 공통 메타 업데이트
        self._update_common_state(state, raw)

        # mousemove 이벤트 누적
        self._merge_mousemove_events(state, raw)

        # count 직접 들어오면 반영
        self._merge_counts(state, raw)

        self._save_state(session_id, state)

        if not self._is_finalize_event(raw, state):
            return None

        feature_payload = self._build_feature_payload(session_id, state)

        # feature 계산 끝났으면 state 제거
        self._delete_state(session_id)
        return feature_payload

    # ------------------------------------------------------------------
    # redis io
    # ------------------------------------------------------------------
    def _redis_key(self, session_id: str) -> str:
        return f"{self.redis_prefix}:{session_id}"

    def _load_state(self, session_id: str) -> Dict[str, Any]:
        key = self._redis_key(session_id)
        raw_value = self.redis_client.get(key)
        if raw_value is None:
            return {
                "session_id": session_id,
                "user_id": None,
                "event_type": None,
                "page_enter_ts": None,
                "page_leave_ts": None,
                "mousemove_count": 0,
                "mousemove_teleport_count": 0,
                "viewport_width": None,
                "viewport_height": None,
                "mousemove_events": [],
            }

        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8")
        return json.loads(raw_value)

    def _save_state(self, session_id: str, state: Dict[str, Any]) -> None:
        key = self._redis_key(session_id)
        self.redis_client.setex(
            key,
            self.state_ttl_sec,
            json.dumps(state, ensure_ascii=False),
        )

    def _delete_state(self, session_id: str) -> None:
        key = self._redis_key(session_id)
        self.redis_client.delete(key)

    # ------------------------------------------------------------------
    # state update
    # ------------------------------------------------------------------
    def _update_common_state(self, state: Dict[str, Any], raw: Dict[str, Any]) -> None:
        user_id = _safe_str(raw.get("user_id"))
        event_type = _safe_str(raw.get("event_type"))

        page_enter_ts = _safe_int(raw.get("page_enter_ts"))
        page_leave_ts = _safe_int(raw.get("page_leave_ts"))
        viewport_width = _safe_int(raw.get("viewport_width"))
        viewport_height = _safe_int(raw.get("viewport_height"))

        if user_id is not None:
            state["user_id"] = user_id

        if event_type is not None:
            state["event_type"] = event_type

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

    def _merge_counts(self, state: Dict[str, Any], raw: Dict[str, Any]) -> None:
        mousemove_count = _safe_int(raw.get("mousemove_count"))
        if mousemove_count is not None and mousemove_count >= 0:
            state["mousemove_count"] = max(state.get("mousemove_count", 0), mousemove_count)

        mousemove_teleport_count = _safe_int(raw.get("mousemove_teleport_count"))
        if mousemove_teleport_count is not None and mousemove_teleport_count >= 0:
            state["mousemove_teleport_count"] = max(
                state.get("mousemove_teleport_count", 0),
                mousemove_teleport_count,
            )

    def _merge_mousemove_events(self, state: Dict[str, Any], raw: Dict[str, Any]) -> None:
        events = raw.get("mousemove_events")
        if not isinstance(events, list) or not events:
            return

        existing = state.get("mousemove_events", [])
        normalized: List[Dict[str, int]] = []

        for event in events:
            if not isinstance(event, dict):
                continue

            ts = _safe_int(event.get("timestamp") or event.get("ts"))
            x = _safe_int(event.get("x"))
            y = _safe_int(event.get("y"))

            if ts is None or x is None or y is None:
                continue

            normalized.append({"timestamp": ts, "x": x, "y": y})

        if not normalized:
            return

        merged = existing + normalized
        merged.sort(key=lambda item: item["timestamp"])

        # 중복 제거
        deduped: List[Dict[str, int]] = []
        last_key = None
        for item in merged:
            key = (item["timestamp"], item["x"], item["y"])
            if key == last_key:
                continue
            deduped.append(item)
            last_key = key

        state["mousemove_events"] = deduped

        # count가 raw로 안 들어오면 events 길이로 보정
        if state.get("mousemove_count", 0) <= 0:
            state["mousemove_count"] = len(deduped)

    # ------------------------------------------------------------------
    # finalize
    # ------------------------------------------------------------------
    def _is_finalize_event(self, raw: Dict[str, Any], state: Dict[str, Any]) -> bool:
        """
        종료 조건:
        - raw에 page_leave_ts가 들어온 경우
        - 또는 raw에 종료 플래그가 명시된 경우
        """
        if _safe_int(raw.get("page_leave_ts")) is not None:
            return True

        is_stage_end = raw.get("is_stage_end")
        if isinstance(is_stage_end, bool) and is_stage_end:
            return True

        current_event_type = _safe_str(raw.get("event_type"))
        stored_event_type = _safe_str(state.get("event_type"))

        # seatmap 구간 전용으로 쓴다면, seatmap 상태에서 leave_ts가 들어온 시점만 종료로 보는 게 가장 안전
        if current_event_type == "seatmap" and state.get("page_leave_ts") is not None:
            return True
        if stored_event_type == "seatmap" and state.get("page_leave_ts") is not None:
            return True

        return False

    def _build_feature_payload(self, session_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
        duration_ms = self._calc_duration_ms(
            page_enter_ts=_safe_int(state.get("page_enter_ts")),
            page_leave_ts=_safe_int(state.get("page_leave_ts")),
        )

        mousemove_count = _safe_int(state.get("mousemove_count")) or 0

        mouse_activity_rate = self._calc_mouse_activity_rate(
            mousemove_count=mousemove_count,
            duration_ms=duration_ms,
        )

        mouse_teleport_rate = self._calc_mouse_teleport_rate(state)

        return {
            "session_id": session_id,
            "duration_ms": float(duration_ms),
            "mouse_activity_rate": float(mouse_activity_rate),
            "mouse_teleport_rate": float(mouse_teleport_rate),
        }

    # ------------------------------------------------------------------
    # feature calc
    # ------------------------------------------------------------------
    def _calc_duration_ms(
        self,
        page_enter_ts: Optional[int],
        page_leave_ts: Optional[int],
    ) -> int:
        if page_enter_ts is None or page_leave_ts is None:
            raise ValueError("FE finalize requires both page_enter_ts and page_leave_ts")

        duration_ms = page_leave_ts - page_enter_ts
        if duration_ms <= 0:
            raise ValueError(
                f"Invalid duration_ms: page_leave_ts({page_leave_ts}) "
                f"must be greater than page_enter_ts({page_enter_ts})"
            )
        return duration_ms

    def _calc_mouse_activity_rate(self, mousemove_count: int, duration_ms: int) -> float:
        duration_sec = duration_ms / 1000.0
        if duration_sec <= 0:
            return 0.0
        return mousemove_count / duration_sec

    def _calc_mouse_teleport_rate(self, state: Dict[str, Any]) -> float:
        mousemove_count = _safe_int(state.get("mousemove_count")) or 0
        direct_teleport_count = _safe_int(state.get("mousemove_teleport_count"))

        if direct_teleport_count is not None and direct_teleport_count > 0:
            return direct_teleport_count / max(mousemove_count, 1)

        events = state.get("mousemove_events", [])
        if not isinstance(events, list) or len(events) < 2:
            return 0.0

        viewport_width = _safe_float(state.get("viewport_width"))
        viewport_height = _safe_float(state.get("viewport_height"))

        # viewport 정보가 없으면 예전 단순 거리 방식보다 0.0이 더 안전
        if viewport_width is None or viewport_height is None or viewport_width <= 0 or viewport_height <= 0:
            return 0.0

        teleport_count = 0

        for i in range(1, len(events)):
            prev = events[i - 1]
            curr = events[i]

            prev_ts = _safe_float(prev.get("timestamp"))
            curr_ts = _safe_float(curr.get("timestamp"))
            prev_x = _safe_float(prev.get("x"))
            prev_y = _safe_float(prev.get("y"))
            curr_x = _safe_float(curr.get("x"))
            curr_y = _safe_float(curr.get("y"))

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
                dt < self.teleport_dt_threshold_ms and norm_dist > self.teleport_norm_dist_threshold
            ) or (
                norm_speed > self.teleport_norm_speed_threshold
            ):
                teleport_count += 1

        return teleport_count / max(mousemove_count, 1)