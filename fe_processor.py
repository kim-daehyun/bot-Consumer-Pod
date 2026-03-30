from __future__ import annotations

from math import hypot
from typing import Any, Dict, List, Optional


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
    client_telemetry_log raw 1건을 받아 FE 모델 입력 feature 3개를 계산한다.

    계산 결과:
    - duration_ms
    - mouse_activity_rate
    - mouse_teleport_rate

    전제:
    - FE raw는 이미 seatmap 구간 요약본 형태로 들어온다.
    - 따라서 stateful aggregation 없이 즉시 변환형으로 처리한다.
    """

    def __init__(self, teleport_distance_threshold: float = 200.0) -> None:
        """
        teleport_distance_threshold:
            mousemove 연속 좌표 간 거리가 이 값 이상이면 순간 점프(teleport)로 간주한다.
            단위는 화면 좌표(px) 기준.
        """
        self.teleport_distance_threshold = teleport_distance_threshold

    def process(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        raw 1건을 받아 FE inference payload를 반환한다.

        반환 예시:
        {
            "session_id": "sess_001",
            "duration_ms": 12000.0,
            "mouse_activity_rate": 24.1,
            "mouse_teleport_rate": 0.024
        }
        """
        session_id = _safe_str(raw.get("session_id"))
        if not session_id:
            raise ValueError("FE raw must include session_id")

        page_enter_ts = _safe_int(raw.get("page_enter_ts"))
        page_leave_ts = _safe_int(raw.get("page_leave_ts"))
        duration_ms = self._calc_duration_ms(page_enter_ts, page_leave_ts)

        mousemove_count = self._extract_mousemove_count(raw)
        mouse_activity_rate = self._calc_mouse_activity_rate(
            mousemove_count=mousemove_count,
            duration_ms=duration_ms,
        )

        mousemove_events = raw.get("mousemove_events")
        mouse_teleport_rate = self._calc_mouse_teleport_rate(
            mousemove_events=mousemove_events,
            mousemove_count=mousemove_count,
        )

        return {
            "session_id": session_id,
            "duration_ms": float(duration_ms),
            "mouse_activity_rate": float(mouse_activity_rate),
            "mouse_teleport_rate": float(mouse_teleport_rate),
        }

    def _calc_duration_ms(
        self,
        page_enter_ts: Optional[int],
        page_leave_ts: Optional[int],
    ) -> int:
        """
        seatmap 구간 체류 시간(ms) 계산.
        """
        if page_enter_ts is None or page_leave_ts is None:
            raise ValueError("FE raw must include page_enter_ts and page_leave_ts")

        duration_ms = page_leave_ts - page_enter_ts
        if duration_ms <= 0:
            raise ValueError(
                f"Invalid FE duration_ms: page_leave_ts({page_leave_ts}) "
                f"must be greater than page_enter_ts({page_enter_ts})"
            )
        return duration_ms

    def _extract_mousemove_count(self, raw: Dict[str, Any]) -> int:
        """
        mousemove_count 추출.
        우선순위:
        1) raw['mousemove_count']
        2) len(raw['mousemove_events'])
        """
        mousemove_count = _safe_int(raw.get("mousemove_count"))
        if mousemove_count is not None and mousemove_count >= 0:
            return mousemove_count

        mousemove_events = raw.get("mousemove_events")
        if isinstance(mousemove_events, list):
            return len(mousemove_events)

        return 0

    def _calc_mouse_activity_rate(
        self,
        mousemove_count: int,
        duration_ms: int,
    ) -> float:
        """
        초당 mousemove 발생 빈도.
        """
        duration_sec = duration_ms / 1000.0
        if duration_sec <= 0:
            return 0.0
        return mousemove_count / duration_sec

    def _calc_mouse_teleport_rate(
        self,
        mousemove_events: Any,
        mousemove_count: int,
    ) -> float:
        """
        순간 점프 비율 계산.

        정의:
        - mousemove 연속 좌표 간 거리(px)가 threshold 이상이면 teleport 1회로 간주
        - teleport_rate = teleport_count / max(mousemove_count, 1)

        주의:
        - mousemove_events가 없으면 0.0 반환
        - raw에 mousemove_teleport_count가 직접 들어오면 그 값을 우선 사용
        """
        # raw에 이미 요약 count가 있으면 우선 사용
        if isinstance(mousemove_events, dict):
            # 예기치 않은 포맷 방어
            return 0.0

        # 혹시 raw에 이미 계산된 teleport count가 포함될 수 있는 경우 대비
        # main process()에서 raw 전체를 넘기지 않으므로 외부 raw 접근은 여기서 못 함
        # 필요하면 추후 process()에서 직접 분기 가능

        if not isinstance(mousemove_events, list) or len(mousemove_events) < 2:
            return 0.0

        teleport_count = 0
        prev_x: Optional[float] = None
        prev_y: Optional[float] = None

        for event in mousemove_events:
            if not isinstance(event, dict):
                continue

            x = _safe_float(event.get("x"))
            y = _safe_float(event.get("y"))

            if x is None or y is None:
                continue

            if prev_x is not None and prev_y is not None:
                dist = hypot(x - prev_x, y - prev_y)
                if dist >= self.teleport_distance_threshold:
                    teleport_count += 1

            prev_x = x
            prev_y = y

        return teleport_count / max(mousemove_count, 1)