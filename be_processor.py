from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from state_store import BEState, StateStore


class BEProcessor:
    """
    server_request_log + domain_event_log 를 조인하여
    BE 모델 입력 feature 4개를 만드는 processor.

    최종 feature:
    - endpoint_burst_max_1s
    - req_interval_cv
    - target_retry_count
    - payment_ready_to_terminal_ms
    """

    def __init__(self, state_store: StateStore) -> None:
        self.state_store = state_store

    # ------------------------------------------------------------------
    # public entrypoints
    # ------------------------------------------------------------------
    def process_server_request_log(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        server_request_log raw를 state에 반영한다.
        아직 payment event가 안 붙었으면 None 반환,
        4개 feature가 모두 완성되면 inference payload 반환.
        """
        state = self.state_store.get_or_create_be_state(raw)
        return self._build_feature_if_ready(state)

    def process_domain_event_log(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        domain_event_log raw를 state에 반영한다.
        request 쪽 데이터와 조인이 완료되면 inference payload 반환.
        """
        state = self.state_store.get_or_create_be_state(raw)
        return self._build_feature_if_ready(state)

    # ------------------------------------------------------------------
    # feature builder
    # ------------------------------------------------------------------
    def _build_feature_if_ready(self, state: BEState) -> Optional[Dict[str, Any]]:
        """
        BE feature 4개가 모두 계산 가능한 상태인지 확인하고,
        가능하면 inference payload를 만든다.
        """
        if not self._is_request_feature_ready(state):
            return None

        payment_ready_to_terminal_ms = self._calc_payment_ready_to_terminal_ms(state)
        if payment_ready_to_terminal_ms is None:
            return None

        feature_payload = {
            "session_id": self._resolve_session_id(state),
            "endpoint_burst_max_1s": float(self._calc_endpoint_burst_max_1s(state)),
            "req_interval_cv": float(self._calc_req_interval_cv(state)),
            "target_retry_count": float(self._calc_target_retry_count(state)),
            "payment_ready_to_terminal_ms": float(payment_ready_to_terminal_ms),
        }

        # 최종 feature가 완성되면 state 제거
        self.state_store.pop_be_state(state.join_key)
        return feature_payload

    def _is_request_feature_ready(self, state: BEState) -> bool:
        """
        request 기반 feature 3개를 계산할 최소 데이터가 있는지 확인
        """
        return len(state.request_timestamps) >= 1

    def _resolve_session_id(self, state: BEState) -> str:
        """
        FastAPI payload에 넣을 session_id를 결정한다.
        우선순위:
        1) uuid
        2) reservation_number
        3) order_id
        4) join_key
        """
        if state.uuid:
            return state.uuid
        if state.reservation_number:
            return state.reservation_number
        if state.order_id:
            return state.order_id
        return state.join_key

    # ------------------------------------------------------------------
    # request feature 1
    # ------------------------------------------------------------------
    def _calc_endpoint_burst_max_1s(self, state: BEState) -> float:
        """
        같은 endpoint를 1초(1000ms) 내에 최대 몇 번 호출했는지 계산.
        endpoint별 burst를 구한 뒤 최대값을 사용한다.
        """
        if not state.endpoint_timestamps:
            return 0.0

        max_burst = 0

        for _, timestamps in state.endpoint_timestamps.items():
            burst = self._calc_burst_within_1s(sorted(timestamps))
            if burst > max_burst:
                max_burst = burst

        return float(max_burst)

    def _calc_burst_within_1s(self, timestamps: List[int]) -> int:
        if not timestamps:
            return 0

        left = 0
        max_count = 0

        for right in range(len(timestamps)):
            while timestamps[right] - timestamps[left] > 1000:
                left += 1
            max_count = max(max_count, right - left + 1)

        return max_count

    # ------------------------------------------------------------------
    # request feature 2
    # ------------------------------------------------------------------
    def _calc_req_interval_cv(self, state: BEState) -> float:
        """
        요청 간격의 coefficient of variation 계산.
        요청이 2개 미만이면 간격이 없으므로 0.0 반환.
        """
        timestamps = sorted(state.request_timestamps)
        if len(timestamps) < 2:
            return 0.0

        intervals = [
            timestamps[i] - timestamps[i - 1]
            for i in range(1, len(timestamps))
        ]

        if not intervals:
            return 0.0

        mean_val = sum(intervals) / len(intervals)
        if mean_val == 0:
            return 0.0

        variance = sum((x - mean_val) ** 2 for x in intervals) / len(intervals)
        std_val = math.sqrt(variance)
        return std_val / mean_val

    # ------------------------------------------------------------------
    # request feature 3
    # ------------------------------------------------------------------
    def _calc_target_retry_count(self, state: BEState) -> float:
        """
        같은 목표를 몇 번 다시 시도했는지 계산.
        state_store 에서 이미 target_counts를 누적했다고 가정.
        예:
        - 같은 target 1번 요청 -> retry 0
        - 같은 target 3번 요청 -> retry 2
        """
        if not state.target_counts:
            return 0.0

        max_attempts = max(state.target_counts.values())
        retry_count = max(max_attempts - 1, 0)
        return float(retry_count)

    # ------------------------------------------------------------------
    # payment join feature
    # ------------------------------------------------------------------
    def _calc_payment_ready_to_terminal_ms(self, state: BEState) -> Optional[float]:
        """
        payment_ready 시점 ~ 최종 결제 종료 시점(approved/fail) 차이 계산
        """
        if state.ts_payment_ready is None:
            return None

        terminal_ts = state.terminal_ts
        if terminal_ts is None:
            return None

        diff = terminal_ts - state.ts_payment_ready
        if diff < 0:
            # 이상 데이터 방어
            return None

        return float(diff)