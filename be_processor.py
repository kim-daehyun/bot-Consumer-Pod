from __future__ import annotations

import math
import re
from typing import Any, Dict, List, Optional

from state_store import BEState, StateStore


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


class BEProcessor:
    """
    bot-detection/scripts/preprocess_be.py 기준으로
    BE feature 4개를 만든다.

    feature:
      - ts_payment_ready
      - ts_whole_session
      - req_interval_cv_pre_hold
      - req_interval_cv_hold_gap

    식별자:
      - session_id
      - X-User-Id
      - orderId
    """

    HOLD_SEAT_PATTERN = re.compile(r"^/api/ticketing/[^/]+/hold/seat$")
    LOGIN_PATH = "/api/auth/login"
    PAY_CONFIRM_PATH = "/api/payments/confirm"
    PAY_FAIL_PATH = "/api/payments/fail"

    def __init__(self, state_store: StateStore) -> None:
        self.state_store = state_store

    def process_server_request_log(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        state = self.state_store.get_or_create_be_state(raw)
        self._update_request_state(state, raw)
        return self._build_feature_if_ready(state)

    def process_domain_event_log(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        state = self.state_store.get_or_create_be_state(raw)
        self._update_event_state(state, raw)
        return self._build_feature_if_ready(state)

    def _update_request_state(self, state: BEState, raw: Dict[str, Any]) -> None:
        ts = _safe_int(raw.get("tsServer"))
        path = _safe_str(raw.get("path"))
        if ts is None or not path:
            return

        state.request_records.append(
            {
                "tsServer": ts,
                "path": path,
                "requestBody": raw.get("requestBody", {}),
                "queryParams": raw.get("queryParams", {}),
            }
        )

        if path.endswith("/payment-ready") and state.ts_payment_ready_start is None:
            state.ts_payment_ready_start = ts

    def _update_event_state(self, state: BEState, raw: Dict[str, Any]) -> None:
        ts = (
            _safe_int(raw.get("tsServer"))
            or _safe_int(raw.get("timestamp"))
            or _safe_int(raw.get("ts_approvedAt"))
            or _safe_int(raw.get("ts_approvedAt_or_ts_fail"))
        )
        if ts is None:
            return

        path = _safe_str(raw.get("path"))
        event_name = _safe_str(raw.get("event_name"))
        approved_at = (
            _safe_int(raw.get("approvedAt"))
            or _safe_int(raw.get("ts_approvedAt"))
            or _safe_int(raw.get("ts_approvedAt_or_ts_fail"))
        )

        if path and path.endswith("/payment-ready") and state.ts_payment_ready_start is None:
            state.ts_payment_ready_start = ts

        if approved_at is not None:
            state.ts_terminal = approved_at
            return

        if path == self.PAY_FAIL_PATH:
            state.ts_terminal = ts
            return

        if event_name in {"payment_success", "payment_fail"} and state.ts_terminal is None:
            state.ts_terminal = ts

    def _build_feature_if_ready(self, state: BEState) -> Optional[Dict[str, Any]]:
        if not state.request_records:
            return None

        ts_payment_ready = self._calc_ts_payment_ready(state)
        ts_whole_session = self._calc_ts_whole_session(state)
        req_interval_cv_pre_hold = self._calc_req_interval_cv_pre_hold(state)
        req_interval_cv_hold_gap = self._calc_req_interval_cv_hold_gap(state)

        if ts_payment_ready is None:
            return None
        if ts_whole_session is None:
            return None
        if not state.session_id or not state.x_user_id or not state.order_id:
            return None

        payload = {
            "session_id": state.session_id,
            "X-User-Id": state.x_user_id,
            "orderId": state.order_id,
            "ts_payment_ready": float(ts_payment_ready),
            "ts_whole_session": float(ts_whole_session),
            "req_interval_cv_pre_hold": float(req_interval_cv_pre_hold),
            "req_interval_cv_hold_gap": float(req_interval_cv_hold_gap),
        }

        self.state_store.pop_be_state(state.join_key)
        return payload

    def _calc_ts_payment_ready(self, state: BEState) -> Optional[float]:
        if state.ts_payment_ready_start is None or state.ts_terminal is None:
            return None

        value = state.ts_terminal - state.ts_payment_ready_start
        if value <= 0:
            return None
        return float(value)

    def _calc_ts_whole_session(self, state: BEState) -> Optional[float]:
        login_ts = None
        confirm_ts = None

        for record in sorted(state.request_records, key=lambda r: r["tsServer"]):
            path = record["path"]
            ts = record["tsServer"]

            if login_ts is None and path == self.LOGIN_PATH:
                login_ts = ts

            if path == self.PAY_CONFIRM_PATH:
                confirm_ts = ts

        if login_ts is None or confirm_ts is None:
            return None

        value = confirm_ts - login_ts
        if value <= 0:
            return None
        return float(value)

    def _split_pre_post_hold(self, state: BEState) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        records = sorted(state.request_records, key=lambda r: r["tsServer"])

        first_hold_idx = None
        for idx, record in enumerate(records):
            if self.HOLD_SEAT_PATTERN.match(record["path"]):
                first_hold_idx = idx
                break

        if first_hold_idx is None:
            return records, []

        pre_records = records[:first_hold_idx]
        post_records = records[first_hold_idx:]
        return pre_records, post_records

    def _calc_req_interval_cv_pre_hold(self, state: BEState) -> float:
        pre_records, _ = self._split_pre_post_hold(state)
        timestamps = [record["tsServer"] for record in pre_records]
        return self._calc_cv_from_timestamps(timestamps)

    def _calc_req_interval_cv_post_hold(self, state: BEState) -> float:
        _, post_records = self._split_pre_post_hold(state)
        timestamps = [record["tsServer"] for record in post_records]
        return self._calc_cv_from_timestamps(timestamps)

    def _calc_req_interval_cv_hold_gap(self, state: BEState) -> float:
        pre_cv = self._calc_req_interval_cv_pre_hold(state)
        post_cv = self._calc_req_interval_cv_post_hold(state)
        return abs(post_cv - pre_cv)

    def _calc_cv_from_timestamps(self, timestamps: List[int]) -> float:
        if len(timestamps) < 2:
            return 0.0

        timestamps = sorted(timestamps)
        intervals: List[float] = []

        for i in range(1, len(timestamps)):
            dt = timestamps[i] - timestamps[i - 1]
            if dt > 0:
                intervals.append(float(dt))

        if not intervals:
            return 0.0

        mean_dt = sum(intervals) / len(intervals)
        if mean_dt == 0:
            return 0.0

        variance = sum((x - mean_dt) ** 2 for x in intervals) / len(intervals)
        std_dt = math.sqrt(variance)
        return std_dt / mean_dt