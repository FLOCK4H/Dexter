from __future__ import annotations

import datetime as dt
import math
import time
from decimal import Decimal
from typing import Any


_MIN_REASONABLE_UNIX_SECONDS = 946684800  # 2000-01-01T00:00:00Z
_MAX_FUTURE_SKEW_SECONDS = 86400


def _coerce_numeric_timestamp(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return float(value)
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def normalize_unix_timestamp(
    value: Any,
    *,
    fallback: float | int | None = None,
    now: float | int | None = None,
) -> int:
    if now is None:
        now = time.time()
    now = float(now)
    if fallback is None:
        fallback = now
    fallback = int(float(fallback))

    numeric = _coerce_numeric_timestamp(value)
    if numeric is None:
        return fallback

    magnitude = abs(numeric)
    if magnitude >= 1_000_000_000_000_000_000:
        numeric /= 1_000_000_000
    elif magnitude >= 1_000_000_000_000_000:
        numeric /= 1_000_000
    elif magnitude >= 1_000_000_000_000:
        numeric /= 1_000

    normalized = int(numeric)
    if normalized < _MIN_REASONABLE_UNIX_SECONDS:
        return fallback
    if normalized > int(now) + _MAX_FUTURE_SKEW_SECONDS:
        return fallback
    return normalized


def safe_utc_datetime_from_timestamp(
    value: Any,
    *,
    fallback: float | int | None = None,
    now: float | int | None = None,
) -> dt.datetime:
    normalized = normalize_unix_timestamp(value, fallback=fallback, now=now)
    return dt.datetime.fromtimestamp(normalized, tz=dt.timezone.utc)


def normalize_event_payload_timestamp(
    payload: dict[str, Any] | None,
    *,
    fallback: float | int | None = None,
    now: float | int | None = None,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return payload
    if "timestamp" not in payload:
        return payload
    normalized = normalize_unix_timestamp(
        payload.get("timestamp"),
        fallback=fallback,
        now=now,
    )
    if payload.get("timestamp") == normalized:
        return payload
    updated = dict(payload)
    updated["timestamp"] = normalized
    return updated
