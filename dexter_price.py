from __future__ import annotations

from decimal import Decimal, InvalidOperation
import logging
import os
import threading
import time
from typing import Callable

import requests

COINGECKO_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
COINBASE_PRICE_URL = "https://api.coinbase.com/v2/prices/SOL-USD/spot"
KRAKEN_PRICE_URL = "https://api.kraken.com/0/public/Ticker?pair=SOLUSD"

DEFAULT_REQUEST_TIMEOUT_SECONDS = 4.0
DEFAULT_CACHE_TTL_SECONDS = 300.0
DEFAULT_FAILURE_LOG_COOLDOWN_SECONDS = 300.0
DEFAULT_FALLBACK_PRICE_USD = Decimal("210.11")
REQUEST_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Dexter/3.0",
}

_cache_lock = threading.Lock()
_cached_price_usd: Decimal | None = None
_cached_at_monotonic = 0.0
_last_failure_log_at_monotonic = 0.0


class SolPriceLookupError(RuntimeError):
    pass


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_decimal(name: str, default: Decimal) -> Decimal:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        return default
    if value <= 0:
        return default
    return value


def _coerce_price(value: object) -> Decimal:
    try:
        price = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise SolPriceLookupError(f"invalid price payload: {value!r}") from exc
    if price <= 0:
        raise SolPriceLookupError(f"non-positive price payload: {price}")
    return price


def _request_json(url: str) -> dict:
    response = requests.get(
        url,
        headers=REQUEST_HEADERS,
        timeout=_env_float(
            "DEXTER_SOL_PRICE_REQUEST_TIMEOUT_SECONDS",
            DEFAULT_REQUEST_TIMEOUT_SECONDS,
        ),
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise SolPriceLookupError("unexpected non-dict JSON response")
    return payload


def _fetch_from_coingecko() -> Decimal:
    data = _request_json(COINGECKO_PRICE_URL)
    solana = data.get("solana")
    if not isinstance(solana, dict) or "usd" not in solana:
        raise SolPriceLookupError("CoinGecko response missing solana.usd")
    return _coerce_price(solana["usd"])


def _fetch_from_coinbase() -> Decimal:
    data = _request_json(COINBASE_PRICE_URL)
    payload = data.get("data")
    if not isinstance(payload, dict) or "amount" not in payload:
        raise SolPriceLookupError("Coinbase response missing data.amount")
    return _coerce_price(payload["amount"])


def _fetch_from_kraken() -> Decimal:
    data = _request_json(KRAKEN_PRICE_URL)
    errors = data.get("error")
    if errors:
        raise SolPriceLookupError(f"Kraken returned errors: {errors}")
    result = data.get("result")
    if not isinstance(result, dict) or not result:
        raise SolPriceLookupError("Kraken response missing result")
    ticker = next(iter(result.values()))
    if not isinstance(ticker, dict):
        raise SolPriceLookupError("Kraken ticker payload missing")
    close = ticker.get("c")
    if not isinstance(close, list) or not close:
        raise SolPriceLookupError("Kraken response missing close price")
    return _coerce_price(close[0])


PRICE_PROVIDERS: tuple[tuple[str, Callable[[], Decimal]], ...] = (
    ("CoinGecko", _fetch_from_coingecko),
    ("Coinbase", _fetch_from_coinbase),
    ("Kraken", _fetch_from_kraken),
)


def invalidate_solana_price_cache() -> None:
    global _cached_price_usd, _cached_at_monotonic, _last_failure_log_at_monotonic
    with _cache_lock:
        _cached_price_usd = None
        _cached_at_monotonic = 0.0
        _last_failure_log_at_monotonic = 0.0


def _store_cached_price(price: Decimal, now_monotonic: float) -> Decimal:
    global _cached_price_usd, _cached_at_monotonic
    with _cache_lock:
        _cached_price_usd = price
        _cached_at_monotonic = now_monotonic
    return price


def _log_failure_once(logger: logging.Logger, message: str, now_monotonic: float) -> None:
    global _last_failure_log_at_monotonic
    cooldown = max(
        _env_float(
            "DEXTER_SOL_PRICE_FAILURE_LOG_COOLDOWN_SECONDS",
            DEFAULT_FAILURE_LOG_COOLDOWN_SECONDS,
        ),
        0.0,
    )
    if cooldown == 0:
        logger.warning(message)
        return
    with _cache_lock:
        if now_monotonic - _last_failure_log_at_monotonic < cooldown:
            return
        _last_failure_log_at_monotonic = now_monotonic
    logger.warning(message)


def get_solana_price_usd(
    *,
    force_refresh: bool = False,
    logger: logging.Logger | None = None,
) -> Decimal:
    logger = logger or logging.getLogger(__name__)
    now_monotonic = time.monotonic()
    cache_ttl = max(
        _env_float(
            "DEXTER_SOL_PRICE_CACHE_TTL_SECONDS",
            DEFAULT_CACHE_TTL_SECONDS,
        ),
        0.0,
    )

    with _cache_lock:
        cached_price = _cached_price_usd
        cached_at = _cached_at_monotonic

    if (
        not force_refresh
        and cached_price is not None
        and cache_ttl > 0
        and now_monotonic - cached_at < cache_ttl
    ):
        return cached_price

    errors: list[str] = []
    for provider_name, provider in PRICE_PROVIDERS:
        try:
            fresh_price = provider()
        except Exception as exc:
            errors.append(f"{provider_name}: {exc}")
            continue
        return _store_cached_price(fresh_price, now_monotonic)

    fallback_price = cached_price or _env_decimal(
        "DEXTER_SOL_PRICE_FALLBACK_USD",
        DEFAULT_FALLBACK_PRICE_USD,
    )
    mode = "cached" if cached_price is not None else "fallback"
    error_suffix = "; ".join(errors) if errors else "no provider details"
    _log_failure_once(
        logger,
        f"Failed to refresh SOL/USD price from CoinGecko, Coinbase, and Kraken; "
        f"using {mode} price {fallback_price} USD. Details: {error_suffix}",
        now_monotonic,
    )
    return _store_cached_price(fallback_price, now_monotonic)
