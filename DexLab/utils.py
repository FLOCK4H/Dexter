from __future__ import annotations

import math
from decimal import Decimal, ROUND_CEILING
from typing import Any

from aiohttp import ClientSession, ClientTimeout

import settings as legacy_settings


DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS = 300_000
DEFAULT_PRIORITY_FEE_MIN_MICRO_LAMPORTS = 10_000
DEFAULT_PRIORITY_FEE_MAX_MICRO_LAMPORTS = 30_000_000
FALLBACK_PRIORITY_FEE_ADDRESSES = (
    "11111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
)
PRIORITY_FEE_LEVEL_DEFAULTS = {
    "low": 50_000,
    "medium": 100_000,
    "high": 500_000,
    "turbo": 1_000_000,
    "max": 30_000_000,
}
PRIORITY_FEE_LEVEL_PERCENTILES = {
    "low": 0.50,
    "medium": 0.75,
    "high": 0.90,
    "turbo": 0.95,
    "max": 0.99,
}


async def usd_to_lamports(usd_amount: float, sol_price_usd: Decimal) -> int:
    sol_per_usd = Decimal(str(usd_amount)) / sol_price_usd
    lamports = int(sol_per_usd * Decimal(1_000_000_000))
    return lamports


async def lamports_to_tokens(lamports: int, price: Decimal) -> Decimal:
    lams_to_human = Decimal(lamports) / Decimal(1_000_000_000)
    tokens = lams_to_human / Decimal(price)
    token_amount = tokens * Decimal(1_000_000)
    return int(token_amount)


def usd_to_microlamports(usd_fee: float, sol_price_usd: Decimal, compute_units: int) -> int:
    if compute_units <= 0:
        raise ValueError("compute_units must be > 0")
    sol_fee = Decimal(str(usd_fee)) / sol_price_usd
    lamports_total = sol_fee * Decimal(1_000_000_000)
    micro_lamports_per_unit = (
        lamports_total * Decimal(1_000_000) / Decimal(compute_units)
    ).to_integral_value(rounding=ROUND_CEILING)
    return int(micro_lamports_per_unit)


def priority_fee_lamports(
    priority_micro_lamports: int,
    compute_units: int = DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS,
) -> int:
    if priority_micro_lamports <= 0 or compute_units <= 0:
        return 0
    total_micro_lamports = int(priority_micro_lamports) * int(compute_units)
    return (total_micro_lamports + 999_999) // 1_000_000


def normalize_priority_fee_level(raw: str | None = None) -> str:
    value = str(raw or getattr(legacy_settings, "FEE_LEVEL", "medium")).strip().lower()
    if value not in PRIORITY_FEE_LEVEL_DEFAULTS:
        return "medium"
    return value


def priority_fee_cap_sol() -> Decimal | None:
    value = getattr(legacy_settings, "MAX_FEE", Decimal("0"))
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    if value <= 0:
        return None
    return value


def clamp_priority_fee_from_cap(
    priority_micro_lamports: int,
    compute_units: int = DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS,
    *,
    max_fee_sol: Decimal | None = None,
) -> int:
    if priority_micro_lamports <= 0:
        return 0
    if compute_units <= 0:
        return int(priority_micro_lamports)

    fee_cap_sol = priority_fee_cap_sol() if max_fee_sol is None else max_fee_sol
    if fee_cap_sol is None or fee_cap_sol <= 0:
        return int(priority_micro_lamports)

    max_fee_lamports = int(
        (Decimal(fee_cap_sol) * Decimal(1_000_000_000)).to_integral_value(rounding=ROUND_CEILING)
    )
    if max_fee_lamports <= 0:
        return 0

    max_micro_lamports = (max_fee_lamports * 1_000_000) // int(compute_units)
    return min(int(priority_micro_lamports), int(max_micro_lamports))


def _default_priority_fee_micro_lamports(level: str) -> int:
    return PRIORITY_FEE_LEVEL_DEFAULTS[normalize_priority_fee_level(level)]


def _normalize_priority_fee_addresses(addresses: list[Any] | None) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    for item in addresses or FALLBACK_PRIORITY_FEE_ADDRESSES:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        values.append(text)
        if len(values) >= 128:
            break
    if not values:
        return list(FALLBACK_PRIORITY_FEE_ADDRESSES)
    return values


async def estimate_priority_fee_micro_lamports(
    rpc_url: str,
    *,
    addresses: list[Any] | None = None,
    fee_level: str | None = None,
    compute_units: int = DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS,
    timeout_seconds: float = 4.0,
) -> int:
    level = normalize_priority_fee_level(fee_level)
    default_fee = _default_priority_fee_micro_lamports(level)
    normalized_addresses = _normalize_priority_fee_addresses(addresses)
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getRecentPrioritizationFees",
        "params": [normalized_addresses],
    }

    try:
        timeout = ClientTimeout(total=timeout_seconds)
        async with ClientSession(timeout=timeout) as session:
            async with session.post(rpc_url, json=payload) as response:
                response.raise_for_status()
                data = await response.json()
    except Exception:
        return clamp_priority_fee_from_cap(default_fee, compute_units)

    result = data.get("result") or []
    samples: list[int] = []
    for item in result:
        try:
            price = int(item.get("prioritizationFee") or 0)
        except Exception:
            continue
        if price > 0:
            samples.append(price)

    if not samples:
        return clamp_priority_fee_from_cap(default_fee, compute_units)

    samples.sort()
    percentile = PRIORITY_FEE_LEVEL_PERCENTILES[level]
    index = math.ceil((len(samples) - 1) * percentile)
    estimate = samples[min(index, len(samples) - 1)]
    estimate = max(DEFAULT_PRIORITY_FEE_MIN_MICRO_LAMPORTS, estimate)
    estimate = min(DEFAULT_PRIORITY_FEE_MAX_MICRO_LAMPORTS, estimate)
    return clamp_priority_fee_from_cap(estimate, compute_units)
