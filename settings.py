from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import os
import sys

from dexter_config import ENV_PATH, read_env_values


@dataclass(frozen=True)
class LegacySettingSpec:
    key: str
    default: str
    kind: str
    label: str
    detail: str
    section: str
    options: tuple[str, ...] = ()


LEGACY_SETTINGS_SPECS: dict[str, LegacySettingSpec] = {
    "TOTAL_SWAPS_ABOVE_2_MINTS": LegacySettingSpec(
        "TOTAL_SWAPS_ABOVE_2_MINTS",
        "1",
        "int",
        "Min swaps for creators with 2+ mints",
        "Minimum total swaps across all tokens for creators with multiple prior mints.",
        "Trust Factor",
    ),
    "TOTAL_SWAPS_1_MINT": LegacySettingSpec(
        "TOTAL_SWAPS_1_MINT",
        "1",
        "int",
        "Min swaps for creators with 1 mint",
        "Minimum total swaps for creators with only one prior mint in history.",
        "Trust Factor",
    ),
    "MEDIAN_PEAK_MC_ABOVE_2_MINTS": LegacySettingSpec(
        "MEDIAN_PEAK_MC_ABOVE_2_MINTS",
        "1",
        "decimal",
        "Median peak market cap for 2+ mints",
        "Median peak market cap threshold for creators with multiple historical mints.",
        "Trust Factor",
    ),
    "MEDIAN_PEAK_MC_1_MINT": LegacySettingSpec(
        "MEDIAN_PEAK_MC_1_MINT",
        "1",
        "decimal",
        "Median peak market cap for 1 mint",
        "Median peak market cap threshold for creators with a single historical mint.",
        "Trust Factor",
    ),
    "HIGHEST_PRICE_MIN_SWAPS": LegacySettingSpec(
        "HIGHEST_PRICE_MIN_SWAPS",
        "1",
        "int",
        "Min swaps before peak price",
        "Successful mints must reach their peak only after at least this many swaps.",
        "Trust Factor",
    ),
    "SNIPE_PRICE_TO_PEAK_PRICE_RATIO": LegacySettingSpec(
        "SNIPE_PRICE_TO_PEAK_PRICE_RATIO",
        "1.0",
        "decimal",
        "Peak-to-snipe ratio",
        "Required ratio between the peak price and the sampled sniping price.",
        "Trust Factor",
    ),
    "TRUST_FACTOR_RATIO": LegacySettingSpec(
        "TRUST_FACTOR_RATIO",
        "0.0",
        "decimal",
        "Minimum trust factor ratio",
        "Creators below this trust factor are treated as unsuccessful.",
        "Trust Factor",
    ),
    "SNIPING_PRICE_TIME": LegacySettingSpec(
        "SNIPING_PRICE_TIME",
        "0.0",
        "decimal",
        "Sniping price timestamp",
        "Seconds after the first trade used to sample the sniping price reference.",
        "Trust Factor",
    ),
    "AMOUNT_BUY_TL_2": LegacySettingSpec(
        "AMOUNT_BUY_TL_2",
        "0.01",
        "decimal",
        "Trust Level 2 buy size (USD)",
        "USD notional Dexter allocates when a creator lands in Trust Level 2.",
        "Trading & Fees",
    ),
    "AMOUNT_BUY_TL_1": LegacySettingSpec(
        "AMOUNT_BUY_TL_1",
        "0.01",
        "decimal",
        "Trust Level 1 buy size (USD)",
        "USD notional Dexter allocates when a creator lands in Trust Level 1.",
        "Trading & Fees",
    ),
    "SLIPPAGE_AMOUNT": LegacySettingSpec(
        "SLIPPAGE_AMOUNT",
        "1.30",
        "decimal",
        "Default slippage tolerance",
        "Ratio-style slippage control. Example: 1.30 means 30% tolerance; 15 means 15%.",
        "Trading & Fees",
    ),
    "USE_MEV": LegacySettingSpec(
        "USE_MEV",
        "false",
        "bool",
        "Use MEV sender",
        "Enable mainnet-only block-engine submission for live buy and sell transactions.",
        "Trading & Fees",
    ),
    "MEV_PROVIDER": LegacySettingSpec(
        "MEV_PROVIDER",
        "jito",
        "choice",
        "MEV provider",
        "Mainnet sender used when USE_MEV=true.",
        "Trading & Fees",
        options=("jito", "helius", "nextblock", "zero_slot", "temporal", "bloxroute"),
    ),
    "MEV_TIP": LegacySettingSpec(
        "MEV_TIP",
        "0.00001",
        "decimal",
        "MEV tip (SOL)",
        "Tip embedded into the signed transaction when MEV routing is enabled on mainnet.",
        "Trading & Fees",
    ),
    "FEE_LEVEL": LegacySettingSpec(
        "FEE_LEVEL",
        "medium",
        "choice",
        "Priority fee level",
        "Percentile Dexter samples from recent prioritization fees before applying the fee cap.",
        "Trading & Fees",
        options=("low", "medium", "high", "turbo", "max"),
    ),
    "MAX_FEE": LegacySettingSpec(
        "MAX_FEE",
        "0.00005",
        "decimal",
        "Max priority fee per transaction (SOL)",
        "Per-transaction cap for the estimated priority fee before Dexter submits a trade.",
        "Trading & Fees",
    ),
    "PRICE_STEP_UNITS": LegacySettingSpec(
        "PRICE_STEP_UNITS",
        "40",
        "decimal",
        "Price step units (%)",
        "Base percentage step Dexter uses for seeded target sizing and profit stepping.",
        "Trading & Fees",
    ),
    "PROFIT_MARGIN": LegacySettingSpec(
        "PROFIT_MARGIN",
        "0.8",
        "decimal",
        "Profit margin clamp",
        "Clamp multiplier applied to a creator's historical median profit range.",
        "Profit & Exit",
    ),
    "PRICE_TREND_WEIGHT": LegacySettingSpec(
        "PRICE_TREND_WEIGHT",
        "0.4",
        "decimal",
        "Price trend weight",
        "Composite-score weight assigned to price trend signals.",
        "Scoring Weights",
    ),
    "TX_MOMENTUM_WEIGHT": LegacySettingSpec(
        "TX_MOMENTUM_WEIGHT",
        "0.6",
        "decimal",
        "Transaction momentum weight",
        "Composite-score weight assigned to transaction momentum.",
        "Scoring Weights",
    ),
    "INCREMENT_THRESHOLD": LegacySettingSpec(
        "INCREMENT_THRESHOLD",
        "25",
        "decimal",
        "Increment threshold",
        "Composite score required before Dexter expands its current profit step.",
        "Profit & Exit",
    ),
    "INCREMENT_COOLDOWN": LegacySettingSpec(
        "INCREMENT_COOLDOWN",
        "15.0",
        "decimal",
        "Increment cooldown (s)",
        "Cooldown before Dexter can increase the active profit step again.",
        "Profit & Exit",
    ),
    "DECREMENT_THRESHOLD": LegacySettingSpec(
        "DECREMENT_THRESHOLD",
        "20",
        "decimal",
        "Decrement threshold",
        "Composite score below this level can trigger an earlier sell decision.",
        "Profit & Exit",
    ),
    "DROP_TIME": LegacySettingSpec(
        "DROP_TIME",
        "30",
        "int",
        "No-buy drop time (s)",
        "Sell when no buy arrives within this many seconds.",
        "Profit & Exit",
    ),
    "STAGNANT_UNDER_PRICE": LegacySettingSpec(
        "STAGNANT_UNDER_PRICE",
        "13",
        "int",
        "Low-price stagnant window (s)",
        "Treat very low prices as malicious when they remain stagnant for this long.",
        "Profit & Exit",
    ),
    "LEADERBOARD_UPDATE_INTERVAL": LegacySettingSpec(
        "LEADERBOARD_UPDATE_INTERVAL",
        "10",
        "int",
        "Leaderboard refresh interval (min)",
        "Minutes between creator leaderboard refreshes.",
        "Scoring Weights",
    ),
}

LEGACY_SETTINGS_SECTION_ORDER: tuple[str, ...] = (
    "Trading & Fees",
    "Trust Factor",
    "Profit & Exit",
    "Scoring Weights",
)

LEGACY_SETTINGS_FEATURED_KEYS: tuple[str, ...] = (
    "AMOUNT_BUY_TL_1",
    "AMOUNT_BUY_TL_2",
    "SLIPPAGE_AMOUNT",
    "USE_MEV",
    "MEV_PROVIDER",
    "MEV_TIP",
    "FEE_LEVEL",
    "MAX_FEE",
    "PRICE_STEP_UNITS",
    "PROFIT_MARGIN",
)

__all__ = tuple(LEGACY_SETTINGS_SPECS)


def _raw_setting_values() -> dict[str, str]:
    env_values = read_env_values(ENV_PATH)
    values: dict[str, str] = {}
    for key, spec in LEGACY_SETTINGS_SPECS.items():
        raw = os.environ.get(key)
        if raw is None:
            raw = env_values.get(key, spec.default)
        values[key] = str(raw).strip() or spec.default
    return values


def _parse_setting(spec: LegacySettingSpec, raw: str):
    if spec.kind == "int":
        return int(raw)
    if spec.kind == "choice":
        value = raw.strip().lower()
        if value not in spec.options:
            raise ValueError(f"{spec.key} must be one of: {', '.join(spec.options)}")
        return value
    if spec.kind == "bool":
        value = raw.strip().lower()
        if value not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
            raise ValueError(f"{spec.key} must be a boolean value")
        return value in {"1", "true", "yes", "on"}
    if spec.kind == "decimal":
        try:
            return Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f"{spec.key} must be a decimal value") from exc
    return raw


def load_legacy_settings() -> dict[str, object]:
    raw_values = _raw_setting_values()
    return {
        key: _parse_setting(spec, raw_values[key])
        for key, spec in LEGACY_SETTINGS_SPECS.items()
    }


def snapshot_legacy_settings() -> dict[str, object]:
    return {
        key: globals()[key]
        for key in LEGACY_SETTINGS_SPECS
        if key in globals()
    }


def reload_settings() -> dict[str, object]:
    values = load_legacy_settings()
    globals().update(values)
    return values


def sync_loaded_modules(values: dict[str, object] | None = None) -> dict[str, object]:
    snapshot = values or snapshot_legacy_settings()
    for module_name in ("Dexter", "DexAI.trust_factor", "trust_factor", "dexter_cli"):
        module = sys.modules.get(module_name)
        if module is None:
            continue
        for key, value in snapshot.items():
            setattr(module, key, value)
    return snapshot


def reload_and_sync_loaded_modules() -> dict[str, object]:
    values = reload_settings()
    sync_loaded_modules(values)
    return values


reload_settings()
