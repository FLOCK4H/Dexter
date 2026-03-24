from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
import json
import math
from pathlib import Path
from typing import Any


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, Decimal):
        if not value.is_finite():
            return default
        return float(value)
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(result):
        return default
    return result


def _score_weighted(features: dict[str, float], weights: dict[str, float]) -> float:
    total_weight = 0.0
    score = 0.0
    for key, weight in weights.items():
        if weight <= 0:
            continue
        total_weight += float(weight)
        score += _clamp(features.get(key, 0.0)) * float(weight)
    if total_weight <= 0:
        return 0.0
    return score / total_weight


def _top_holder_share(holders: dict[str, Any]) -> float:
    balances: list[int] = []
    for holder in (holders or {}).values():
        balance = holder.get("balance", 0) if isinstance(holder, dict) else 0
        try:
            balance_int = int(balance or 0)
        except (TypeError, ValueError):
            balance_int = 0
        if balance_int > 0:
            balances.append(balance_int)
    if not balances:
        return 0.0
    total = sum(balances)
    if total <= 0:
        return 0.0
    return max(balances) / total


def _creator_wallet_action_score(owner: str | None, holders: dict[str, Any]) -> float:
    if not owner:
        return 0.5
    holder = (holders or {}).get(owner)
    if not isinstance(holder, dict):
        return 0.75
    balance_changes = holder.get("balance_changes", []) or []
    buys = 0
    sells = 0
    for item in balance_changes:
        action_type = str((item or {}).get("type", "")).lower()
        if action_type == "buy":
            buys += 1
        elif action_type == "sell":
            sells += 1
    total = buys + sells
    if total <= 0:
        return 0.75
    return _clamp((buys + 1) / (total + 2))


@dataclass(frozen=True)
class StrategyProfile:
    name: str
    version: str
    creator_weight: float
    token_weight: float
    buy_threshold: float
    min_entry_score: float
    min_trust_factor: float
    max_failure_cluster_ratio: float
    max_rug_ratio: float
    min_liquidity_quality: float
    creator_feature_weights: dict[str, float]
    token_feature_weights: dict[str, float]
    exit_drawdown_ratio: float
    exit_drop_time_seconds: float
    exit_stagnant_seconds: float
    exit_low_price_stagnant_seconds: float
    low_price_threshold: float

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


BUILTIN_STRATEGY_PROFILES: dict[str, StrategyProfile] = {
    "aggressive": StrategyProfile(
        name="aggressive",
        version="aggressive@2026-03-20",
        creator_weight=0.45,
        token_weight=0.55,
        buy_threshold=0.48,
        min_entry_score=0.48,
        min_trust_factor=0.35,
        max_failure_cluster_ratio=0.75,
        max_rug_ratio=0.80,
        min_liquidity_quality=0.10,
        creator_feature_weights={
            "trust_factor": 2.0,
            "launch_cadence_quality": 0.6,
            "failure_cluster_quality": 1.0,
            "rug_pattern_quality": 1.0,
            "migration_quality": 0.5,
            "wallet_reuse_quality": 0.3,
            "performance_quality": 1.2,
        },
        token_feature_weights={
            "buy_sell_imbalance": 1.6,
            "holder_concentration_quality": 1.0,
            "velocity_quality": 1.6,
            "decay_quality": 1.0,
            "liquidity_quality": 1.0,
            "creator_wallet_action_quality": 0.8,
        },
        exit_drawdown_ratio=0.48,
        exit_drop_time_seconds=12.0,
        exit_stagnant_seconds=1800.0,
        exit_low_price_stagnant_seconds=13.0,
        low_price_threshold=0.00000003,
    ),
    "balanced": StrategyProfile(
        name="balanced",
        version="balanced@2026-03-20",
        creator_weight=0.55,
        token_weight=0.45,
        buy_threshold=0.57,
        min_entry_score=0.57,
        min_trust_factor=0.45,
        max_failure_cluster_ratio=0.55,
        max_rug_ratio=0.65,
        min_liquidity_quality=0.16,
        creator_feature_weights={
            "trust_factor": 2.2,
            "launch_cadence_quality": 1.0,
            "failure_cluster_quality": 1.2,
            "rug_pattern_quality": 1.3,
            "migration_quality": 0.8,
            "wallet_reuse_quality": 0.5,
            "performance_quality": 1.0,
        },
        token_feature_weights={
            "buy_sell_imbalance": 1.2,
            "holder_concentration_quality": 1.2,
            "velocity_quality": 1.1,
            "decay_quality": 1.0,
            "liquidity_quality": 1.3,
            "creator_wallet_action_quality": 1.0,
        },
        exit_drawdown_ratio=0.50,
        exit_drop_time_seconds=15.0,
        exit_stagnant_seconds=1800.0,
        exit_low_price_stagnant_seconds=13.0,
        low_price_threshold=0.00000003,
    ),
    "conservative": StrategyProfile(
        name="conservative",
        version="conservative@2026-03-20",
        creator_weight=0.65,
        token_weight=0.35,
        buy_threshold=0.66,
        min_entry_score=0.66,
        min_trust_factor=0.55,
        max_failure_cluster_ratio=0.40,
        max_rug_ratio=0.45,
        min_liquidity_quality=0.22,
        creator_feature_weights={
            "trust_factor": 2.5,
            "launch_cadence_quality": 1.3,
            "failure_cluster_quality": 1.5,
            "rug_pattern_quality": 1.6,
            "migration_quality": 1.0,
            "wallet_reuse_quality": 0.6,
            "performance_quality": 0.9,
        },
        token_feature_weights={
            "buy_sell_imbalance": 0.9,
            "holder_concentration_quality": 1.4,
            "velocity_quality": 0.8,
            "decay_quality": 1.2,
            "liquidity_quality": 1.5,
            "creator_wallet_action_quality": 1.2,
        },
        exit_drawdown_ratio=0.45,
        exit_drop_time_seconds=18.0,
        exit_stagnant_seconds=1200.0,
        exit_low_price_stagnant_seconds=10.0,
        low_price_threshold=0.000000035,
    ),
}


def get_strategy_profile(name: str | None = None) -> StrategyProfile:
    selected = (name or "balanced").strip().lower()
    if selected not in BUILTIN_STRATEGY_PROFILES:
        raise ValueError(
            f"Unknown strategy profile '{selected}'. Available profiles: {', '.join(sorted(BUILTIN_STRATEGY_PROFILES))}"
        )
    return BUILTIN_STRATEGY_PROFILES[selected]


def compute_creator_features(entry: dict[str, Any] | None) -> dict[str, float]:
    entry = dict(entry or {})
    trust_factor = _clamp(_safe_float(entry.get("trust_factor"), 0.0))
    median_launch_gap_seconds = _safe_float(entry.get("median_launch_gap_seconds"), 900.0)
    launch_cadence_quality = _clamp(median_launch_gap_seconds / 3600.0)
    failure_cluster_ratio = _clamp(_safe_float(entry.get("failure_cluster_ratio"), 0.0))
    rug_ratio = _clamp(_safe_float(entry.get("rug_ratio"), 0.0))
    migration_ratio = _clamp(_safe_float(entry.get("migration_ratio"), 0.0))
    wallet_reuse_ratio = _clamp(_safe_float(entry.get("wallet_reuse_ratio"), 0.0))
    performance_score = _safe_float(entry.get("performance_score"), 0.0)
    performance_quality = _clamp(math.log10(performance_score + 1.0) / 6.0)
    return {
        "trust_factor": trust_factor,
        "launch_cadence_quality": launch_cadence_quality,
        "failure_cluster_quality": 1.0 - failure_cluster_ratio,
        "failure_cluster_ratio": failure_cluster_ratio,
        "rug_pattern_quality": 1.0 - rug_ratio,
        "rug_ratio": rug_ratio,
        "migration_quality": 1.0 - migration_ratio,
        "migration_ratio": migration_ratio,
        "wallet_reuse_quality": 1.0 - wallet_reuse_ratio,
        "wallet_reuse_ratio": wallet_reuse_ratio,
        "performance_quality": performance_quality,
    }


def compute_token_features(mint_state: dict[str, Any] | None, owner: str | None = None) -> dict[str, float]:
    mint_state = dict(mint_state or {})
    tx_counts = mint_state.get("tx_counts") or {}
    holders = mint_state.get("holders") or {}
    buys = max(0, int(tx_counts.get("buys", 0) or 0))
    sells = max(0, int(tx_counts.get("sells", 0) or 0))
    swaps = max(0, int(tx_counts.get("swaps", buys + sells) or 0))
    total_side_count = buys + sells
    buy_sell_imbalance = _clamp((buys + 1) / (total_side_count + 2))
    holder_concentration_quality = 1.0 - _clamp(_top_holder_share(holders))

    velocity_quality = _clamp(swaps / 25.0)

    current_price = _safe_float(mint_state.get("price") or mint_state.get("current_price"), 0.0)
    open_price = _safe_float(mint_state.get("open_price"), current_price)
    peak_price = _safe_float(mint_state.get("high_price"), current_price)
    decay_quality = 0.5
    if peak_price > 0:
        decay_quality = _clamp(current_price / peak_price)
    elif open_price > 0:
        decay_quality = _clamp(current_price / open_price)

    liquidity = _safe_float(mint_state.get("liquidity"), 0.0)
    market_cap = _safe_float(mint_state.get("mc") or mint_state.get("market_cap"), 0.0)
    liquidity_quality = _clamp((liquidity / market_cap) * 4.0) if market_cap > 0 else 0.0

    creator_wallet_action_quality = _creator_wallet_action_score(owner, holders)

    return {
        "buy_sell_imbalance": buy_sell_imbalance,
        "holder_concentration_quality": holder_concentration_quality,
        "velocity_quality": velocity_quality,
        "decay_quality": decay_quality,
        "liquidity_quality": liquidity_quality,
        "creator_wallet_action_quality": creator_wallet_action_quality,
    }


@dataclass(frozen=True)
class EntryStrategyEvaluation:
    profile_name: str
    profile_version: str
    should_buy: bool
    trust_level: int
    creator_score: float
    token_score: float
    total_score: float
    creator_features: dict[str, float]
    token_features: dict[str, float]
    thresholds: dict[str, float]
    blocking_reasons: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_name": self.profile_name,
            "profile_version": self.profile_version,
            "should_buy": self.should_buy,
            "trust_level": self.trust_level,
            "creator_score": self.creator_score,
            "token_score": self.token_score,
            "total_score": self.total_score,
            "creator_features": self.creator_features,
            "token_features": self.token_features,
            "thresholds": self.thresholds,
            "blocking_reasons": list(self.blocking_reasons),
        }


def evaluate_entry(
    profile: StrategyProfile,
    creator_entry: dict[str, Any] | None,
    token_state: dict[str, Any] | None,
    *,
    owner: str | None = None,
    current_exposure: int = 0,
    wallet_address: str | None = None,
) -> EntryStrategyEvaluation:
    creator_features = compute_creator_features(creator_entry)
    token_features = compute_token_features(token_state, owner=owner)
    creator_score = _score_weighted(creator_features, profile.creator_feature_weights)
    token_score = _score_weighted(token_features, profile.token_feature_weights)
    total_score = _clamp(
        (creator_score * float(profile.creator_weight)) + (token_score * float(profile.token_weight))
    )
    reasons: list[str] = []
    passed = True

    if creator_features["trust_factor"] < profile.min_trust_factor:
        passed = False
        reasons.append("trust_factor_below_threshold")
    if creator_features["failure_cluster_ratio"] > profile.max_failure_cluster_ratio:
        passed = False
        reasons.append("failure_cluster_too_high")
    if creator_features["rug_ratio"] > profile.max_rug_ratio:
        passed = False
        reasons.append("rug_ratio_too_high")
    if token_features["liquidity_quality"] < profile.min_liquidity_quality:
        passed = False
        reasons.append("liquidity_too_thin")
    if total_score < profile.min_entry_score:
        passed = False
        reasons.append("total_score_below_threshold")
    if current_exposure > 0:
        passed = False
        reasons.append("creator_already_has_open_exposure")

    trust_level = 0
    if passed and total_score >= max(profile.min_entry_score + 0.12, 0.75):
        trust_level = 2
    elif passed:
        trust_level = 1

    if passed and not reasons:
        reasons.append("entry_profile_passed")

    return EntryStrategyEvaluation(
        profile_name=profile.name,
        profile_version=profile.version,
        should_buy=passed,
        trust_level=trust_level,
        creator_score=round(creator_score, 6),
        token_score=round(token_score, 6),
        total_score=round(total_score, 6),
        creator_features=creator_features,
        token_features=token_features,
        thresholds={
            "buy_threshold": profile.buy_threshold,
            "min_entry_score": profile.min_entry_score,
            "min_trust_factor": profile.min_trust_factor,
            "max_failure_cluster_ratio": profile.max_failure_cluster_ratio,
            "max_rug_ratio": profile.max_rug_ratio,
            "min_liquidity_quality": profile.min_liquidity_quality,
        },
        blocking_reasons=([reason for reason in reasons if reason != "entry_profile_passed"] if not passed else []),
    )


def evaluate_exit(
    profile: StrategyProfile,
    *,
    position: dict[str, Any] | None,
    mint_state: dict[str, Any] | None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    position = dict(position or {})
    mint_state = dict(mint_state or {})
    context = dict(context or {})

    price = _safe_float(mint_state.get("price") or mint_state.get("current_price"), 0.0)
    peak_price = _safe_float(mint_state.get("high_price"), price)
    drawdown_ratio = 0.0
    if peak_price > 0 and price >= 0:
        drawdown_ratio = _clamp(1.0 - (price / peak_price))

    self_peak_change = _safe_float(context.get("self_peak_change"), 0.0)
    target_pct = _safe_float(context.get("target_pct"), 0.0)
    time_since_last_buy = _safe_float(context.get("time_since_last_buy"), 0.0)
    time_since_last_change = _safe_float(context.get("time_since_last_change"), 0.0)
    malicious = bool(context.get("malicious"))
    is_drop_time = bool(context.get("is_drop_time"))
    forced_reason = context.get("forced_reason")

    should_exit = False
    reason = "hold"
    explanations: list[str] = []

    if forced_reason:
        should_exit = True
        reason = str(forced_reason)
        explanations.append("operator_command")
    elif malicious or drawdown_ratio >= profile.exit_drawdown_ratio:
        should_exit = True
        reason = "malicious"
        explanations.append("drawdown_or_malicious_pattern")
    elif is_drop_time or time_since_last_buy >= profile.exit_drop_time_seconds:
        should_exit = True
        reason = "drop-time"
        explanations.append("buy_flow_stalled")
    elif self_peak_change >= target_pct > 0:
        should_exit = True
        reason = "target_hit"
        explanations.append("profit_target_reached")
    elif time_since_last_change >= profile.exit_stagnant_seconds:
        should_exit = True
        reason = "stagnant"
        explanations.append("price_stagnated")
    elif price <= profile.low_price_threshold and time_since_last_change >= profile.exit_low_price_stagnant_seconds:
        should_exit = True
        reason = "low-price-stagnant"
        explanations.append("low_price_and_stagnant")

    return {
        "profile": {"name": profile.name, "version": profile.version},
        "should_exit": should_exit,
        "reason": reason,
        "explanations": explanations or ["hold"],
        "breakdown": {
            "drawdown_ratio": round(drawdown_ratio, 6),
            "self_peak_change": round(self_peak_change, 6),
            "target_pct": round(target_pct, 6),
            "time_since_last_buy": round(time_since_last_buy, 3),
            "time_since_last_change": round(time_since_last_change, 3),
            "low_price_threshold": profile.low_price_threshold,
            "exit_drawdown_ratio": profile.exit_drawdown_ratio,
        },
        "position_context": {
            "token_balance": int(position.get("token_balance", 0) or 0),
            "buy_price": position.get("buy_price"),
        },
    }


def explain_sell(
    *,
    profile: StrategyProfile,
    token_state: dict[str, Any] | None,
    position: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    reason: str | None = None,
    buy_price: Any | None = None,
    current_price: Any | None = None,
) -> dict[str, Any]:
    merged_position = dict(position or {})
    if buy_price not in (None, ""):
        merged_position["buy_price"] = str(buy_price)
    merged_state = dict(token_state or {})
    if current_price not in (None, "") and "price" not in merged_state:
        merged_state["price"] = current_price
    explanation = evaluate_exit(profile, position=merged_position, mint_state=merged_state, context=context)
    if reason:
        explanation["requested_reason"] = reason
    return explanation


def serialize_strategy_profile(profile: StrategyProfile) -> dict[str, Any]:
    return profile.as_dict()


def _historical_creator_entry(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {
            "trust_factor": 0.0,
            "failure_cluster_ratio": 0.0,
            "rug_ratio": 0.0,
            "migration_ratio": 0.0,
            "wallet_reuse_ratio": 0.0,
            "median_launch_gap_seconds": 0.0,
            "performance_score": 0.0,
        }
    success_count = sum(1 for record in records if record.get("successful"))
    unsuccess_count = len(records) - success_count
    trust_factor = success_count / len(records)
    longest_failure_run = 0
    current_failure_run = 0
    rug_count = 0
    migration_count = 0
    launch_times: list[float] = []
    peak_market_caps: list[float] = []
    success_ratios: list[float] = []
    for record in records:
        if record.get("successful"):
            current_failure_run = 0
            if record.get("success_ratio", 0) > 0:
                success_ratios.append(_safe_float(record.get("success_ratio"), 0.0))
        else:
            current_failure_run += 1
            longest_failure_run = max(longest_failure_run, current_failure_run)
        if record.get("is_rug"):
            rug_count += 1
        if record.get("migrated"):
            migration_count += 1
        creation_time = _safe_float(record.get("creation_time"), 0.0)
        if creation_time > 0:
            launch_times.append(creation_time)
        peak_market_caps.append(_safe_float(record.get("peak_market_cap"), 0.0))
    launch_times = sorted(launch_times)
    gaps = [
        launch_times[index] - launch_times[index - 1]
        for index in range(1, len(launch_times))
        if launch_times[index] > launch_times[index - 1]
    ]
    median_gap = sorted(gaps)[len(gaps) // 2] if gaps else 0.0
    performance_score = 0.0
    if peak_market_caps:
        performance_score = sum(peak_market_caps) / len(peak_market_caps)
        if success_ratios:
            performance_score *= sum(success_ratios) / len(success_ratios)
    return {
        "trust_factor": trust_factor,
        "failure_cluster_ratio": longest_failure_run / len(records),
        "rug_ratio": rug_count / len(records),
        "migration_ratio": migration_count / len(records),
        "wallet_reuse_ratio": _clamp((len(records) - 1) / 10.0),
        "median_launch_gap_seconds": median_gap,
        "performance_score": performance_score,
    }


def summarize_historical_record(record: dict[str, Any]) -> dict[str, Any]:
    price_history = record.get("price_history") or {}
    tx_counts = record.get("tx_counts") or {}
    final_ohlc = record.get("final_ohlc") or {}
    open_price = _safe_float(final_ohlc.get("open"), 0.0)
    high_price = _safe_float(final_ohlc.get("high"), open_price)
    current_price = _safe_float(final_ohlc.get("close"), open_price)
    peak_market_cap = _safe_float(record.get("peak_market_cap"), 0.0)
    final_market_cap = _safe_float(record.get("final_market_cap"), 0.0)
    success_ratio = 0.0
    if open_price > 0 and high_price > 0:
        success_ratio = ((high_price - open_price) / open_price) * 100.0
    is_rug = peak_market_cap > 0 and final_market_cap <= peak_market_cap * 0.20
    return {
        "mint_id": record.get("mint_id"),
        "owner": record.get("owner"),
        "creation_time": record.get("creation_time"),
        "peak_market_cap": peak_market_cap,
        "successful": bool(record.get("successful")),
        "success_ratio": success_ratio,
        "is_rug": is_rug,
        "migrated": bool(record.get("migrated")),
        "mint_state": {
            "price_history": price_history,
            "tx_counts": tx_counts,
            "price": current_price,
            "current_price": current_price,
            "open_price": open_price,
            "high_price": high_price,
            "market_cap": final_market_cap,
            "mc": final_market_cap,
            "liquidity": max(0.0, final_market_cap * 0.12),
            "holders": record.get("holders") or {},
        },
    }


def backtest_records(
    records: list[dict[str, Any]],
    *,
    profile: StrategyProfile,
) -> dict[str, Any]:
    def _derived_success(raw_record: dict[str, Any], summary: dict[str, Any]) -> bool:
        if "successful" in raw_record:
            return bool(raw_record.get("successful"))
        tx_counts = raw_record.get("tx_counts") or {}
        swaps = int(tx_counts.get("swaps", 0) or 0)
        open_price = _safe_float((raw_record.get("final_ohlc") or {}).get("open"), 0.0)
        high_price = _safe_float((raw_record.get("final_ohlc") or {}).get("high"), open_price)
        if open_price <= 0 or high_price <= 0:
            return False
        return swaps >= 25 and high_price >= (open_price * 1.5)

    by_creator: dict[str, list[dict[str, Any]]] = {}
    evaluated_cases: list[dict[str, Any]] = []
    passed = 0
    winners = 0
    realized_edge_pct = 0.0

    sorted_records = sorted(
        records,
        key=lambda item: _safe_float(item.get("creation_time"), 0.0),
    )
    for raw_record in sorted_records:
        owner = str(raw_record.get("owner") or "")
        prior_records = by_creator.setdefault(owner, [])
        creator_entry = _historical_creator_entry(prior_records)
        record = summarize_historical_record(raw_record)
        successful = _derived_success(raw_record, record)
        decision = evaluate_entry(
            profile,
            creator_entry,
            record["mint_state"],
            owner=owner,
            current_exposure=0,
        )
        outcome_ratio = _safe_float(record.get("success_ratio"), 0.0)
        if decision.should_buy:
            passed += 1
            realized_edge_pct += outcome_ratio if successful else -25.0
            if successful:
                winners += 1
        evaluated_cases.append(
            {
                "mint_id": raw_record.get("mint_id"),
                "owner": owner,
                "decision": decision.to_dict(),
                "successful": successful,
                "outcome_ratio": round(outcome_ratio, 6),
            }
        )
        prior_records.append(
            {
                "creation_time": record.get("creation_time"),
                "peak_market_cap": record.get("peak_market_cap"),
                "successful": successful,
                "success_ratio": outcome_ratio,
                "is_rug": record.get("is_rug"),
                "migrated": bool(raw_record.get("migrated")),
            }
        )

    return {
        "profile": {"name": profile.name, "version": profile.version},
        "metrics": {
            "records": len(sorted_records),
            "passed": passed,
            "skipped": max(0, len(sorted_records) - passed),
            "wins": winners,
            "win_rate": round((winners / passed), 6) if passed else 0.0,
            "realized_edge_pct": round(realized_edge_pct, 6),
        },
        "cases": evaluated_cases,
    }


def render_backtest_report(report: dict[str, Any]) -> str:
    profile = report.get("profile", {})
    metrics = report.get("metrics", {})
    return "\n".join(
        [
            f"profile={profile.get('name')} version={profile.get('version')}",
            (
                "metrics "
                f"records={metrics.get('records', 0)} "
                f"passed={metrics.get('passed', 0)} "
                f"wins={metrics.get('wins', 0)} "
                f"win_rate={metrics.get('win_rate', 0)} "
                f"realized_edge_pct={metrics.get('realized_edge_pct', 0)}"
            ),
        ]
    )


def load_records_from_json(path: str | Path) -> list[dict[str, Any]]:
    file_path = Path(path)
    raw = file_path.read_text(encoding="utf-8")
    if file_path.suffix.lower() == ".jsonl":
        return [
            json.loads(line)
            for line in raw.splitlines()
            if line.strip()
        ]
    loaded = json.loads(raw)
    if isinstance(loaded, list):
        return loaded
    if isinstance(loaded, dict) and isinstance(loaded.get("records"), list):
        return loaded["records"]
    raise ValueError("Backtest input must be a JSON array, JSON object with a records list, or JSONL.")
