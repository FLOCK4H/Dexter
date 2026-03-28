from __future__ import annotations

import asyncio
from dataclasses import dataclass
import datetime as dt
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Protocol
import uuid

import asyncpg


PHASE2_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS phase2_raw_events (
        fingerprint TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        program TEXT NOT NULL,
        signature TEXT NOT NULL,
        slot BIGINT NOT NULL DEFAULT 0,
        log_index INTEGER NOT NULL DEFAULT 0,
        event_type TEXT NOT NULL,
        is_mint BOOLEAN NOT NULL DEFAULT FALSE,
        mint_id TEXT,
        owner TEXT,
        observed_at TIMESTAMPTZ NOT NULL,
        raw_logs JSONB NOT NULL DEFAULT '[]'::jsonb,
        parsed_payload JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_raw_events_signature
    ON phase2_raw_events(signature);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_raw_events_mint_observed
    ON phase2_raw_events(mint_id, observed_at DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_raw_events_owner_observed
    ON phase2_raw_events(owner, observed_at DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_mints (
        mint_id TEXT PRIMARY KEY,
        owner TEXT,
        name TEXT,
        symbol TEXT,
        bonding_curve TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        mint_sig TEXT,
        created_at TIMESTAMPTZ,
        last_event_fingerprint TEXT,
        last_event_slot BIGINT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_mint_snapshots (
        snapshot_id BIGSERIAL PRIMARY KEY,
        mint_id TEXT NOT NULL REFERENCES phase2_mints(mint_id) ON DELETE CASCADE,
        lifecycle_state TEXT NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL,
        owner TEXT,
        name TEXT,
        symbol TEXT,
        bonding_curve TEXT,
        market_cap DOUBLE PRECISION,
        price_usd DOUBLE PRECISION,
        liquidity DOUBLE PRECISION,
        open_price DOUBLE PRECISION,
        high_price DOUBLE PRECISION,
        low_price DOUBLE PRECISION,
        current_price DOUBLE PRECISION,
        age_seconds DOUBLE PRECISION,
        tx_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
        volume JSONB NOT NULL DEFAULT '{}'::jsonb,
        holders JSONB NOT NULL DEFAULT '{}'::jsonb,
        price_history JSONB NOT NULL DEFAULT '{}'::jsonb,
        last_event_fingerprint TEXT,
        last_event_slot BIGINT,
        snapshot_payload JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_mint_snapshots_mint_time
    ON phase2_mint_snapshots(mint_id, recorded_at DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_leaderboard_generations (
        version TEXT PRIMARY KEY,
        generated_at TIMESTAMPTZ NOT NULL,
        source TEXT NOT NULL,
        creator_count INTEGER NOT NULL,
        ranking_hash TEXT NOT NULL,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_creator_snapshots (
        snapshot_id BIGSERIAL PRIMARY KEY,
        leaderboard_version TEXT NOT NULL REFERENCES phase2_leaderboard_generations(version) ON DELETE CASCADE,
        creator TEXT NOT NULL,
        rank INTEGER NOT NULL,
        mint_count INTEGER NOT NULL DEFAULT 0,
        total_swaps INTEGER NOT NULL DEFAULT 0,
        success_count INTEGER NOT NULL DEFAULT 0,
        unsuccess_count INTEGER NOT NULL DEFAULT 0,
        median_peak_market_cap DOUBLE PRECISION NOT NULL DEFAULT 0,
        median_market_cap DOUBLE PRECISION NOT NULL DEFAULT 0,
        median_open_price DOUBLE PRECISION NOT NULL DEFAULT 0,
        median_high_price DOUBLE PRECISION NOT NULL DEFAULT 0,
        trust_factor DOUBLE PRECISION NOT NULL DEFAULT 0,
        avg_success_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
        median_success_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
        performance_score DOUBLE PRECISION NOT NULL DEFAULT 0,
        entry_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        UNIQUE (leaderboard_version, creator)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_creator_snapshots_creator_version
    ON phase2_creator_snapshots(creator, leaderboard_version DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_trade_sessions (
        session_id TEXT PRIMARY KEY,
        mint_id TEXT NOT NULL,
        owner TEXT NOT NULL,
        runtime_mode TEXT NOT NULL,
        trust_level INTEGER,
        leaderboard_version TEXT REFERENCES phase2_leaderboard_generations(version),
        open_reason TEXT,
        status TEXT NOT NULL DEFAULT 'open',
        session_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        buy_cost_lamports BIGINT,
        sell_proceeds_lamports BIGINT,
        realized_profit_lamports BIGINT,
        opened_at TIMESTAMPTZ NOT NULL,
        closed_at TIMESTAMPTZ,
        close_reason TEXT
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_trade_sessions_mint_time
    ON phase2_trade_sessions(mint_id, opened_at DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_orders (
        order_id BIGSERIAL PRIMARY KEY,
        session_id TEXT NOT NULL REFERENCES phase2_trade_sessions(session_id) ON DELETE CASCADE,
        mint_id TEXT NOT NULL,
        side TEXT NOT NULL,
        venue TEXT NOT NULL,
        status TEXT NOT NULL,
        requested_token_amount BIGINT,
        requested_lamports BIGINT,
        tx_id TEXT,
        order_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_orders_session_side
    ON phase2_orders(session_id, side, created_at);
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_fills (
        fill_id BIGSERIAL PRIMARY KEY,
        order_id BIGINT NOT NULL REFERENCES phase2_orders(order_id) ON DELETE CASCADE,
        session_id TEXT NOT NULL REFERENCES phase2_trade_sessions(session_id) ON DELETE CASCADE,
        mint_id TEXT NOT NULL,
        side TEXT NOT NULL,
        tx_id TEXT,
        fill_price TEXT,
        token_amount BIGINT,
        cost_lamports BIGINT,
        proceeds_lamports BIGINT,
        fee_lamports BIGINT,
        wallet_delta_lamports BIGINT,
        wallet_balance BIGINT,
        fill_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        filled_at TIMESTAMPTZ NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_fills_session_time
    ON phase2_fills(session_id, filled_at);
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_holding_snapshots (
        snapshot_id BIGSERIAL PRIMARY KEY,
        session_id TEXT NOT NULL REFERENCES phase2_trade_sessions(session_id) ON DELETE CASCADE,
        mint_id TEXT NOT NULL,
        owner TEXT,
        status TEXT NOT NULL,
        token_balance BIGINT NOT NULL DEFAULT 0,
        buy_price TEXT,
        cost_basis_lamports BIGINT,
        buy_fee_lamports BIGINT,
        buy_wallet_balance BIGINT,
        snapshot_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        recorded_at TIMESTAMPTZ NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_holding_snapshots_session_time
    ON phase2_holding_snapshots(session_id, recorded_at);
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_strategy_decisions (
        decision_id BIGSERIAL PRIMARY KEY,
        session_id TEXT REFERENCES phase2_trade_sessions(session_id) ON DELETE CASCADE,
        mint_id TEXT NOT NULL,
        owner TEXT,
        decision_type TEXT NOT NULL,
        reason TEXT NOT NULL,
        raw_event_fingerprint TEXT,
        decision_inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
        decision_outcome JSONB NOT NULL DEFAULT '{}'::jsonb,
        decided_at TIMESTAMPTZ NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_strategy_decisions_session_time
    ON phase2_strategy_decisions(session_id, decided_at);
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_strategy_profiles (
        profile_key TEXT PRIMARY KEY,
        profile_name TEXT NOT NULL,
        version TEXT NOT NULL,
        definition JSONB NOT NULL DEFAULT '{}'::jsonb,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_position_journal (
        session_id TEXT PRIMARY KEY REFERENCES phase2_trade_sessions(session_id) ON DELETE CASCADE,
        mint_id TEXT NOT NULL,
        owner TEXT,
        strategy_profile TEXT,
        status TEXT NOT NULL,
        intended_size_lamports BIGINT,
        intended_token_amount BIGINT,
        executed_size_lamports BIGINT,
        executed_token_amount BIGINT,
        actual_buy_price TEXT,
        exit_reason TEXT,
        realized_profit_lamports BIGINT,
        journal_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        opened_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        closed_at TIMESTAMPTZ
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_position_journal_status_time
    ON phase2_position_journal(status, updated_at DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_backtest_runs (
        run_id TEXT PRIMARY KEY,
        profile_key TEXT NOT NULL REFERENCES phase2_strategy_profiles(profile_key) ON DELETE RESTRICT,
        input_source TEXT NOT NULL,
        summary_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_backtest_trades (
        trade_id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES phase2_backtest_runs(run_id) ON DELETE CASCADE,
        mint_id TEXT NOT NULL,
        owner TEXT,
        entered BOOLEAN NOT NULL DEFAULT FALSE,
        score DOUBLE PRECISION NOT NULL DEFAULT 0,
        realized_return_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
        trade_payload JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_backtest_trades_run
    ON phase2_backtest_trades(run_id, entered DESC, score DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS phase2_risk_events (
        event_id BIGSERIAL PRIMARY KEY,
        session_id TEXT REFERENCES phase2_trade_sessions(session_id) ON DELETE CASCADE,
        mint_id TEXT NOT NULL,
        owner TEXT,
        event_type TEXT NOT NULL,
        severity TEXT NOT NULL,
        detail TEXT NOT NULL,
        event_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_phase2_risk_events_session_time
    ON phase2_risk_events(session_id, created_at DESC);
    """,
]


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _ensure_utc(value: dt.datetime | None) -> dt.datetime:
    if value is None:
        return _utc_now()
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dt.datetime):
        return _ensure_utc(value).isoformat()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value


def _jsonb_param(value: Any) -> str:
    return json.dumps(_json_safe(value), sort_keys=True, allow_nan=False)


def _canonical_payload(value: Any) -> str:
    return json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def _finite_float_or(value: Any, default: float | None = None) -> float | None:
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


def compute_event_fingerprint(
    *,
    source: str,
    signature: str,
    log_index: int,
    event_type: str,
    mint_id: str | None,
    payload: Any,
) -> str:
    seed = "|".join(
        [
            source,
            signature or "",
            str(log_index),
            event_type,
            mint_id or "",
            _canonical_payload(payload),
        ]
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def compute_leaderboard_version(leaderboard: dict[str, dict[str, Any]]) -> str:
    entries = prepare_leaderboard_entries(leaderboard)
    payload = _canonical_payload(entries)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"lgd_{digest[:20]}"


def prepare_leaderboard_entries(leaderboard: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = sorted(
        leaderboard.items(),
        key=lambda item: (item[1].get("performance_score", 0), item[0]),
        reverse=True,
    )
    entries: list[dict[str, Any]] = []
    for rank, (creator, entry) in enumerate(ranked, start=1):
        normalized = dict(_json_safe(entry))
        normalized["creator"] = creator
        normalized["rank"] = rank
        entries.append(normalized)
    return entries


def _default_export_dir(config: Any) -> Path:
    if getattr(config, "phase2", None) is not None:
        export_dir = getattr(config.phase2, "export_dir", None)
        if export_dir is not None:
            return Path(export_dir)
    return Path(config.project_root) / "dev" / "exports"


def _default_retention_days(config: Any) -> int:
    if getattr(config, "phase2", None) is not None:
        return int(getattr(config.phase2, "raw_event_retention_days", 30))
    return 30


def _default_mint_snapshot_interval_seconds(config: Any) -> int:
    if getattr(config, "phase2", None) is not None:
        return int(getattr(config.phase2, "mint_snapshot_interval_seconds", 15))
    return 15


def _default_mint_snapshot_retention_per_mint(config: Any) -> int:
    if getattr(config, "phase2", None) is not None:
        return int(getattr(config.phase2, "mint_snapshot_retention_per_mint", 4))
    return 4


def _default_maintenance_interval_seconds(config: Any) -> int:
    if getattr(config, "phase2", None) is not None:
        return int(getattr(config.phase2, "maintenance_interval_seconds", 60))
    return 60


def _default_max_database_size_bytes(config: Any) -> int:
    if getattr(config, "phase2", None) is not None:
        return int(getattr(config.phase2, "max_database_size_bytes", 2147483648))
    return 2147483648


def _container_length(value: Any) -> int:
    if isinstance(value, (dict, list, tuple, set)):
        return len(value)
    return 0


def _compact_mint_snapshot(snapshot: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    tx_counts = snapshot.get("tx_counts") or {}
    volume = snapshot.get("volume") or {}
    holders = snapshot.get("holders") or {}
    price_history = snapshot.get("price_history") or {}
    compact_payload = {
        "mint_sig": snapshot.get("mint_sig"),
        "created_at": snapshot.get("created_at"),
        "holder_count": _container_length(holders),
        "price_point_count": _container_length(price_history),
        "swap_count": int((tx_counts or {}).get("swaps", 0) or 0) if isinstance(tx_counts, dict) else 0,
        "buy_count": int((tx_counts or {}).get("buys", 0) or 0) if isinstance(tx_counts, dict) else 0,
        "sell_count": int((tx_counts or {}).get("sells", 0) or 0) if isinstance(tx_counts, dict) else 0,
        "volume_windows": sorted(str(key) for key in volume.keys()) if isinstance(volume, dict) else [],
    }
    sanitized_snapshot = dict(snapshot)
    sanitized_snapshot["holders"] = {}
    sanitized_snapshot["price_history"] = {}
    return sanitized_snapshot, _json_safe(compact_payload)


@dataclass(frozen=True)
class ReplayInvariant:
    name: str
    status: str
    detail: str


class SessionReplayRepository(Protocol):
    async def fetch_session_bundle(
        self,
        *,
        session_id: str | None = None,
        mint_id: str | None = None,
    ) -> dict[str, Any]:
        ...


class Phase2Store:
    def __init__(self, db_dsn: str, config: Any | None = None):
        self.db_dsn = db_dsn
        self.config = config
        self.pool: asyncpg.Pool | None = None
        self._last_retention_prune: dt.datetime | None = None
        self._last_snapshot_prune: dt.datetime | None = None
        self._last_vacuum_at: dt.datetime | None = None
        self._last_mint_snapshot_recorded: dict[str, tuple[str, dt.datetime]] = {}

    def bind_pool(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def _with_connection(self, operation):
        if self.pool is not None:
            async with self.pool.acquire() as conn:
                return await operation(conn)
        conn = await asyncpg.connect(self.db_dsn)
        try:
            return await operation(conn)
        finally:
            await conn.close()

    async def ensure_schema(self) -> None:
        async def _operation(conn):
            for statement in PHASE2_SCHEMA_STATEMENTS:
                await conn.execute(statement)

        await self._with_connection(_operation)

    async def record_raw_event(
        self,
        *,
        source: str,
        program: str,
        signature: str,
        slot: int,
        log_index: int,
        event_type: str,
        is_mint: bool,
        payload: dict[str, Any],
        raw_logs: list[str] | None = None,
        observed_at: dt.datetime | None = None,
    ) -> str:
        mint_id = str(payload.get("mint", "") or "")
        owner = str(payload.get("user") or payload.get("owner") or "")
        fingerprint = compute_event_fingerprint(
            source=source,
            signature=signature,
            log_index=log_index,
            event_type=event_type,
            mint_id=mint_id or None,
            payload=payload,
        )
        observed_at = _ensure_utc(observed_at)

        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_raw_events (
                    fingerprint,
                    source,
                    program,
                    signature,
                    slot,
                    log_index,
                    event_type,
                    is_mint,
                    mint_id,
                    owner,
                    observed_at,
                    raw_logs,
                    parsed_payload
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), NULLIF($10, ''),
                    $11, $12::jsonb, $13::jsonb
                )
                ON CONFLICT (fingerprint) DO NOTHING
                """,
                fingerprint,
                source,
                program,
                signature,
                int(slot or 0),
                int(log_index),
                event_type,
                bool(is_mint),
                mint_id,
                owner,
                observed_at,
                _jsonb_param(raw_logs or []),
                _jsonb_param(payload),
            )

        await self._with_connection(_operation)
        return fingerprint

    async def upsert_mint_record(
        self,
        *,
        mint_id: str,
        owner: str | None = None,
        name: str | None = None,
        symbol: str | None = None,
        bonding_curve: str | None = None,
        status: str = "active",
        mint_sig: str | None = None,
        created_at: dt.datetime | None = None,
        last_event_fingerprint: str | None = None,
        last_event_slot: int | None = None,
    ) -> None:
        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_mints (
                    mint_id,
                    owner,
                    name,
                    symbol,
                    bonding_curve,
                    status,
                    mint_sig,
                    created_at,
                    last_event_fingerprint,
                    last_event_slot,
                    updated_at
                )
                VALUES (
                    $1, NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''),
                    $6, NULLIF($7, ''), $8, NULLIF($9, ''), $10, $11
                )
                ON CONFLICT (mint_id) DO UPDATE SET
                    owner = COALESCE(EXCLUDED.owner, phase2_mints.owner),
                    name = COALESCE(EXCLUDED.name, phase2_mints.name),
                    symbol = COALESCE(EXCLUDED.symbol, phase2_mints.symbol),
                    bonding_curve = COALESCE(EXCLUDED.bonding_curve, phase2_mints.bonding_curve),
                    status = EXCLUDED.status,
                    mint_sig = COALESCE(EXCLUDED.mint_sig, phase2_mints.mint_sig),
                    created_at = COALESCE(EXCLUDED.created_at, phase2_mints.created_at),
                    last_event_fingerprint = COALESCE(EXCLUDED.last_event_fingerprint, phase2_mints.last_event_fingerprint),
                    last_event_slot = COALESCE(EXCLUDED.last_event_slot, phase2_mints.last_event_slot),
                    updated_at = EXCLUDED.updated_at
                """,
                mint_id,
                owner or "",
                name or "",
                symbol or "",
                bonding_curve or "",
                status,
                mint_sig or "",
                _ensure_utc(created_at) if created_at is not None else None,
                last_event_fingerprint or "",
                int(last_event_slot) if last_event_slot is not None else None,
                _utc_now(),
            )

        await self._with_connection(_operation)

    async def record_mint_snapshot(
        self,
        *,
        mint_id: str,
        lifecycle_state: str,
        snapshot: dict[str, Any],
        recorded_at: dt.datetime | None = None,
        last_event_fingerprint: str | None = None,
        last_event_slot: int | None = None,
    ) -> None:
        await self.upsert_mint_record(
            mint_id=mint_id,
            owner=str(snapshot.get("owner") or ""),
            name=str(snapshot.get("name") or ""),
            symbol=str(snapshot.get("symbol") or ""),
            bonding_curve=str(snapshot.get("bonding_curve") or ""),
            status=lifecycle_state,
            mint_sig=str(snapshot.get("mint_sig") or ""),
            created_at=snapshot.get("created_at"),
            last_event_fingerprint=last_event_fingerprint,
            last_event_slot=last_event_slot,
        )

        recorded_at = _ensure_utc(recorded_at)
        if not self._should_record_mint_snapshot(
            mint_id=mint_id,
            lifecycle_state=lifecycle_state,
            recorded_at=recorded_at,
        ):
            return

        compact_snapshot, compact_payload = _compact_mint_snapshot(snapshot)

        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_mint_snapshots (
                    mint_id,
                    lifecycle_state,
                    recorded_at,
                    owner,
                    name,
                    symbol,
                    bonding_curve,
                    market_cap,
                    price_usd,
                    liquidity,
                    open_price,
                    high_price,
                    low_price,
                    current_price,
                    age_seconds,
                    tx_counts,
                    volume,
                    holders,
                    price_history,
                    last_event_fingerprint,
                    last_event_slot,
                    snapshot_payload
                )
                VALUES (
                    $1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''),
                    $8, $9, $10, $11, $12, $13, $14, $15,
                    $16::jsonb, $17::jsonb, $18::jsonb, $19::jsonb,
                    NULLIF($20, ''), $21, $22::jsonb
                )
                """,
                mint_id,
                lifecycle_state,
                recorded_at,
                str(compact_snapshot.get("owner") or ""),
                str(compact_snapshot.get("name") or ""),
                str(compact_snapshot.get("symbol") or ""),
                str(compact_snapshot.get("bonding_curve") or ""),
                _finite_float_or(compact_snapshot.get("market_cap"), 0.0),
                _finite_float_or(compact_snapshot.get("price_usd"), 0.0),
                _finite_float_or(compact_snapshot.get("liquidity"), 0.0),
                _finite_float_or(compact_snapshot.get("open_price"), 0.0),
                _finite_float_or(compact_snapshot.get("high_price"), 0.0),
                _finite_float_or(compact_snapshot.get("low_price")),
                _finite_float_or(compact_snapshot.get("current_price"), 0.0),
                _finite_float_or(compact_snapshot.get("age"), 0.0),
                _jsonb_param(compact_snapshot.get("tx_counts") or {}),
                _jsonb_param(compact_snapshot.get("volume") or {}),
                _jsonb_param({}),
                _jsonb_param({}),
                last_event_fingerprint or "",
                int(last_event_slot) if last_event_slot is not None else None,
                _jsonb_param(compact_payload),
            )

        await self._with_connection(_operation)

    def _should_record_mint_snapshot(
        self,
        *,
        mint_id: str,
        lifecycle_state: str,
        recorded_at: dt.datetime,
    ) -> bool:
        if lifecycle_state not in {"active", "migrated"}:
            self._last_mint_snapshot_recorded[mint_id] = (lifecycle_state, recorded_at)
            return True

        interval_seconds = _default_mint_snapshot_interval_seconds(self.config)
        previous = self._last_mint_snapshot_recorded.get(mint_id)
        if previous is None:
            self._last_mint_snapshot_recorded[mint_id] = (lifecycle_state, recorded_at)
            return True

        previous_state, previous_recorded_at = previous
        if previous_state != lifecycle_state:
            self._last_mint_snapshot_recorded[mint_id] = (lifecycle_state, recorded_at)
            return True

        should_record = (recorded_at - previous_recorded_at) >= dt.timedelta(seconds=interval_seconds)
        if should_record:
            self._last_mint_snapshot_recorded[mint_id] = (lifecycle_state, recorded_at)
        return should_record

    async def record_leaderboard_generation(
        self,
        leaderboard: dict[str, dict[str, Any]],
        *,
        source: str,
        generated_at: dt.datetime | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        generated_at = _ensure_utc(generated_at)
        entries = prepare_leaderboard_entries(leaderboard)
        version = compute_leaderboard_version(leaderboard)
        ranking_hash = hashlib.sha256(_canonical_payload(entries).encode("utf-8")).hexdigest()

        async def _operation(conn):
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO phase2_leaderboard_generations (
                        version,
                        generated_at,
                        source,
                        creator_count,
                        ranking_hash,
                        metadata
                    )
                    VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                    ON CONFLICT (version) DO NOTHING
                    """,
                    version,
                    generated_at,
                    source,
                    len(entries),
                    ranking_hash,
                    _jsonb_param(metadata or {}),
                )
                await conn.execute(
                    "DELETE FROM phase2_creator_snapshots WHERE leaderboard_version = $1",
                    version,
                )
                for entry in entries:
                    await conn.execute(
                        """
                        INSERT INTO phase2_creator_snapshots (
                            leaderboard_version,
                            creator,
                            rank,
                            mint_count,
                            total_swaps,
                            success_count,
                            unsuccess_count,
                            median_peak_market_cap,
                            median_market_cap,
                            median_open_price,
                            median_high_price,
                            trust_factor,
                            avg_success_ratio,
                            median_success_ratio,
                            performance_score,
                            entry_payload
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                            $12, $13, $14, $15, $16::jsonb
                        )
                        """,
                        version,
                        entry["creator"],
                        int(entry["rank"]),
                        int(entry.get("mint_count", 0) or 0),
                        int(entry.get("total_swaps", 0) or 0),
                        int(entry.get("success_count", 0) or 0),
                        int(entry.get("unsuccess_count", 0) or 0),
                        float(entry.get("median_peak_market_cap", 0) or 0),
                        float(entry.get("median_market_cap", 0) or 0),
                        float(entry.get("median_open_price", 0) or 0),
                        float(entry.get("median_high_price", 0) or 0),
                        float(entry.get("trust_factor", 0) or 0),
                        float(entry.get("avg_success_ratio", 0) or 0),
                        float(entry.get("median_success_ratio", 0) or 0),
                        float(entry.get("performance_score", 0) or 0),
                        _jsonb_param(entry),
                    )

        await self._with_connection(_operation)
        return version

    async def start_trade_session(
        self,
        *,
        mint_id: str,
        owner: str,
        runtime_mode: str,
        trust_level: int | None = None,
        leaderboard_version: str | None = None,
        open_reason: str | None = None,
        opened_at: dt.datetime | None = None,
        session_context: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> str:
        opened_at = _ensure_utc(opened_at)
        session_id = session_id or f"session_{uuid.uuid4().hex}"

        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_trade_sessions (
                    session_id,
                    mint_id,
                    owner,
                    runtime_mode,
                    trust_level,
                    leaderboard_version,
                    open_reason,
                    status,
                    session_context,
                    opened_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, NULLIF($6, ''), NULLIF($7, ''),
                    'open', $8::jsonb, $9
                )
                ON CONFLICT (session_id) DO NOTHING
                """,
                session_id,
                mint_id,
                owner,
                runtime_mode,
                trust_level,
                leaderboard_version or "",
                open_reason or "",
                _jsonb_param(session_context or {}),
                opened_at,
            )

        await self._with_connection(_operation)
        return session_id

    async def close_trade_session(
        self,
        session_id: str,
        *,
        status: str,
        close_reason: str | None = None,
        buy_cost_lamports: int | None = None,
        sell_proceeds_lamports: int | None = None,
        realized_profit_lamports: int | None = None,
        closed_at: dt.datetime | None = None,
    ) -> None:
        async def _operation(conn):
            await conn.execute(
                """
                UPDATE phase2_trade_sessions
                SET status = $2,
                    close_reason = NULLIF($3, ''),
                    buy_cost_lamports = COALESCE($4, buy_cost_lamports),
                    sell_proceeds_lamports = COALESCE($5, sell_proceeds_lamports),
                    realized_profit_lamports = COALESCE($6, realized_profit_lamports),
                    closed_at = $7
                WHERE session_id = $1
                """,
                session_id,
                status,
                close_reason or "",
                buy_cost_lamports,
                sell_proceeds_lamports,
                realized_profit_lamports,
                _ensure_utc(closed_at),
            )

        await self._with_connection(_operation)

    async def record_order(
        self,
        *,
        session_id: str,
        mint_id: str,
        side: str,
        venue: str,
        status: str,
        requested_token_amount: int | None = None,
        requested_lamports: int | None = None,
        tx_id: str | None = None,
        context: dict[str, Any] | None = None,
        created_at: dt.datetime | None = None,
    ) -> int:
        created_at = _ensure_utc(created_at)

        async def _operation(conn):
            row = await conn.fetchrow(
                """
                INSERT INTO phase2_orders (
                    session_id,
                    mint_id,
                    side,
                    venue,
                    status,
                    requested_token_amount,
                    requested_lamports,
                    tx_id,
                    order_context,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), $9::jsonb, $10, $10
                )
                RETURNING order_id
                """,
                session_id,
                mint_id,
                side,
                venue,
                status,
                requested_token_amount,
                requested_lamports,
                tx_id or "",
                _jsonb_param(context or {}),
                created_at,
            )
            return int(row["order_id"])

        return await self._with_connection(_operation)

    async def update_order(
        self,
        order_id: int,
        *,
        status: str,
        tx_id: str | None = None,
        context: dict[str, Any] | None = None,
        updated_at: dt.datetime | None = None,
    ) -> None:
        async def _operation(conn):
            await conn.execute(
                """
                UPDATE phase2_orders
                SET status = $2,
                    tx_id = COALESCE(NULLIF($3, ''), tx_id),
                    order_context = order_context || $4::jsonb,
                    updated_at = $5
                WHERE order_id = $1
                """,
                order_id,
                status,
                tx_id or "",
                _jsonb_param(context or {}),
                _ensure_utc(updated_at),
            )

        await self._with_connection(_operation)

    async def record_fill(
        self,
        *,
        order_id: int,
        session_id: str,
        mint_id: str,
        side: str,
        tx_id: str | None = None,
        fill_price: str | None = None,
        token_amount: int | None = None,
        cost_lamports: int | None = None,
        proceeds_lamports: int | None = None,
        fee_lamports: int | None = None,
        wallet_delta_lamports: int | None = None,
        wallet_balance: int | None = None,
        context: dict[str, Any] | None = None,
        filled_at: dt.datetime | None = None,
    ) -> None:
        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_fills (
                    order_id,
                    session_id,
                    mint_id,
                    side,
                    tx_id,
                    fill_price,
                    token_amount,
                    cost_lamports,
                    proceeds_lamports,
                    fee_lamports,
                    wallet_delta_lamports,
                    wallet_balance,
                    fill_context,
                    filled_at
                )
                VALUES (
                    $1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), $7, $8, $9, $10,
                    $11, $12, $13::jsonb, $14
                )
                """,
                order_id,
                session_id,
                mint_id,
                side,
                tx_id or "",
                fill_price or "",
                token_amount,
                cost_lamports,
                proceeds_lamports,
                fee_lamports,
                wallet_delta_lamports,
                wallet_balance,
                _jsonb_param(context or {}),
                _ensure_utc(filled_at),
            )

        await self._with_connection(_operation)

    async def record_holding_snapshot(
        self,
        *,
        session_id: str,
        mint_id: str,
        owner: str | None,
        status: str,
        token_balance: int,
        buy_price: str | None = None,
        cost_basis_lamports: int | None = None,
        buy_fee_lamports: int | None = None,
        buy_wallet_balance: int | None = None,
        snapshot_payload: dict[str, Any] | None = None,
        recorded_at: dt.datetime | None = None,
    ) -> None:
        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_holding_snapshots (
                    session_id,
                    mint_id,
                    owner,
                    status,
                    token_balance,
                    buy_price,
                    cost_basis_lamports,
                    buy_fee_lamports,
                    buy_wallet_balance,
                    snapshot_payload,
                    recorded_at
                )
                VALUES (
                    $1, $2, NULLIF($3, ''), $4, $5, NULLIF($6, ''), $7, $8, $9,
                    $10::jsonb, $11
                )
                """,
                session_id,
                mint_id,
                owner or "",
                status,
                int(token_balance or 0),
                buy_price or "",
                cost_basis_lamports,
                buy_fee_lamports,
                buy_wallet_balance,
                _jsonb_param(snapshot_payload or {}),
                _ensure_utc(recorded_at),
            )

        await self._with_connection(_operation)

    async def record_strategy_decision(
        self,
        *,
        session_id: str | None,
        mint_id: str,
        owner: str | None,
        decision_type: str,
        reason: str,
        raw_event_fingerprint: str | None = None,
        decision_inputs: dict[str, Any] | None = None,
        decision_outcome: dict[str, Any] | None = None,
        decided_at: dt.datetime | None = None,
    ) -> None:
        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_strategy_decisions (
                    session_id,
                    mint_id,
                    owner,
                    decision_type,
                    reason,
                    raw_event_fingerprint,
                    decision_inputs,
                    decision_outcome,
                    decided_at
                )
                VALUES (
                    NULLIF($1, ''), $2, NULLIF($3, ''), $4, $5, NULLIF($6, ''),
                    $7::jsonb, $8::jsonb, $9
                )
                """,
                session_id or "",
                mint_id,
                owner or "",
                decision_type,
                reason,
                raw_event_fingerprint or "",
                _jsonb_param(decision_inputs or {}),
                _jsonb_param(decision_outcome or {}),
                _ensure_utc(decided_at),
            )

        await self._with_connection(_operation)

    async def ensure_strategy_profile(
        self,
        *,
        profile_key: str,
        profile_name: str,
        version: str,
        definition: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        created_at: dt.datetime | None = None,
    ) -> None:
        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_strategy_profiles (
                    profile_key,
                    profile_name,
                    version,
                    definition,
                    metadata,
                    created_at
                )
                VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6)
                ON CONFLICT (profile_key) DO UPDATE
                SET profile_name = EXCLUDED.profile_name,
                    version = EXCLUDED.version,
                    definition = EXCLUDED.definition,
                    metadata = EXCLUDED.metadata
                """,
                profile_key,
                profile_name,
                version,
                _jsonb_param(definition),
                _jsonb_param(metadata or {}),
                _ensure_utc(created_at),
            )

        await self._with_connection(_operation)

    async def record_position_intent(
        self,
        *,
        session_id: str,
        mint_id: str,
        owner: str | None,
        strategy_profile: str | None,
        status: str,
        intended_size_lamports: int | None = None,
        intended_token_amount: int | None = None,
        context: dict[str, Any] | None = None,
        opened_at: dt.datetime | None = None,
    ) -> None:
        timestamp = _ensure_utc(opened_at)

        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_position_journal (
                    session_id,
                    mint_id,
                    owner,
                    strategy_profile,
                    status,
                    intended_size_lamports,
                    intended_token_amount,
                    journal_context,
                    opened_at,
                    updated_at
                )
                VALUES (
                    $1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6, $7, $8::jsonb, $9, $9
                )
                ON CONFLICT (session_id) DO UPDATE
                SET strategy_profile = COALESCE(NULLIF($4, ''), phase2_position_journal.strategy_profile),
                    status = EXCLUDED.status,
                    intended_size_lamports = COALESCE($6, phase2_position_journal.intended_size_lamports),
                    intended_token_amount = COALESCE($7, phase2_position_journal.intended_token_amount),
                    journal_context = phase2_position_journal.journal_context || $8::jsonb,
                    updated_at = $9
                """,
                session_id,
                mint_id,
                owner or "",
                strategy_profile or "",
                status,
                intended_size_lamports,
                intended_token_amount,
                _jsonb_param(context or {}),
                timestamp,
            )

        await self._with_connection(_operation)

    async def update_position_entry_fill(
        self,
        *,
        session_id: str,
        status: str,
        executed_size_lamports: int | None = None,
        executed_token_amount: int | None = None,
        actual_buy_price: str | None = None,
        context: dict[str, Any] | None = None,
        updated_at: dt.datetime | None = None,
    ) -> None:
        async def _operation(conn):
            await conn.execute(
                """
                UPDATE phase2_position_journal
                SET status = $2,
                    executed_size_lamports = COALESCE($3, executed_size_lamports),
                    executed_token_amount = COALESCE($4, executed_token_amount),
                    actual_buy_price = COALESCE(NULLIF($5, ''), actual_buy_price),
                    journal_context = journal_context || $6::jsonb,
                    updated_at = $7
                WHERE session_id = $1
                """,
                session_id,
                status,
                executed_size_lamports,
                executed_token_amount,
                actual_buy_price or "",
                _jsonb_param(context or {}),
                _ensure_utc(updated_at),
            )

        await self._with_connection(_operation)

    async def close_position_journal(
        self,
        *,
        session_id: str,
        status: str,
        exit_reason: str | None = None,
        realized_profit_lamports: int | None = None,
        context: dict[str, Any] | None = None,
        closed_at: dt.datetime | None = None,
    ) -> None:
        timestamp = _ensure_utc(closed_at)

        async def _operation(conn):
            await conn.execute(
                """
                UPDATE phase2_position_journal
                SET status = $2,
                    exit_reason = COALESCE(NULLIF($3, ''), exit_reason),
                    realized_profit_lamports = COALESCE($4, realized_profit_lamports),
                    journal_context = journal_context || $5::jsonb,
                    updated_at = $6,
                    closed_at = $6
                WHERE session_id = $1
                """,
                session_id,
                status,
                exit_reason or "",
                realized_profit_lamports,
                _jsonb_param(context or {}),
                timestamp,
            )

        await self._with_connection(_operation)

    async def record_risk_event(
        self,
        *,
        session_id: str | None,
        mint_id: str,
        owner: str | None,
        event_type: str,
        severity: str,
        detail: str,
        context: dict[str, Any] | None = None,
        created_at: dt.datetime | None = None,
    ) -> None:
        async def _operation(conn):
            await conn.execute(
                """
                INSERT INTO phase2_risk_events (
                    session_id,
                    mint_id,
                    owner,
                    event_type,
                    severity,
                    detail,
                    event_context,
                    created_at
                )
                VALUES (
                    NULLIF($1, ''), $2, NULLIF($3, ''), $4, $5, $6, $7::jsonb, $8
                )
                """,
                session_id or "",
                mint_id,
                owner or "",
                event_type,
                severity,
                detail,
                _jsonb_param(context or {}),
                _ensure_utc(created_at),
            )

        await self._with_connection(_operation)

    async def fetch_strategy_profiles(self) -> list[dict[str, Any]]:
        async def _operation(conn):
            rows = await conn.fetch(
                """
                SELECT profile_key, profile_name, version, definition, metadata, created_at
                FROM phase2_strategy_profiles
                ORDER BY created_at DESC, profile_name ASC
                """
            )
            return [dict(row) for row in rows]

        return await self._with_connection(_operation)

    async def record_backtest_run(
        self,
        *,
        profile_key: str,
        input_source: str,
        report: dict[str, Any],
        run_id: str | None = None,
    ) -> str:
        run_id = run_id or f"backtest_{uuid.uuid4().hex}"
        trades = list(report.get("trades") or [])

        async def _operation(conn):
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO phase2_backtest_runs (
                        run_id,
                        profile_key,
                        input_source,
                        summary_payload,
                        created_at
                    )
                    VALUES ($1, $2, $3, $4::jsonb, $5)
                    """,
                    run_id,
                    profile_key,
                    input_source,
                    _jsonb_param(report),
                    _utc_now(),
                )
                for trade in trades:
                    await conn.execute(
                        """
                        INSERT INTO phase2_backtest_trades (
                            run_id,
                            mint_id,
                            owner,
                            entered,
                            score,
                            realized_return_pct,
                            trade_payload
                        )
                        VALUES ($1, $2, NULLIF($3, ''), $4, $5, $6, $7::jsonb)
                        """,
                        run_id,
                        str(trade.get("mint_id") or ""),
                        str(trade.get("owner") or ""),
                        bool(trade.get("entered", False)),
                        float(trade.get("score", 0.0) or 0.0),
                        float(trade.get("realized_return_pct", 0.0) or 0.0),
                        _jsonb_param(trade),
                    )
            return run_id

        return await self._with_connection(_operation)

    async def fetch_runtime_overview(self) -> dict[str, Any]:
        async def _operation(conn):
            open_sessions = await conn.fetchval(
                "SELECT COUNT(*) FROM phase2_trade_sessions WHERE status = 'open'"
            )
            pending_orders = await conn.fetchval(
                "SELECT COUNT(*) FROM phase2_orders WHERE status IN ('attempting', 'submitted')"
            )
            risk_events = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM phase2_risk_events
                WHERE created_at >= $1
                """,
                _utc_now() - dt.timedelta(hours=24),
            )
            return {
                "open_sessions": int(open_sessions or 0),
                "pending_orders": int(pending_orders or 0),
                "recent_risk_events": int(risk_events or 0),
            }

        return await self._with_connection(_operation)

    async def fetch_active_sessions(self, *, limit: int = 20) -> list[dict[str, Any]]:
        async def _operation(conn):
            rows = await conn.fetch(
                """
                SELECT
                    s.session_id,
                    s.mint_id,
                    s.owner,
                    s.runtime_mode,
                    s.open_reason,
                    s.opened_at,
                    COALESCE(pj.status, s.status) AS status,
                    pj.actual_buy_price,
                    pj.executed_token_amount,
                    pj.journal_context
                FROM phase2_trade_sessions s
                LEFT JOIN phase2_position_journal pj ON pj.session_id = s.session_id
                WHERE s.status = 'open'
                ORDER BY s.opened_at DESC
                LIMIT $1
                """,
                int(limit),
            )
            results: list[dict[str, Any]] = []
            for row in rows:
                context = row.get("journal_context") or {}
                if isinstance(context, str):
                    try:
                        context = json.loads(context)
                    except Exception:
                        context = {}
                results.append(
                    {
                        "session_id": row["session_id"],
                        "mint_id": row["mint_id"],
                        "owner": row["owner"],
                        "runtime_mode": row["runtime_mode"],
                        "status": row["status"],
                        "price": row["actual_buy_price"],
                        "token_balance": row["executed_token_amount"],
                        "market": context.get("market", "pump_fun"),
                    }
                )
            return results

        return await self._with_connection(_operation)

    async def fetch_recent_activity(self, *, limit: int = 20) -> list[dict[str, Any]]:
        limit = int(limit)

        async def _operation(conn):
            fills = [
                {
                    "timestamp": row["filled_at"],
                    "kind": "fill",
                    "mint_id": row["mint_id"],
                    "side": row["side"],
                    "tx_id": row["tx_id"],
                }
                for row in await conn.fetch(
                    """
                    SELECT mint_id, side, tx_id, filled_at
                    FROM phase2_fills
                    ORDER BY filled_at DESC, fill_id DESC
                    LIMIT $1
                    """,
                    limit,
                )
            ]
            decisions = [
                {
                    "timestamp": row["decided_at"],
                    "kind": "decision",
                    "mint_id": row["mint_id"],
                    "decision_type": row["decision_type"],
                    "reason": row["reason"],
                }
                for row in await conn.fetch(
                    """
                    SELECT mint_id, decision_type, reason, decided_at
                    FROM phase2_strategy_decisions
                    ORDER BY decided_at DESC, decision_id DESC
                    LIMIT $1
                    """,
                    limit,
                )
            ]
            risks = [
                {
                    "timestamp": row["created_at"],
                    "kind": "risk",
                    "mint_id": row["mint_id"],
                    "status": row["severity"],
                    "reason": row["detail"],
                }
                for row in await conn.fetch(
                    """
                    SELECT mint_id, severity, detail, created_at
                    FROM phase2_risk_events
                    ORDER BY created_at DESC, event_id DESC
                    LIMIT $1
                    """,
                    limit,
                )
            ]
            merged = fills + decisions + risks
            merged.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
            return merged[:limit]

        return await self._with_connection(_operation)

    async def fetch_top_creators(self, *, limit: int = 10) -> list[dict[str, Any]]:
        async def _operation(conn):
            generation = await conn.fetchrow(
                """
                SELECT version
                FROM phase2_leaderboard_generations
                ORDER BY generated_at DESC
                LIMIT 1
                """
            )
            if generation is None:
                return []
            rows = await conn.fetch(
                """
                SELECT creator, rank, performance_score, trust_factor, entry_payload
                FROM phase2_creator_snapshots
                WHERE leaderboard_version = $1
                ORDER BY rank ASC
                LIMIT $2
                """,
                generation["version"],
                int(limit),
            )
            return [dict(row) for row in rows]

        return await self._with_connection(_operation)

    async def fetch_watchlist_candidates(self, *, limit: int = 10) -> list[dict[str, Any]]:
        async def _operation(conn):
            rows = await conn.fetch(
                """
                SELECT *
                FROM (
                    SELECT DISTINCT ON (mint_id)
                        mint_id,
                        owner,
                        lifecycle_state,
                        current_price,
                        market_cap,
                        recorded_at
                    FROM phase2_mint_snapshots
                    WHERE lifecycle_state IN ('active', 'migrated')
                    ORDER BY mint_id, recorded_at DESC, snapshot_id DESC
                ) AS latest_mints
                ORDER BY recorded_at DESC
                LIMIT $1
                """,
                int(limit),
            )
            return [dict(row) for row in rows]

        return await self._with_connection(_operation)

    async def fetch_recent_failures(self, *, limit: int = 20) -> list[dict[str, Any]]:
        async def _operation(conn):
            rows = await conn.fetch(
                """
                SELECT *
                FROM phase2_risk_events
                WHERE severity IN ('warn', 'error', 'critical')
                ORDER BY created_at DESC, event_id DESC
                LIMIT $1
                """,
                int(limit),
            )
            return [dict(row) for row in rows]

        return await self._with_connection(_operation)

    async def prune_raw_event_retention(self, *, force: bool = False) -> int:
        if self.config is None:
            return 0

        now = _utc_now()
        if not force and self._last_retention_prune is not None:
            if (now - self._last_retention_prune) < dt.timedelta(minutes=30):
                return 0

        retention_days = _default_retention_days(self.config)
        cutoff = now - dt.timedelta(days=retention_days)

        async def _operation(conn):
            result = await conn.execute(
                "DELETE FROM phase2_raw_events WHERE observed_at < $1",
                cutoff,
            )
            return int(result.split()[-1])

        deleted = await self._with_connection(_operation)
        self._last_retention_prune = now
        return deleted

    async def prune_mint_snapshot_retention(self, *, force: bool = False) -> int:
        if self.config is None:
            return 0

        now = _utc_now()
        if not force and self._last_snapshot_prune is not None:
            interval_seconds = _default_maintenance_interval_seconds(self.config)
            if (now - self._last_snapshot_prune) < dt.timedelta(seconds=interval_seconds):
                return 0

        keep_per_mint = _default_mint_snapshot_retention_per_mint(self.config)

        async def _operation(conn):
            result = await conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        snapshot_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY mint_id, lifecycle_state
                            ORDER BY recorded_at DESC, snapshot_id DESC
                        ) AS rank_index
                    FROM phase2_mint_snapshots
                )
                DELETE FROM phase2_mint_snapshots AS snapshots
                USING ranked
                WHERE snapshots.snapshot_id = ranked.snapshot_id
                  AND ranked.rank_index > $1
                """,
                int(keep_per_mint),
            )
            return int(result.split()[-1])

        deleted = await self._with_connection(_operation)
        self._last_snapshot_prune = now
        return deleted

    async def compact_mint_snapshot_storage(self) -> int:
        async def _operation(conn):
            result = await conn.execute(
                """
                UPDATE phase2_mint_snapshots
                SET holders = '{}'::jsonb,
                    price_history = '{}'::jsonb,
                    snapshot_payload = jsonb_strip_nulls(
                        jsonb_build_object(
                            'mint_sig', snapshot_payload->'mint_sig',
                            'created_at', snapshot_payload->'created_at',
                            'holder_count',
                                CASE
                                    WHEN jsonb_typeof(holders) = 'object' THEN (
                                        SELECT COUNT(*) FROM jsonb_each(holders)
                                    )
                                    ELSE 0
                                END,
                            'price_point_count',
                                CASE
                                    WHEN jsonb_typeof(price_history) = 'object' THEN (
                                        SELECT COUNT(*) FROM jsonb_each(price_history)
                                    )
                                    ELSE 0
                                END,
                            'swap_count', COALESCE((tx_counts->>'swaps')::int, 0),
                            'buy_count', COALESCE((tx_counts->>'buys')::int, 0),
                            'sell_count', COALESCE((tx_counts->>'sells')::int, 0)
                        )
                    )
                WHERE holders <> '{}'::jsonb
                   OR price_history <> '{}'::jsonb
                   OR snapshot_payload ? 'holders'
                   OR snapshot_payload ? 'price_history'
                """
            )
            return int(result.split()[-1])

        return await self._with_connection(_operation)

    async def database_size_bytes(self) -> int:
        async def _operation(conn):
            return int(await conn.fetchval("SELECT pg_database_size(current_database())"))

        return await self._with_connection(_operation)

    async def vacuum_tables(self, tables: list[str], *, full: bool = False) -> None:
        if not tables:
            return

        now = _utc_now()
        if not full and self._last_vacuum_at is not None:
            interval_seconds = max(300, _default_maintenance_interval_seconds(self.config))
            if (now - self._last_vacuum_at) < dt.timedelta(seconds=interval_seconds):
                return

        vacuum_prefix = "VACUUM (FULL, ANALYZE)" if full else "VACUUM (ANALYZE)"

        async def _operation(conn):
            for table in tables:
                await conn.execute(f"{vacuum_prefix} {table}")

        await self._with_connection(_operation)
        self._last_vacuum_at = now

    async def fetch_session_bundle(
        self,
        *,
        session_id: str | None = None,
        mint_id: str | None = None,
    ) -> dict[str, Any]:
        if not session_id and not mint_id:
            raise ValueError("session_id or mint_id is required for replay/export lookup.")

        async def _operation(conn):
            if session_id:
                session_row = await conn.fetchrow(
                    "SELECT * FROM phase2_trade_sessions WHERE session_id = $1",
                    session_id,
                )
            else:
                session_row = await conn.fetchrow(
                    """
                    SELECT *
                    FROM phase2_trade_sessions
                    WHERE mint_id = $1
                    ORDER BY opened_at DESC
                    LIMIT 1
                    """,
                    mint_id,
                )

            if session_row is None:
                return {
                    "session": None,
                    "orders": [],
                    "fills": [],
                    "holdings": [],
                    "decisions": [],
                    "position_journal": None,
                    "risk_events": [],
                    "strategy_profile": None,
                    "raw_events": [],
                    "leaderboard_generation": None,
                    "leaderboard_entries": [],
                }

            session = dict(session_row)
            leaderboard_generation = None
            leaderboard_entries: list[dict[str, Any]] = []
            position_journal = await conn.fetchrow(
                """
                SELECT *
                FROM phase2_position_journal
                WHERE session_id = $1
                """,
                session["session_id"],
            )
            risk_events = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT *
                    FROM phase2_risk_events
                    WHERE session_id = $1
                    ORDER BY created_at ASC, event_id ASC
                    """,
                    session["session_id"],
                )
            ]
            strategy_profile = None
            if position_journal and position_journal.get("strategy_profile"):
                strategy_profile = await conn.fetchrow(
                    """
                    SELECT *
                    FROM phase2_strategy_profiles
                    WHERE profile_key = $1
                    """,
                    position_journal["strategy_profile"],
                )
            if session.get("leaderboard_version"):
                leaderboard_generation = await conn.fetchrow(
                    "SELECT * FROM phase2_leaderboard_generations WHERE version = $1",
                    session["leaderboard_version"],
                )
                leaderboard_entries = [
                    dict(row)
                    for row in await conn.fetch(
                        """
                        SELECT *
                        FROM phase2_creator_snapshots
                        WHERE leaderboard_version = $1
                        ORDER BY rank ASC
                        """,
                        session["leaderboard_version"],
                    )
                ]

            decisions = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT *
                    FROM phase2_strategy_decisions
                    WHERE session_id = $1
                    ORDER BY decided_at ASC, decision_id ASC
                    """,
                    session["session_id"],
                )
            ]
            orders = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT *
                    FROM phase2_orders
                    WHERE session_id = $1
                    ORDER BY created_at ASC, order_id ASC
                    """,
                    session["session_id"],
                )
            ]
            fills = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT *
                    FROM phase2_fills
                    WHERE session_id = $1
                    ORDER BY filled_at ASC, fill_id ASC
                    """,
                    session["session_id"],
                )
            ]
            holdings = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT *
                    FROM phase2_holding_snapshots
                    WHERE session_id = $1
                    ORDER BY recorded_at ASC, snapshot_id ASC
                    """,
                    session["session_id"],
                )
            ]

            session_opened = session.get("opened_at") or _utc_now()
            session_closed = session.get("closed_at") or _utc_now()
            raw_events = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT *
                    FROM phase2_raw_events
                    WHERE mint_id = $1
                      AND observed_at >= $2
                      AND observed_at <= $3
                    ORDER BY observed_at ASC, log_index ASC
                    """,
                    session["mint_id"],
                    session_opened - dt.timedelta(minutes=1),
                    session_closed + dt.timedelta(minutes=1),
                )
            ]
            return {
                "session": session,
                "orders": orders,
                "fills": fills,
                "holdings": holdings,
                "decisions": decisions,
                "position_journal": dict(position_journal) if position_journal else None,
                "risk_events": risk_events,
                "strategy_profile": dict(strategy_profile) if strategy_profile else None,
                "raw_events": raw_events,
                "leaderboard_generation": dict(leaderboard_generation) if leaderboard_generation else None,
                "leaderboard_entries": leaderboard_entries,
            }

        return await self._with_connection(_operation)

    async def export_dataset(
        self,
        *,
        kind: str,
        output_path: Path,
        session_id: str | None = None,
        mint_id: str | None = None,
        leaderboard_version: str | None = None,
        limit: int | None = None,
    ) -> int:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        limit = None if limit is None or limit <= 0 else int(limit)

        async def _operation(conn):
            if kind == "sessions":
                query = "SELECT * FROM phase2_trade_sessions"
                params: list[Any] = []
                if session_id:
                    query += " WHERE session_id = $1"
                    params.append(session_id)
                elif mint_id:
                    query += " WHERE mint_id = $1"
                    params.append(mint_id)
                query += " ORDER BY opened_at DESC"
                if limit:
                    query += f" LIMIT {limit}"
                rows = await conn.fetch(query, *params)
            elif kind == "raw_events":
                query = "SELECT * FROM phase2_raw_events"
                params = []
                if mint_id:
                    query += " WHERE mint_id = $1"
                    params.append(mint_id)
                query += " ORDER BY observed_at DESC"
                if limit:
                    query += f" LIMIT {limit}"
                rows = await conn.fetch(query, *params)
            elif kind == "leaderboard":
                if leaderboard_version:
                    rows = await conn.fetch(
                        """
                        SELECT *
                        FROM phase2_creator_snapshots
                        WHERE leaderboard_version = $1
                        ORDER BY rank ASC
                        """,
                        leaderboard_version,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT *
                        FROM phase2_leaderboard_generations
                        ORDER BY generated_at DESC
                        LIMIT $1
                        """,
                        limit or 50,
                    )
            elif kind == "positions":
                query = "SELECT * FROM phase2_position_journal ORDER BY updated_at DESC"
                if limit:
                    query += f" LIMIT {limit}"
                rows = await conn.fetch(query)
            elif kind == "risk_events":
                query = "SELECT * FROM phase2_risk_events ORDER BY created_at DESC"
                if limit:
                    query += f" LIMIT {limit}"
                rows = await conn.fetch(query)
            elif kind == "strategy_profiles":
                query = "SELECT * FROM phase2_strategy_profiles ORDER BY created_at DESC"
                if limit:
                    query += f" LIMIT {limit}"
                rows = await conn.fetch(query)
            else:
                raise ValueError(f"Unsupported export kind: {kind}")
            return [dict(row) for row in rows]

        rows = await self._with_connection(_operation)
        with output_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(_json_safe(row), sort_keys=True) + "\n")
        return len(rows)

    async def fetch_operator_snapshot(self, *, limit: int = 5) -> dict[str, Any]:
        limit = max(1, int(limit))

        async def _operation(conn):
            latest_version = await conn.fetchval(
                """
                SELECT version
                FROM phase2_leaderboard_generations
                ORDER BY generated_at DESC
                LIMIT 1
                """
            )
            top_creators: list[dict[str, Any]] = []
            if latest_version:
                top_creators = [
                    dict(row)
                    for row in await conn.fetch(
                        """
                        SELECT creator, rank, performance_score, trust_factor, median_success_ratio
                        FROM phase2_creator_snapshots
                        WHERE leaderboard_version = $1
                        ORDER BY rank ASC
                        LIMIT $2
                        """,
                        latest_version,
                        limit,
                    )
                ]
            active_sessions = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT session_id, mint_id, owner, status, opened_at, close_reason
                    FROM phase2_trade_sessions
                    WHERE status = 'open'
                    ORDER BY opened_at DESC
                    LIMIT $1
                    """,
                    limit,
                )
            ]
            recent_positions = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT *
                    FROM phase2_position_journal
                    ORDER BY updated_at DESC
                    LIMIT $1
                    """,
                    limit,
                )
            ]
            recent_risk_events = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT *
                    FROM phase2_risk_events
                    ORDER BY created_at DESC
                    LIMIT $1
                    """,
                    limit,
                )
            ]
            recent_fills = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT session_id, mint_id, side, tx_id, wallet_delta_lamports, filled_at
                    FROM phase2_fills
                    ORDER BY filled_at DESC
                    LIMIT $1
                    """,
                    limit,
                )
            ]
            return {
                "leaderboard_version": latest_version,
                "top_creators": top_creators,
                "active_sessions": active_sessions,
                "recent_positions": recent_positions,
                "recent_risk_events": recent_risk_events,
                "recent_fills": recent_fills,
            }

        return await self._with_connection(_operation)


class ReplayEngine:
    def __init__(self, repository: SessionReplayRepository):
        self.repository = repository

    async def replay_session(
        self,
        *,
        session_id: str | None = None,
        mint_id: str | None = None,
    ) -> dict[str, Any]:
        bundle = await self.repository.fetch_session_bundle(session_id=session_id, mint_id=mint_id)
        session = bundle.get("session")
        if not session:
            raise ValueError("No Phase 2 session was found for the requested identifier.")

        fills = bundle.get("fills", [])
        decisions = bundle.get("decisions", [])
        holdings = bundle.get("holdings", [])
        position_journal = bundle.get("position_journal")
        risk_events = bundle.get("risk_events", [])
        strategy_profile = bundle.get("strategy_profile")
        raw_events = bundle.get("raw_events", [])
        orders = bundle.get("orders", [])

        buy_fill = next((fill for fill in fills if fill.get("side") == "buy"), None)
        sell_fill = next((fill for fill in reversed(fills) if fill.get("side") == "sell"), None)

        invariants: list[ReplayInvariant] = []
        if buy_fill and sell_fill:
            expected_profit = int(
                (sell_fill.get("wallet_delta_lamports") or sell_fill.get("proceeds_lamports") or 0)
                - (buy_fill.get("cost_lamports") or 0)
            )
            recorded_profit = session.get("realized_profit_lamports")
            if recorded_profit is None:
                invariants.append(
                    ReplayInvariant(
                        name="realized_profit_recorded",
                        status="warn",
                        detail="Replay derived realized profit, but the session summary did not store it.",
                    )
                )
            elif int(recorded_profit) == expected_profit:
                invariants.append(
                    ReplayInvariant(
                        name="realized_profit_matches_fills",
                        status="pass",
                        detail=f"sell wallet delta - buy cost = {expected_profit} lamports.",
                    )
                )
            else:
                invariants.append(
                    ReplayInvariant(
                        name="realized_profit_matches_fills",
                        status="fail",
                        detail=(
                            f"Session stored {recorded_profit} lamports but replay derived "
                            f"{expected_profit} lamports."
                        ),
                    )
                )
        else:
            invariants.append(
                ReplayInvariant(
                    name="fills_present",
                    status="warn",
                    detail="Replay did not find both a buy fill and a sell fill for this session.",
                )
            )

        if session.get("leaderboard_version"):
            invariants.append(
                ReplayInvariant(
                    name="leaderboard_version_pinned",
                    status="pass",
                    detail=f"Session is pinned to leaderboard version {session['leaderboard_version']}.",
                )
            )
        else:
            invariants.append(
                ReplayInvariant(
                    name="leaderboard_version_pinned",
                    status="warn",
                    detail="Session is missing a leaderboard version reference.",
                )
            )

        decision_types = {decision.get("decision_type") for decision in decisions}
        if {"buy", "sell"} & decision_types:
            invariants.append(
                ReplayInvariant(
                    name="decision_journal_present",
                    status="pass",
                    detail=f"Recorded decisions: {', '.join(sorted(filter(None, decision_types)))}.",
                )
            )
        else:
            invariants.append(
                ReplayInvariant(
                    name="decision_journal_present",
                    status="warn",
                    detail="Replay found no buy/sell decision rows for the session.",
                )
            )

        if position_journal:
            invariants.append(
                ReplayInvariant(
                    name="position_journal_present",
                    status="pass",
                    detail=f"Position journal status={position_journal.get('status')}.",
                )
            )
        else:
            invariants.append(
                ReplayInvariant(
                    name="position_journal_present",
                    status="warn",
                    detail="Replay found no position journal row for the session.",
                )
            )

        if strategy_profile:
            invariants.append(
                ReplayInvariant(
                    name="strategy_profile_pinned",
                    status="pass",
                    detail=(
                        f"Session uses strategy profile {strategy_profile.get('profile_name')} "
                        f"({strategy_profile.get('version')})."
                    ),
                )
            )
        else:
            invariants.append(
                ReplayInvariant(
                    name="strategy_profile_pinned",
                    status="warn",
                    detail="Replay found no pinned strategy profile for the session.",
                )
            )

        timeline = []
        for event in raw_events:
            timeline.append(
                {
                    "timestamp": event.get("observed_at"),
                    "kind": "raw_event",
                    "event_type": event.get("event_type"),
                    "fingerprint": event.get("fingerprint"),
                }
            )
        for decision in decisions:
            timeline.append(
                {
                    "timestamp": decision.get("decided_at"),
                    "kind": "decision",
                    "decision_type": decision.get("decision_type"),
                    "reason": decision.get("reason"),
                }
            )
        for order in orders:
            timeline.append(
                {
                    "timestamp": order.get("created_at"),
                    "kind": "order",
                    "side": order.get("side"),
                    "status": order.get("status"),
                    "tx_id": order.get("tx_id"),
                }
            )
        for fill in fills:
            timeline.append(
                {
                    "timestamp": fill.get("filled_at"),
                    "kind": "fill",
                    "side": fill.get("side"),
                    "tx_id": fill.get("tx_id"),
                }
            )
        for holding in holdings:
            timeline.append(
                {
                    "timestamp": holding.get("recorded_at"),
                    "kind": "holding",
                    "status": holding.get("status"),
                    "token_balance": holding.get("token_balance"),
                }
            )
        for event in risk_events:
            timeline.append(
                {
                    "timestamp": event.get("created_at"),
                    "kind": "risk_event",
                    "event_type": event.get("event_type"),
                    "severity": event.get("severity"),
                    "detail": event.get("detail"),
                }
            )
        timeline.sort(key=lambda item: (str(item.get("timestamp") or ""), item["kind"]))

        return {
            "session": _json_safe(session),
            "strategy_profile": _json_safe(strategy_profile),
            "position_journal": _json_safe(position_journal),
            "leaderboard_generation": _json_safe(bundle.get("leaderboard_generation")),
            "leaderboard_entries": _json_safe(bundle.get("leaderboard_entries", [])),
            "counts": {
                "raw_events": len(raw_events),
                "decisions": len(decisions),
                "orders": len(orders),
                "fills": len(fills),
                "holdings": len(holdings),
                "risk_events": len(risk_events),
            },
            "invariants": [_json_safe(invariant.__dict__) for invariant in invariants],
            "timeline": _json_safe(timeline),
        }


def render_replay_report(report: dict[str, Any]) -> str:
    session = report["session"]
    lines = [
        f"session_id={session['session_id']} mint_id={session['mint_id']} owner={session['owner']}",
        f"status={session.get('status')} opened_at={session.get('opened_at')} closed_at={session.get('closed_at')}",
        (
            "counts "
            f"raw_events={report['counts']['raw_events']} "
            f"decisions={report['counts']['decisions']} "
            f"orders={report['counts']['orders']} "
            f"fills={report['counts']['fills']} "
            f"holdings={report['counts']['holdings']} "
            f"risk_events={report['counts'].get('risk_events', 0)}"
        ),
    ]
    for invariant in report["invariants"]:
        lines.append(f"{invariant['status'].upper():<4} {invariant['name']}: {invariant['detail']}")
    return "\n".join(lines)


async def run_replay(
    db_dsn: str,
    *,
    session_id: str | None = None,
    mint_id: str | None = None,
    as_json: bool = False,
) -> str:
    store = Phase2Store(db_dsn)
    engine = ReplayEngine(store)
    report = await engine.replay_session(session_id=session_id, mint_id=mint_id)
    if as_json:
        return json.dumps(_json_safe(report), indent=2, sort_keys=True)
    return render_replay_report(report)


async def run_export(
    db_dsn: str,
    *,
    config: Any,
    kind: str,
    output_path: str | None = None,
    session_id: str | None = None,
    mint_id: str | None = None,
    leaderboard_version: str | None = None,
    limit: int | None = None,
) -> tuple[Path, int]:
    store = Phase2Store(db_dsn, config=config)
    export_dir = _default_export_dir(config)
    export_dir.mkdir(parents=True, exist_ok=True)
    path = Path(output_path) if output_path else export_dir / f"{kind}-{_utc_now().strftime('%Y%m%dT%H%M%SZ')}.jsonl"
    count = await store.export_dataset(
        kind=kind,
        output_path=path,
        session_id=session_id,
        mint_id=mint_id,
        leaderboard_version=leaderboard_version,
        limit=limit,
    )
    return path, count
