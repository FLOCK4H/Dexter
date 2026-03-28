from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
import logging
import os
from pathlib import Path
import re
from typing import Literal
from urllib.parse import quote, unquote, urlsplit

from dexter_mev import MevConfig, mev_tip_lamports_from_sol, normalize_mev_provider

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args, **_kwargs) -> None:
        return None

RuntimeMode = Literal["read_only", "paper", "simulate", "live"]
Component = Literal["collector", "trader", "doctor", "database", "analyze"]
ExecutionMode = Literal["read_only", "paper", "simulate", "live"]
NetworkName = Literal["mainnet", "devnet"]

CODE_ROOT = Path(__file__).resolve().parent


def _looks_like_workspace(path: Path) -> bool:
    return (
        path.is_dir()
        and (path / ".env.example").exists()
        and ((path / "Dexter.py").exists() or (path / "pyproject.toml").exists())
    )


def _resolve_project_root() -> Path:
    override = os.getenv("DEXTER_HOME") or os.getenv("DEXTER_PROJECT_ROOT")
    if override:
        return Path(override).expanduser().resolve()

    cwd = Path.cwd()
    if _looks_like_workspace(cwd):
        return cwd.resolve()

    return CODE_ROOT


PROJECT_ROOT = _resolve_project_root()
ENV_PATH = PROJECT_ROOT / ".env"
DEFAULT_BACKUP_DIR = PROJECT_ROOT.parent / "dexter_backups"
VALID_RUNTIME_MODES = {"read_only", "paper", "simulate", "live"}
VALID_NETWORKS = {"mainnet", "devnet"}
DATASTORE_ENV_ALIASES = {
    "DEXTER_DATASTORE_ENABLED": ("DEXTER_PHASE2_ENABLED",),
    "DEXTER_DATASTORE_RAW_EVENT_RETENTION_DAYS": ("DEXTER_PHASE2_RAW_EVENT_RETENTION_DAYS",),
    "DEXTER_DATASTORE_EXPORT_DIR": ("DEXTER_PHASE2_EXPORT_DIR",),
}
DEVNET_RPC_ENDPOINTS = (
    "https://api.devnet.solana.com",
    "wss://api.devnet.solana.com",
)
DEFAULT_MAINNET_RPC_ENDPOINTS = (
    "https://api.mainnet-beta.solana.com",
    "wss://api.mainnet-beta.solana.com",
)
ENV_KEY_PATTERN = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$")


@dataclass(frozen=True)
class RuntimeConfig:
    mode: RuntimeMode
    network: NetworkName
    allow_mainnet_live: bool
    mainnet_dry_run: bool
    emergency_stop_file: Path
    close_positions_on_shutdown: bool
    enable_wslogs: bool = True


@dataclass(frozen=True)
class RpcConfig:
    http_url: str
    ws_url: str
    private_key: str


@dataclass(frozen=True)
class DatabaseConfig:
    dsn: str
    host: str
    port: int
    name: str
    user: str
    password: str
    min_pool_size: int
    max_pool_size: int
    admin_dsn: str
    admin_host: str
    admin_port: int
    admin_name: str
    admin_user: str
    admin_password: str


@dataclass(frozen=True)
class BackupConfig:
    enabled: bool
    directory: Path
    interval_seconds: int
    retention_count: int
    pg_dump_path: str


@dataclass(frozen=True)
class DataStoreConfig:
    # Dexter's normalized replay/export/dashboard data layer.
    enabled: bool
    raw_event_retention_days: int
    export_dir: Path
    mint_snapshot_interval_seconds: int
    mint_snapshot_retention_per_mint: int
    maintenance_interval_seconds: int
    max_database_size_bytes: int


Phase2Config = DataStoreConfig


@dataclass(frozen=True)
class RiskConfig:
    per_trade_sol_cap: Decimal
    session_sol_cap: Decimal
    # Gross spend cap for the UTC day across all entries.
    daily_sol_cap: Decimal
    max_concurrent_sessions: int
    per_creator_max_sessions: int
    wallet_reserve_floor_sol: Decimal
    # Realized-loss stop for the UTC day; different from daily_sol_cap.
    daily_drawdown_stop_sol: Decimal


@dataclass(frozen=True)
class StrategyConfig:
    default_profile: str
    min_entry_score_override: Decimal | None


@dataclass(frozen=True)
class ExecutionPolicyConfig:
    quote_retry_limit: int
    send_retry_limit: int
    confirmation_retry_limit: int
    retry_delay_seconds: Decimal


@dataclass(frozen=True)
class WalletConfig:
    # trading_private_key is the signer Dexter uses for create/buy/sell.
    # hot_private_key and treasury_address are currently informational labels for
    # runtime snapshots and future operator flows.
    trading_private_key: str
    hot_private_key: str
    treasury_address: str


@dataclass(frozen=True)
class AlertConfig:
    telegram_bot_token: str
    telegram_chat_id: str
    discord_webhook_url: str
    desktop_notifications: bool


@dataclass(frozen=True)
class PathConfig:
    log_dir: Path
    results_file: Path
    leaderboard_file: Path
    state_dir: Path
    trader_snapshot_file: Path
    collector_snapshot_file: Path
    operator_control_file: Path


@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    runtime: RuntimeConfig
    rpc: RpcConfig
    mev: MevConfig
    database: DatabaseConfig
    backup: BackupConfig
    phase2: DataStoreConfig
    risk: RiskConfig
    strategy: StrategyConfig
    execution: ExecutionPolicyConfig
    wallets: WalletConfig
    alerts: AlertConfig
    paths: PathConfig

    @property
    def data_store(self) -> DataStoreConfig:
        return self.phase2


def load_env_file() -> None:
    load_dotenv(dotenv_path=ENV_PATH, override=False)


def _decode_env_value(raw: str) -> str:
    value = raw.rstrip()
    if value and value[0] not in {'"', "'"}:
        comment_index = None
        for index, char in enumerate(value):
            if char != "#":
                continue
            if index == 0 or value[index - 1].isspace():
                comment_index = index
                break
        if comment_index is not None:
            value = value[:comment_index].rstrip()
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        quote_char = value[0]
        body = value[1:-1]
        if quote_char == '"':
            return bytes(body, "utf-8").decode("unicode_escape")
        return body
    return value


def _split_env_comment(raw: str) -> tuple[str, str]:
    value = raw.rstrip()
    if not value or value[0] in {'"', "'"}:
        return value, ""
    for index, char in enumerate(value):
        if char != "#":
            continue
        if index == 0 or value[index - 1].isspace():
            return value[:index].rstrip(), value[index - 1 if index > 0 and value[index - 1].isspace() else index :]
    return value, ""


def _encode_env_value(value: str) -> str:
    text = str(value)
    if not text:
        return ""
    if any(char.isspace() for char in text) or any(char in text for char in {'#', '"', "'", "\\"}):
        escaped = (
            text.replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
            .replace('"', '\\"')
        )
        return f'"{escaped}"'
    return text


def read_env_values(path: Path = ENV_PATH) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = ENV_KEY_PATTERN.match(line.strip())
        if not match:
            continue
        key, raw_value = match.groups()
        values[key] = _decode_env_value(raw_value)
    return values


def update_env_file(
    updates: dict[str, str | None],
    *,
    path: Path = ENV_PATH,
) -> Path:
    existing_lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    normalized_updates = {
        key: "" if value is None else str(value)
        for key, value in updates.items()
    }
    remaining = dict(normalized_updates)
    output_lines: list[str] = []

    for line in existing_lines:
        match = ENV_KEY_PATTERN.match(line.strip())
        if not match:
            output_lines.append(line)
            continue
        key, raw_value = match.groups()
        if key in remaining:
            _value, inline_comment = _split_env_comment(raw_value)
            rendered_value = _encode_env_value(remaining.pop(key))
            output_lines.append(f"{key}={rendered_value}{inline_comment}")
            continue
        output_lines.append(line)

    if remaining:
        if output_lines and output_lines[-1].strip():
            output_lines.append("")
        for key, value in remaining.items():
            output_lines.append(f"{key}={_encode_env_value(value)}")

    rendered = "\n".join(output_lines).rstrip()
    path.write_text(f"{rendered}\n" if rendered else "", encoding="utf-8")

    for key, value in normalized_updates.items():
        os.environ[key] = value
    load_config.cache_clear()
    return path


def _env(name: str, default: str = "") -> str:
    for candidate in (name, *DATASTORE_ENV_ALIASES.get(name, ())):
        raw = os.getenv(candidate)
        if raw is not None and raw.strip():
            return raw.strip()
    return default.strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if not raw:
        return default
    return int(raw)


def _env_decimal(name: str, default: str) -> Decimal:
    raw = _env(name, default)
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value for {name}: {raw}") from exc


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name)
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _build_postgres_dsn(user: str, password: str, host: str, port: int, database: str) -> str:
    if not user or not host or not database:
        return ""
    auth = quote(user, safe="")
    if password:
        auth = f"{auth}:{quote(password, safe='')}"
    return f"postgres://{auth}@{host}:{port}/{database}"


def _parse_dsn(dsn: str) -> tuple[str, int, str, str, str]:
    if not dsn:
        return ("", 5432, "", "", "")
    parsed = urlsplit(dsn)
    host = parsed.hostname or ""
    port = parsed.port or 5432
    name = parsed.path.lstrip("/")
    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    return host, port, name, user, password


def _resolve_network(network_override: str | None = None) -> NetworkName:
    raw_network = (network_override or _env("DEXTER_NETWORK", "mainnet")).lower()
    if raw_network not in VALID_NETWORKS:
        raise ValueError("DEXTER_NETWORK must be one of: devnet, mainnet")
    return raw_network  # type: ignore[return-value]


def _rpc_urls_for_network(network: NetworkName) -> tuple[str, str]:
    if network == "devnet":
        return DEVNET_RPC_ENDPOINTS
    return (
        _env("HTTP_URL", DEFAULT_MAINNET_RPC_ENDPOINTS[0]),
        _env("WS_URL", DEFAULT_MAINNET_RPC_ENDPOINTS[1]),
    )


def _path_is_within(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


@lru_cache(maxsize=16)
def load_config(mode_override: str | None = None, network_override: str | None = None) -> AppConfig:
    load_env_file()

    runtime_mode = (mode_override or _env("DEXTER_RUNTIME_MODE", "read_only")).lower()
    if runtime_mode not in VALID_RUNTIME_MODES:
        raise ValueError(
            "DEXTER_RUNTIME_MODE must be one of: read_only, paper, simulate, live"
        )

    network = _resolve_network(network_override)
    http_url, ws_url = _rpc_urls_for_network(network)
    private_key = _env("PRIVATE_KEY")
    trading_private_key = _env("DEXTER_TRADING_PRIVATE_KEY", private_key)
    hot_private_key = _env("DEXTER_HOT_PRIVATE_KEY", trading_private_key)
    mev_tip_sol, mev_tip_lamports = mev_tip_lamports_from_sol(_env("MEV_TIP", "0.00001"))

    database_url = _env("DATABASE_URL")
    if not database_url:
        database_url = _build_postgres_dsn(
            user=_env("DB_USER", "dexter_user"),
            password=_env("DB_PASSWORD"),
            host=_env("DB_HOST", "127.0.0.1"),
            port=_env_int("DB_PORT", 5432),
            database=_env("DB_NAME", "dexter_db"),
        )

    admin_dsn = _env("POSTGRES_ADMIN_DSN")
    if not admin_dsn:
        admin_dsn = _build_postgres_dsn(
            user=_env("POSTGRES_ADMIN_USER", "postgres"),
            password=_env("POSTGRES_ADMIN_PASSWORD"),
            host=_env("POSTGRES_ADMIN_HOST", _env("DB_HOST", "127.0.0.1")),
            port=_env_int("POSTGRES_ADMIN_PORT", _env_int("DB_PORT", 5432)),
            database=_env("POSTGRES_ADMIN_DB", "postgres"),
        )

    db_host, db_port, db_name, db_user, db_password = _parse_dsn(database_url)
    admin_host, admin_port, admin_name, admin_user, admin_password = _parse_dsn(admin_dsn)

    log_dir = Path(_env("DEXTER_LOG_DIR", str(PROJECT_ROOT / "dev" / "logs"))).expanduser()
    results_file = Path(_env("DEXTER_RESULTS_FILE", str(PROJECT_ROOT / "dev" / "results.txt"))).expanduser()
    leaderboard_file = Path(
        _env("DEXTER_LEADERBOARD_FILE", str(PROJECT_ROOT / "dev" / "leaderboard.txt"))
    ).expanduser()
    state_dir = Path(_env("DEXTER_STATE_DIR", str(PROJECT_ROOT / "dev" / "state"))).expanduser()
    trader_snapshot_file = Path(_env("DEXTER_TRADER_SNAPSHOT_FILE", str(state_dir / "trader-runtime.json"))).expanduser()
    collector_snapshot_file = Path(_env("DEXTER_COLLECTOR_SNAPSHOT_FILE", str(state_dir / "collector-runtime.json"))).expanduser()
    operator_control_file = Path(_env("DEXTER_OPERATOR_CONTROL_FILE", str(state_dir / "operator-control.json"))).expanduser()

    config = AppConfig(
        project_root=PROJECT_ROOT,
        runtime=RuntimeConfig(
            mode=runtime_mode,  # type: ignore[arg-type]
            network=network,
            allow_mainnet_live=_env_bool("DEXTER_ALLOW_MAINNET_LIVE", False),
            mainnet_dry_run=_env_bool("DEXTER_MAINNET_DRY_RUN", True),
            emergency_stop_file=Path(
                _env("DEXTER_EMERGENCY_STOP_FILE", str(PROJECT_ROOT / "dev" / "EMERGENCY_STOP"))
            ).expanduser(),
            close_positions_on_shutdown=_env_bool("DEXTER_CLOSE_POSITIONS_ON_SHUTDOWN", False),
            enable_wslogs=_env_bool("DEXTER_ENABLE_WSLOGS", True),
        ),
        rpc=RpcConfig(
            http_url=http_url,
            ws_url=ws_url,
            private_key=trading_private_key,
        ),
        mev=MevConfig(
            enabled=_env_bool("USE_MEV", False),
            provider=normalize_mev_provider(_env("MEV_PROVIDER", "jito")),
            tip_sol=mev_tip_sol,
            tip_lamports=mev_tip_lamports,
            jito_key=_env("MEV_JITO_KEY"),
            nextblock_key=_env("MEV_NEXTBLOCK_KEY"),
            zero_slot_key=_env("MEV_ZERO_SLOT_KEY"),
            temporal_key=_env("MEV_TEMPORAL_KEY"),
            bloxroute_key=_env("MEV_BLOXROUTE_KEY"),
        ),
        database=DatabaseConfig(
            dsn=database_url,
            host=db_host,
            port=db_port,
            name=db_name,
            user=db_user,
            password=db_password,
            min_pool_size=_env_int("DEXTER_DB_MIN_POOL_SIZE", 1),
            max_pool_size=_env_int("DEXTER_DB_MAX_POOL_SIZE", 20),
            admin_dsn=admin_dsn,
            admin_host=admin_host,
            admin_port=admin_port,
            admin_name=admin_name,
            admin_user=admin_user,
            admin_password=admin_password,
        ),
        backup=BackupConfig(
            enabled=_env_bool("DEXTER_BACKUP_ENABLED", True),
            directory=Path(_env("DEXTER_BACKUP_DIR", str(DEFAULT_BACKUP_DIR))).expanduser(),
            interval_seconds=_env_int("DEXTER_BACKUP_INTERVAL_SECONDS", 3600),
            retention_count=_env_int("DEXTER_BACKUP_RETENTION_COUNT", 24),
            pg_dump_path=_env("DEXTER_PG_DUMP_PATH", "pg_dump"),
        ),
        phase2=DataStoreConfig(
            enabled=_env_bool("DEXTER_DATASTORE_ENABLED", True),
            raw_event_retention_days=_env_int("DEXTER_DATASTORE_RAW_EVENT_RETENTION_DAYS", 30),
            export_dir=Path(
                _env("DEXTER_DATASTORE_EXPORT_DIR", str(PROJECT_ROOT / "dev" / "exports"))
            ).expanduser(),
            mint_snapshot_interval_seconds=_env_int("DEXTER_DATASTORE_MINT_SNAPSHOT_INTERVAL_SECONDS", 15),
            mint_snapshot_retention_per_mint=_env_int("DEXTER_DATASTORE_MINT_SNAPSHOT_RETENTION_PER_MINT", 4),
            maintenance_interval_seconds=_env_int("DEXTER_DATASTORE_MAINTENANCE_INTERVAL_SECONDS", 60),
            max_database_size_bytes=_env_int("DEXTER_DATASTORE_MAX_DATABASE_SIZE_BYTES", 2147483648),
        ),
        risk=RiskConfig(
            per_trade_sol_cap=_env_decimal("DEXTER_PER_TRADE_SOL_CAP", "100.00"),
            session_sol_cap=_env_decimal("DEXTER_SESSION_SOL_CAP", "1000.00"),
            daily_sol_cap=_env_decimal("DEXTER_DAILY_SOL_CAP", "10000.00"),
            max_concurrent_sessions=_env_int("DEXTER_MAX_CONCURRENT_SESSIONS", 100),
            per_creator_max_sessions=_env_int("DEXTER_PER_CREATOR_MAX_SESSIONS", 100),
            wallet_reserve_floor_sol=_env_decimal("DEXTER_WALLET_RESERVE_FLOOR_SOL", "0.00020398"),
            daily_drawdown_stop_sol=_env_decimal("DEXTER_DAILY_DRAWDOWN_STOP_SOL", "10000.00"),
        ),
        strategy=StrategyConfig(
            default_profile=_env("DEXTER_STRATEGY_PROFILE", "balanced").lower(),
            min_entry_score_override=(
                _env_decimal("DEXTER_MIN_ENTRY_SCORE_OVERRIDE", "0")
                if _env("DEXTER_MIN_ENTRY_SCORE_OVERRIDE")
                else None
            ),
        ),
        execution=ExecutionPolicyConfig(
            quote_retry_limit=_env_int("DEXTER_QUOTE_RETRY_LIMIT", 3),
            send_retry_limit=_env_int("DEXTER_SEND_RETRY_LIMIT", 2),
            confirmation_retry_limit=_env_int("DEXTER_CONFIRMATION_RETRY_LIMIT", 6),
            retry_delay_seconds=_env_decimal("DEXTER_RETRY_DELAY_SECONDS", "0.25"),
        ),
        wallets=WalletConfig(
            trading_private_key=trading_private_key,
            hot_private_key=hot_private_key,
            treasury_address=_env("DEXTER_TREASURY_ADDRESS"),
        ),
        alerts=AlertConfig(
            telegram_bot_token=_env("DEXTER_TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=_env("DEXTER_TELEGRAM_CHAT_ID"),
            discord_webhook_url=_env("DEXTER_DISCORD_WEBHOOK_URL"),
            desktop_notifications=_env_bool("DEXTER_DESKTOP_NOTIFICATIONS", False),
        ),
        paths=PathConfig(
            log_dir=log_dir,
            results_file=results_file,
            leaderboard_file=leaderboard_file,
            state_dir=state_dir,
            trader_snapshot_file=trader_snapshot_file,
            collector_snapshot_file=collector_snapshot_file,
            operator_control_file=operator_control_file,
        ),
    )

    return config


def resolve_trade_execution_mode(config: AppConfig) -> ExecutionMode:
    if config.runtime.mode != "live":
        return config.runtime.mode
    if config.runtime.network == "mainnet" and config.runtime.mainnet_dry_run:
        return "simulate"
    return "live"


def validate_config(config: AppConfig, component: Component) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    execution_mode = resolve_trade_execution_mode(config)

    if config.database.min_pool_size < 1:
        errors.append("DEXTER_DB_MIN_POOL_SIZE must be >= 1.")
    if config.database.max_pool_size < config.database.min_pool_size:
        errors.append("DEXTER_DB_MAX_POOL_SIZE must be >= DEXTER_DB_MIN_POOL_SIZE.")

    if component in {"collector", "trader", "doctor"}:
        if not config.rpc.http_url:
            errors.append("Resolved HTTP RPC URL is missing.")
        if not config.rpc.ws_url:
            errors.append("Resolved WebSocket RPC URL is missing.")

    if component in {"collector", "trader", "analyze", "doctor"} and not config.database.dsn:
        errors.append("DATABASE_URL or DB_* variables are required.")

    if component == "database":
        if not config.database.dsn:
            errors.append("DATABASE_URL or DB_* variables are required for the Dexter app database.")
        if not config.database.admin_dsn:
            warnings.append(
                "POSTGRES_ADMIN_DSN or POSTGRES_ADMIN_* variables are only required when Dexter must create the database or user."
            )

    if component == "trader":
        if execution_mode in {"simulate", "live"} and not config.rpc.private_key:
            errors.append("PRIVATE_KEY is required for simulate and live trader modes.")

    if component == "doctor" and not config.rpc.private_key:
        warnings.append("PRIVATE_KEY is not set; wallet decoding will be skipped.")

    if config.mev.enabled:
        if config.mev.tip_lamports <= 0:
            errors.append("MEV_TIP must resolve to a positive lamport value when USE_MEV=true.")
        if config.runtime.network != "mainnet":
            warnings.append("USE_MEV is enabled, but MEV routing is mainnet-only; Dexter will ignore it on devnet.")
        elif execution_mode == "live" and config.mev.requires_api_key() and not config.mev.active_provider_key():
            errors.append(f"{config.mev.provider} requires its MEV API key when USE_MEV=true.")

    if config.runtime.mode == "live" and config.runtime.network == "mainnet":
        if config.runtime.mainnet_dry_run:
            warnings.append(
                "Mainnet live runtime is in dry-run mode; transactions will be simulated and not submitted."
            )
        elif not config.runtime.allow_mainnet_live:
            errors.append(
                "Live mainnet transaction sends are locked. Set DEXTER_MAINNET_DRY_RUN=false and "
                "DEXTER_ALLOW_MAINNET_LIVE=true to unlock them explicitly."
            )

    if config.runtime.mode == "live" and not config.runtime.close_positions_on_shutdown:
        warnings.append(
            "DEXTER_CLOSE_POSITIONS_ON_SHUTDOWN=false; open live positions will require manual handling on shutdown."
        )

    if config.runtime.emergency_stop_file.exists():
        warnings.append(
            f"Emergency stop is active at {config.runtime.emergency_stop_file}; new trader buys will be blocked."
        )

    if config.backup.enabled:
        if config.backup.interval_seconds < 60:
            errors.append("DEXTER_BACKUP_INTERVAL_SECONDS must be at least 60.")
        if config.backup.retention_count < 1:
            errors.append("DEXTER_BACKUP_RETENTION_COUNT must be at least 1.")
        if _path_is_within(config.backup.directory, config.project_root):
            errors.append("DEXTER_BACKUP_DIR must be outside the repository root.")

    if config.phase2.enabled and config.phase2.raw_event_retention_days < 1:
        errors.append("DEXTER_DATASTORE_RAW_EVENT_RETENTION_DAYS must be at least 1.")
    if config.phase2.enabled and config.phase2.mint_snapshot_interval_seconds < 1:
        errors.append("DEXTER_DATASTORE_MINT_SNAPSHOT_INTERVAL_SECONDS must be at least 1.")
    if config.phase2.enabled and config.phase2.mint_snapshot_retention_per_mint < 1:
        errors.append("DEXTER_DATASTORE_MINT_SNAPSHOT_RETENTION_PER_MINT must be at least 1.")
    if config.phase2.enabled and config.phase2.maintenance_interval_seconds < 10:
        errors.append("DEXTER_DATASTORE_MAINTENANCE_INTERVAL_SECONDS must be at least 10.")
    if config.phase2.enabled and config.phase2.max_database_size_bytes < 0:
        errors.append("DEXTER_DATASTORE_MAX_DATABASE_SIZE_BYTES must be >= 0.")

    if config.risk.per_trade_sol_cap <= 0:
        errors.append("DEXTER_PER_TRADE_SOL_CAP must be > 0.")
    if config.risk.session_sol_cap < config.risk.per_trade_sol_cap:
        errors.append("DEXTER_SESSION_SOL_CAP must be >= DEXTER_PER_TRADE_SOL_CAP.")
    if config.risk.daily_sol_cap < config.risk.session_sol_cap:
        errors.append("DEXTER_DAILY_SOL_CAP must be >= DEXTER_SESSION_SOL_CAP.")
    if config.risk.max_concurrent_sessions < 1:
        errors.append("DEXTER_MAX_CONCURRENT_SESSIONS must be >= 1.")
    if config.risk.per_creator_max_sessions < 1:
        errors.append("DEXTER_PER_CREATOR_MAX_SESSIONS must be >= 1.")
    if config.risk.wallet_reserve_floor_sol < 0:
        errors.append("DEXTER_WALLET_RESERVE_FLOOR_SOL must be >= 0.")
    if config.risk.daily_drawdown_stop_sol < 0:
        errors.append("DEXTER_DAILY_DRAWDOWN_STOP_SOL must be >= 0.")
    if config.execution.quote_retry_limit < 1:
        errors.append("DEXTER_QUOTE_RETRY_LIMIT must be >= 1.")
    if config.execution.send_retry_limit < 1:
        errors.append("DEXTER_SEND_RETRY_LIMIT must be >= 1.")
    if config.execution.confirmation_retry_limit < 1:
        errors.append("DEXTER_CONFIRMATION_RETRY_LIMIT must be >= 1.")
    if config.execution.retry_delay_seconds < 0:
        errors.append("DEXTER_RETRY_DELAY_SECONDS must be >= 0.")
    if config.runtime.mode in {"simulate", "live"} and not config.wallets.trading_private_key:
        errors.append("DEXTER_TRADING_PRIVATE_KEY or PRIVATE_KEY is required for simulate/live trader mode.")
    if config.wallets.hot_private_key and not config.wallets.trading_private_key:
        errors.append("DEXTER_HOT_PRIVATE_KEY requires DEXTER_TRADING_PRIVATE_KEY or PRIVATE_KEY.")
    if config.wallets.treasury_address and not config.wallets.trading_private_key:
        warnings.append("DEXTER_TREASURY_ADDRESS is set without a trading wallet; treasury reporting will be informational only.")
    try:
        from dexter_strategy import get_strategy_profile

        get_strategy_profile(config.strategy.default_profile)
    except Exception as exc:
        errors.append(f"DEXTER_STRATEGY_PROFILE is invalid: {exc}")
    if config.alerts.telegram_chat_id and not config.alerts.telegram_bot_token:
        warnings.append(
            "DEXTER_TELEGRAM_CHAT_ID is set, but DEXTER_TELEGRAM_BOT_TOKEN is still missing."
        )
    elif config.alerts.telegram_bot_token and not config.alerts.telegram_chat_id:
        warnings.append(
            "Telegram bot token is set. DEXTER_TELEGRAM_CHAT_ID can be auto-captured after you message the bot /id while Dexter is running."
        )

    return errors, warnings


def ensure_directories(config: AppConfig) -> None:
    config.paths.log_dir.mkdir(parents=True, exist_ok=True)
    config.paths.results_file.parent.mkdir(parents=True, exist_ok=True)
    config.paths.leaderboard_file.parent.mkdir(parents=True, exist_ok=True)
    config.paths.state_dir.mkdir(parents=True, exist_ok=True)
    config.paths.trader_snapshot_file.parent.mkdir(parents=True, exist_ok=True)
    config.paths.collector_snapshot_file.parent.mkdir(parents=True, exist_ok=True)
    config.paths.operator_control_file.parent.mkdir(parents=True, exist_ok=True)
    if config.phase2.enabled:
        config.phase2.export_dir.mkdir(parents=True, exist_ok=True)
    if config.backup.enabled:
        config.backup.directory.mkdir(parents=True, exist_ok=True)


def redact_url(value: str) -> str:
    if not value:
        return "<unset>"
    parsed = urlsplit(value)
    host = parsed.hostname or value
    scheme = parsed.scheme or "unknown"
    return f"{scheme}://{host}"


def redact_dsn(value: str) -> str:
    if not value:
        return "<unset>"
    host, port, name, user, _password = _parse_dsn(value)
    if not host:
        return "<invalid>"
    safe_user = user or "<unset>"
    safe_name = name or "<unset>"
    return f"postgres://{safe_user}:***@{host}:{port}/{safe_name}"


def log_startup_summary(logger: logging.Logger, config: AppConfig, component: str) -> None:
    execution_mode = resolve_trade_execution_mode(config)
    logger.info(
        "startup component=%s mode=%s execution_mode=%s network=%s mainnet_live_unlock=%s mainnet_dry_run=%s mev_enabled=%s mev_provider=%s mev_tip_lamports=%s http=%s ws=%s db=%s log_dir=%s backup_dir=%s data_store_enabled=%s data_store_export_dir=%s strategy_profile=%s max_sessions=%s per_creator_limit=%s reserve_floor=%s",
        component,
        config.runtime.mode,
        execution_mode,
        config.runtime.network,
        config.runtime.allow_mainnet_live,
        config.runtime.mainnet_dry_run,
        config.mev.enabled,
        config.mev.provider,
        config.mev.tip_lamports,
        redact_url(config.rpc.http_url),
        redact_url(config.rpc.ws_url),
        redact_dsn(config.database.dsn),
        config.paths.log_dir,
        config.backup.directory,
        config.phase2.enabled,
        config.phase2.export_dir,
        config.strategy.default_profile,
        config.risk.max_concurrent_sessions,
        config.risk.per_creator_max_sessions,
        config.risk.wallet_reserve_floor_sol,
    )


def current_utc_timestamp() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
