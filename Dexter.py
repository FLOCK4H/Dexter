import argparse
import asyncio
import logging
import os
import sys
import threading
from pathlib import Path

try:
    import curses
except ModuleNotFoundError as exc:
    if exc.name not in {"curses", "_curses"}:
        raise
    curses = None  # type: ignore[assignment]
    _CURSES_IMPORT_ERROR = exc
else:
    _CURSES_IMPORT_ERROR = None

if __package__ in (None, ""):
    from DexAI.trust_factor import Analyzer
    from DexLab.colors import cc
    from DexLab.common_ import *
else:
    from .DexAI.trust_factor import Analyzer
    from .DexLab.colors import cc
    from .DexLab.common_ import *

import asyncpg, collections
import datetime, time
from dataclasses import dataclass, replace
from decimal import Decimal
import json, traceback, base58
import textwrap
from typing import Any
from solders.keypair import Keypair  # type: ignore
import websockets

if __package__ in (None, ""):
    from DexLab.pump_fun import PumpFun
    from DexLab.pump_swap_executor import PumpSwapExecutor
    from DexLab.pump_swap_market import PumpSwapMarket
    from DexLab.swaps import SolanaSwaps
    from DexLab.utils import (
        DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS,
        estimate_priority_fee_micro_lamports,
        priority_fee_lamports,
        usd_to_lamports,
    )
    from DexLab.wsLogs import DexBetterLogs
else:
    from .DexLab.pump_fun import PumpFun
    from .DexLab.pump_swap_executor import PumpSwapExecutor
    from .DexLab.pump_swap_market import PumpSwapMarket
    from .DexLab.swaps import SolanaSwaps
    from .DexLab.utils import (
        DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS,
        estimate_priority_fee_micro_lamports,
        priority_fee_lamports,
        usd_to_lamports,
    )
    from .DexLab.wsLogs import DexBetterLogs
from aiohttp import ClientConnectionError, ClientResponseError, ClientSession
from dexter_config import (
    ensure_directories,
    load_config,
    log_startup_summary,
    read_env_values,
    resolve_trade_execution_mode,
    update_env_file,
    validate_config,
)
from dexter_logging import configure_root_logging
from dexter_data_store import Phase2Store
from dexter_local_postgres import ensure_local_postgres_running
from dexter_strategy import (
    evaluate_entry,
    explain_sell,
    get_strategy_profile,
    serialize_strategy_profile,
)
from dexter_operator import load_control_state, publish_runtime_snapshot, save_control_state
from dexter_alerts import AlertDispatcher, AlertTargets, TelegramCommandService
from dexter_mev import MevConfig
from dexter_time import (
    normalize_event_payload_timestamp,
    normalize_unix_timestamp,
    safe_utc_datetime_from_timestamp,
)
import settings as legacy_settings
from settings import *
from solana.rpc.async_api import AsyncClient

ROLLING_WINDOW_SIZE = 5
SINGLE_LOCK = True
MANAGED_POSITIONS_FILENAME = "managed-positions.json"
CLI_SPLASH_FRAMES = ("|", "/", "-", "\\")
ENTRY_SIGNAL_MIN_SWAPS = 3
ENTRY_SIGNAL_MIN_AGE_SECONDS = 2.0
ENTRY_SIGNAL_MAX_WAIT_SECONDS = 20.0

BLACKLIST = []
DEX_DIR = os.path.abspath(os.path.dirname(__file__))


def configure_logging(log_dir: Path):
    configure_root_logging(
        format=f'%(asctime)s - {cc.LIGHT_CYAN}Dexter ⚡ | %(message)s{cc.RESET}',
        datefmt='%H:%M:%S',
        level=logging.INFO,
        log_file=log_dir / 'dexter.log',
        stream=sys.stdout,
        force=True,
    )


configure_logging(Path(DEX_DIR) / "dev" / "logs")

def dex_welcome():
    sys.stdout.write(f"""
{cc.MAGENTA}▓█████▄ ▓█████ ▒██   ██▒▄▄▄█████▓▓█████  ██▀███  
{cc.MAGENTA}▒██▀ ██▌▓█   ▀ ▒▒ █ █ ▒░▓  ██▒ ▓▒▓█   ▀ ▓██ ▒ ██▒
{cc.MAGENTA}░██   █▌▒███   ░░  █   ░▒ ▓██░ ▒░▒███   ▓██ ░▄█ ▒
{cc.MAGENTA}░▓█▄   ▌▒▓█  ▄  ░ █ █ ▒ ░ ▓██▓ ░ ▒▓█  ▄ ▒██▀▀█▄  
{cc.MAGENTA}░▒████▓ ░▒████▒▒██▒ ▒██▒  ▒██▒ ░ ░▒████▒░██▓ ▒██▒
{cc.MAGENTA} ▒▒▓  ▒ ░░ ▒░ ░▒▒ ░ ░▓ ░  ▒ ░░   ░░ ▒░ ░░ ▒▓ ░▒▓░
{cc.MAGENTA} ░ ▒  ▒  ░ ░  ░░░   ░▒ ░    ░     ░ ░  ░  ░▒ ░ ▒░
{cc.MAGENTA} ░ ░  ░    ░    ░    ░    ░         ░     ░░   ░ 
{cc.MAGENTA}   ░       ░  ░ ░    ░              ░  ░   ░     
{cc.MAGENTA} ░  
{cc.CYAN}          𝗕𝘆 𝗙𝗟𝗢𝗖𝗞𝟰𝗛               𝘃3.0{cc.RESET}

{cc.RESET}""")
    sys.stdout.flush()


def _utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _parse_instruction_error_code(result: str | None) -> int | None:
    if not result or not isinstance(result, str):
        return None
    if not result.startswith("InstructionError"):
        return None
    _prefix, _separator, raw_code = result.partition(":")
    if not raw_code:
        return None
    try:
        return int(raw_code)
    except (TypeError, ValueError):
        return None


def _managed_positions_path(config) -> Path:
    return config.paths.state_dir / MANAGED_POSITIONS_FILENAME


def _default_managed_positions_store() -> dict[str, Any]:
    return {
        "version": 1,
        "updated_at": None,
        "positions": {},
    }


def load_managed_positions(config) -> dict[str, Any]:
    path = _managed_positions_path(config)
    if not path.exists():
        return _default_managed_positions_store()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return _default_managed_positions_store()
    if not isinstance(payload, dict):
        return _default_managed_positions_store()
    store = _default_managed_positions_store()
    store.update(payload)
    positions = store.get("positions")
    if not isinstance(positions, dict):
        store["positions"] = {}
    return store


def save_managed_positions(config, store: dict[str, Any]) -> Path:
    path = _managed_positions_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(store)
    payload["updated_at"] = _utc_now_iso()
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)
    return path


def list_managed_positions(config, *, status: str | None = None) -> list[dict[str, Any]]:
    positions = list((load_managed_positions(config).get("positions") or {}).values())
    if status:
        positions = [item for item in positions if item.get("status") == status]
    positions.sort(
        key=lambda item: (
            0 if item.get("status") == "open" else 1,
            str(item.get("updated_at") or item.get("opened_at") or ""),
            str(item.get("mint_id") or ""),
        ),
        reverse=True,
    )
    return positions


def _decimal_from_value(value: Any, default: str = "0") -> Decimal:
    if value in (None, ""):
        return Decimal(default)
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _render_short_key(value: str | None) -> str:
    if not value:
        return "(unset)"
    text = str(value)
    if len(text) <= 12:
        return text
    return f"{text[:4]}...{text[-4:]}"


def _price_to_lamports(token_balance: int, price: Decimal) -> int:
    if token_balance <= 0 or price <= 0:
        return 0
    return int(
        (Decimal(int(token_balance)) / Decimal("1000000"))
        * price
        * Decimal("1000000000")
    )


def _position_unrealized_profit_lamports(position: dict[str, Any]) -> int | None:
    if position.get("status") != "open":
        return position.get("realized_profit_lamports")
    cost_basis_lamports = int(position.get("cost_basis_lamports", 0) or 0)
    token_balance = int(position.get("token_balance", 0) or 0)
    current_price = _decimal_from_value(position.get("current_price") or position.get("last_price"))
    if cost_basis_lamports <= 0 or token_balance <= 0 or current_price <= 0:
        return None
    return _price_to_lamports(token_balance, current_price) - cost_basis_lamports


def _interactive_terminal() -> bool:
    return bool(sys.stdin.isatty() and sys.stdout.isatty())


def _tui_available() -> tuple[bool, str | None]:
    if curses is not None:
        return True, None

    detail = "Dexter's interactive terminal UI is unavailable because Python could not import curses."
    if _CURSES_IMPORT_ERROR is not None:
        detail = f"{detail} ({_CURSES_IMPORT_ERROR})"
    if os.name == "nt":
        detail = (
            f"{detail}\n"
            "Windows support requires the bundled `windows-curses` dependency. "
            "Reinstall Dexter with `python -m pip install .` or install it directly with "
            "`python -m pip install windows-curses`."
        )
    return False, detail


def _clear_terminal() -> None:
    if _interactive_terminal():
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()


def show_startup_splash() -> None:
    if not _interactive_terminal():
        return

    art = [
        "██████╗ ███████╗██╗  ██╗████████╗███████╗██████╗ ",
        "██╔══██╗██╔════╝╚██╗██╔╝╚══██╔══╝██╔════╝██╔══██╗",
        "██║  ██║█████╗   ╚███╔╝    ██║   █████╗  ██████╔╝",
        "██║  ██║██╔══╝   ██╔██╗    ██║   ██╔══╝  ██╔══██╗",
        "██████╔╝███████╗██╔╝ ██╗   ██║   ███████╗██║  ██║",
        "╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝",
    ]
    taglines = (
        "Dexter CLI • Run • Create • Manage",
        "Dry-by-default where possible • Recovery-first position management",
    )
    steps = 20
    for idx in range(steps + 1):
        _clear_terminal()
        progress = idx / steps
        filled = int(progress * 28)
        bar = "[" + ("#" * filled) + (" " * (28 - filled)) + "]"
        for line in art:
            print(f"{cc.CYAN}{line}{cc.RESET}")
        print()
        for line in taglines:
            print(f"{cc.LIGHT_CYAN}{line}{cc.RESET}")
        print()
        print(
            f"{cc.LIGHT_GREEN}{CLI_SPLASH_FRAMES[idx % len(CLI_SPLASH_FRAMES)]} "
            f"initializing {bar}{cc.RESET}"
        )
        time.sleep(0.08)
    _clear_terminal()

class Dexter:
    def __init__(self, config, session_seed=None):
        self.config = config
        self.session_seed = dict(session_seed or {})
        self.skip_database_init = bool(self.session_seed.get("skip_database"))
        self.db_dsn = config.database.dsn
        self.http_url = config.rpc.http_url
        self.ws_url = config.rpc.ws_url
        self.runtime_mode = config.runtime.mode
        self.execution_mode = resolve_trade_execution_mode(config)
        self.execution_label = (
            "mainnet_dry_run"
            if self.runtime_mode == "live" and self.execution_mode == "simulate"
            else self.execution_mode
        )
        self.shadow_mode = self.execution_mode in {"paper", "simulate"}
        self.simulation_mode = self.execution_mode == "simulate"
        self.live_send_enabled = self.execution_mode == "live"
        self.active_mev_config = (
            config.mev
            if config.mev.enabled and self.live_send_enabled and config.runtime.network == "mainnet"
            else None
        )
        self.pool = None
        self.analyzer = Analyzer(self.db_dsn)
        self.last_processed_timestamp = None
        self.leaderboard = None
        self.active_sessions = {}
        self.holdings = {}
        self.privkey = None
        if config.wallets.trading_private_key:
            self.privkey = Keypair.from_bytes(base58.b58decode(config.wallets.trading_private_key))
        self.wallet_balance = 0
        self.pending_buy_spend = {}
        self.session_spend = collections.defaultdict(int)
        self.daily_spend_lamports = 0
        self.daily_realized_profit_lamports = 0
        self.daily_spend_date = datetime.datetime.now(datetime.timezone.utc).date()
        self.wallet = str(self.privkey.pubkey()) if self.privkey else f"{self.execution_label}-shadow-wallet"
        self.hot_wallet = self.wallet
        if config.wallets.hot_private_key:
            try:
                self.hot_wallet = str(Keypair.from_bytes(base58.b58decode(config.wallets.hot_private_key)).pubkey())
            except Exception:
                self.hot_wallet = self.wallet
        self.treasury_wallet = config.wallets.treasury_address or self.wallet
        self.dex_dir = DEX_DIR
        self.phase2 = None
        self.leaderboard_version = None
        if self.config.phase2.enabled and not self.skip_database_init:
            self.phase2 = Phase2Store(self.db_dsn, config=self.config)
        self.strategy_profile = get_strategy_profile(self.config.strategy.default_profile)
        if self.config.strategy.min_entry_score_override is not None:
            self.strategy_profile = replace(
                self.strategy_profile,
                min_entry_score=float(self.config.strategy.min_entry_score_override),
            )
        self.operator_state = load_control_state(self.config.paths.operator_control_file)
        self.recent_failures = collections.deque(maxlen=25)
        self.alerts = AlertDispatcher(
            AlertTargets(
                telegram_bot_token=self.config.alerts.telegram_bot_token,
                telegram_chat_id=self.config.alerts.telegram_chat_id,
                discord_webhook_url=self.config.alerts.discord_webhook_url,
                desktop_notifications=self.config.alerts.desktop_notifications,
            )
        )
        self.logs = asyncio.Queue()
        self.stop_event = asyncio.Event()
        self.active_tasks = set()
        self.counter = 0
        self.time_start = 0
        self.updating = False
        self.session = None
        self.async_client = None
        self.pump_swap = None
        self.pump_swap_market = None
        self.pump_swap_executor = None
        self.swaps = None
        self.dexLogs = None
        self.embedded_wslogs = None
        self.embedded_wslogs_task = None
        self.embedded_wslogs_watchdog_task = None
        self.embedded_wslogs_last_reported_error = None
        self.embedded_wslogs_last_checkup_at = None
        self.embedded_wslogs_checkup_interval_seconds = 60
        self.is_closing = False
        self.pump_swap_refresh_interval = 0.5
        self.pump_swap_lookup_interval = 10.0
        self.pump_swap_pending_lookup_interval = 3.0
        self.pump_swap_lookup_miss_backoff_interval = 6.0
        self.pump_swap_lookup_rate_limit_backoff_interval = 20.0
        self.pump_swap_lookup_warning_interval = 30.0
        self.pump_swap_refresh_locks = collections.defaultdict(asyncio.Lock)

        self.swap_folder = collections.defaultdict(
            lambda: {
                "state": {},                  # data about the mint: holders, price, etc.
            }
        )
        self.sub_second_counters = {}
        self.mint_locks = collections.defaultdict(asyncio.Lock)  # One lock per mint_id
    
    """
        Market part
    """
    async def handle_market(self):
        while not self.stop_event.is_set():
            try:
                log, program = await asyncio.wait_for(self.logs.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            if self.stop_event.is_set():
                break
            if log:
                task = asyncio.create_task(self.handle_single_log(log, program))
                self.active_tasks.add(task)

                def _cleanup(_):
                    self.active_tasks.discard(task)
                task.add_done_callback(_cleanup)

    def _is_expected_shutdown_error(self, exc: BaseException) -> bool:
        if not (self.is_closing or self.stop_event.is_set()):
            return False
        if isinstance(
            exc,
            (
                ClientConnectionError,
                ConnectionError,
                TimeoutError,
                websockets.ConnectionClosed,
            ),
        ):
            return True
        message = str(exc).lower()
        return any(
            token in message
            for token in (
                "server disconnected",
                "session is closed",
                "connector is closed",
                "cannot write to closing transport",
            )
        )

    async def handle_single_log(self, log, program):
        try:
            if program == PUMP_SWAP:
                await self.handle_pump_swap_log(log)
                return
            clean_log = await self.dexLogs.collect(log, debug=False)
            if clean_log:
                is_mint = clean_log["is_mint"]
                sig = clean_log["sig"]
                slot = clean_log["slot"]
                raw_logs = clean_log.get("raw_logs", [])
                program_data = clean_log["program_data"]

                if self._detect_pump_fun_to_pump_swap_migration(raw_logs):
                    for data in program_data.values():
                        if not isinstance(data, dict):
                            continue
                        migrated_mint = data.get("mint")
                        if migrated_mint and migrated_mint in self.holdings:
                            await self._mark_pump_swap_pending(
                                migrated_mint,
                                reason="pump_fun_migrate_log",
                                signature=sig,
                                slot=slot,
                            )

                for idx, data in program_data.items():
                    if not isinstance(data, dict):
                        logging.debug(
                            "Ignoring undecoded program payload for %s at log index %s: %r",
                            sig,
                            idx,
                            data,
                        )
                        continue
                    event_data = normalize_event_payload_timestamp(data, fallback=time.time()) or data
                    raw_event_fingerprint = None
                    if self.phase2 is not None:
                        raw_event_fingerprint = await self.phase2.record_raw_event(
                            source="trader",
                            program=program,
                            signature=sig,
                            slot=int(slot or 0),
                            log_index=int(idx),
                            event_type="mints" if is_mint and "bonding_curve" in event_data else "swaps",
                            is_mint=bool(is_mint and "bonding_curve" in event_data),
                            payload=event_data,
                            raw_logs=raw_logs,
                            observed_at=safe_utc_datetime_from_timestamp(
                                event_data.get("timestamp"),
                                fallback=time.time(),
                            ),
                        )
                    if is_mint and "bonding_curve" in event_data:
                        await self.process_data(
                            "mints",
                            sig,
                            event_data,
                            raw_event_fingerprint=raw_event_fingerprint,
                            slot=slot,
                        )
                    elif "bonding_curve" in event_data or "sol_amount" in event_data:
                        await self.process_data(
                            "swaps",
                            sig,
                            event_data,
                            raw_event_fingerprint=raw_event_fingerprint,
                            slot=slot,
                        )
        except Exception as e:
            if self._is_expected_shutdown_error(e):
                return
            logging.error(f"{cc.RED}Error in handle_single_log: {e}{cc.RESET}")
            traceback.print_exc()

    async def process_data(self, type, sig, data, raw_event_fingerprint=None, slot=None):
        """
            This method does concurrency control & the sub-second timestamp logic for each swap.
        """
        mint = data.get('mint', None)
        if not mint:
            return

        if self.leaderboard is None:
            return

        lock = self.mint_locks[mint]
        async with lock:
            # MINTS
            if type == "mints":
                owner = data.get("user")
                name = data.get("name")
                bonding_curve = data.get("bonding_curve")

                OIL = (owner in self.leaderboard)

                if self.counter == 0:
                    self.counter = 1
                    logging.info(f"{cc.LIGHT_GRAY}Sample: {mint}, owner: {owner}, created: {time.strftime('%H:%M:%S')}{cc.RESET}")

                if OIL:
                    logging.info(
                        f"Processed Mint: {mint}, owner: {owner}, created: {time.strftime('%H:%M:%S')}, "
                        f"{cc.GREEN}Match{cc.RESET}"
                    )

                # If it's an owner in the leaderboard, set up a session
                if OIL:
                    self.time_start = time.time()

                    if mint not in self.active_sessions:
                        skip_reason = self._session_skip_reason(mint, owner)
                        if skip_reason:
                            await self._phase2_record_decision(mint, owner, "session_skipped", skip_reason)
                            await self._phase2_record_risk_event(
                                mint,
                                owner,
                                "session_skipped",
                                "warn",
                                skip_reason,
                            )
                            return
                        if len(self.active_sessions) >= int(self.config.risk.max_concurrent_sessions):
                            await self._phase2_record_decision(
                                mint,
                                owner,
                                "session_skipped",
                                "max_concurrent_sessions",
                                decision_outcome={"active_sessions": len(self.active_sessions)},
                            )
                            await self._phase2_record_risk_event(
                                mint,
                                owner,
                                "session_skipped",
                                "warn",
                                "max_concurrent_sessions",
                                context={"active_sessions": len(self.active_sessions)},
                            )
                            return
                        logging.info(f"{cc.YELLOW}Creating session for {mint}...{cc.RESET}")
                        creator = self.leaderboard.get(owner)
                        trust_level = await self.set_trust_level(creator)
                        self.swap_folder[mint] = {
                            "name": name,
                            "owner": owner,
                            "bonding_curve": bonding_curve,
                            "last_event_fingerprint": raw_event_fingerprint,
                            "last_event_slot": slot,
                            "creation_sig": sig,
                            "market": "pump_fun",
                        }
                        await self._phase2_start_session(
                            mint,
                            owner,
                            trust_level=trust_level,
                            open_reason="leaderboard_match",
                            session_context={
                                "creation_sig": sig,
                                "mint_name": name,
                                "bonding_curve": bonding_curve,
                            },
                        )
                        await self._phase2_record_decision(
                            mint,
                            owner,
                            "session_open",
                            "leaderboard_match",
                            decision_inputs={
                                "creation_sig": sig,
                                "slot": slot,
                            },
                        )
                        session_task = asyncio.create_task(
                            self.monitor_mint_session(mint, owner)
                        )
                        self.active_sessions[mint] = session_task
                    else:
                        logging.info(f"Session for {mint} already active.")

            # SWAPS
            elif type == "swaps":
                if mint not in self.swap_folder:
                    return
                if self.swap_folder[mint].get("market") == "pump_swap":
                    return
                self.swap_folder[mint]["last_event_fingerprint"] = raw_event_fingerprint
                self.swap_folder[mint]["last_event_slot"] = slot

                user = data.get('user')
                sol_amount = data.get('sol_amount')
                token_amount = data.get('token_amount')
                is_buy = data.get('is_buy')
                timestamp = normalize_unix_timestamp(
                    data.get('timestamp'),
                    fallback=time.time(),
                )
                vsr = data.get('virtual_sol_reserves')
                vtr = data.get('virtual_token_reserves')

                # Sub-second timestamp logic
                if mint not in self.sub_second_counters:
                    self.sub_second_counters[mint] = (timestamp, 0)
                    counter = 0
                else:
                    last_ts, counter = self.sub_second_counters[mint]
                    if timestamp == last_ts:
                        counter += 1
                    else:
                        counter = 0
                self.sub_second_counters[mint] = (timestamp, counter)

                unique_timestamp = f"{timestamp}.{counter:03d}"

                price = await self._compute_price(vsr, vtr)
                mc = await self._get_market_cap(price)
                price_usd = float(price * self.analyzer.sol_price_usd)
                liquidity = float(
                    (
                        Decimal(str(vsr or 0))
                        + (Decimal(str(vtr or 0)) * price)
                    ) * self.analyzer.sol_price_usd
                )

                st, state_was_empty = self._ensure_market_state(
                    mint,
                    price=price,
                    mc=mc,
                    liquidity=liquidity,
                    price_usd=price_usd,
                    timestamp=int(timestamp) if timestamp else int(time.time()),
                    unique_timestamp=unique_timestamp,
                )
                st["price"] = price
                st["high_price"] = max(Decimal(str(st.get("high_price", price) or price)), price)
                st["mc"] = mc
                st["liquidity"] = liquidity
                st["price_usd"] = price_usd
                st["last_tx_time"] = unique_timestamp
                st["tx_counts"]["swaps"] += 1
                if is_buy:
                    st["tx_counts"]["buys"] += 1
                else:
                    st["tx_counts"]["sells"] += 1

                if user not in st["holders"]:
                    st["holders"][user] = {
                        "balance": int(token_amount),
                        "balance_changes": [{"type": "buy" if is_buy else "sell", "price_was": price}]
                    }
                else:
                    if is_buy:
                        st["holders"][user]["balance"] += int(token_amount)
                    else:
                        st["holders"][user]["balance"] -= int(token_amount)
                    st["holders"][user]["balance_changes"].append(
                        {"type": "buy" if is_buy else "sell", "price_was": price}
                    )

                st["price_history"][unique_timestamp] = price
                if state_was_empty:
                    logging.info(f"Socket Latency: {round(max(0.0, time.time() - timestamp), 2)}s")
                self.swap_folder[mint]["market"] = "pump_fun"

    def _detect_pump_fun_to_pump_swap_migration(self, raw_logs):
        if not raw_logs:
            return False
        needles = (
            "instruction: migrate",
            "instruction: migrate_bonding_curve_creator",
            "instruction: migratebondingcurvecreator",
        )
        for raw_log in raw_logs:
            lowered = raw_log.lower()
            if any(needle in lowered for needle in needles):
                return True
        return False

    async def _mark_pump_swap_pending(self, mint_id: str, *, reason: str, signature: str | None = None, slot: int | None = None):
        mint_state = self.swap_folder.get(mint_id)
        if not mint_state:
            return
        mint_state["migration_pending"] = True
        mint_state["migration_reason"] = reason
        if signature:
            mint_state["migration_signature"] = signature
        if slot is not None:
            mint_state["migration_slot"] = slot
        await self._phase2_record_decision(
            mint_id,
            self.holdings.get(mint_id, {}).get("owner"),
            "migration_detected",
            reason,
            decision_outcome={
                "market": "pump_swap_pending",
                "signature": signature,
                "slot": slot,
            },
        )

    async def _activate_pump_swap_market(self, mint_id: str, pool_state, *, signature: str | None = None, slot: int | None = None):
        mint_state = self.swap_folder.get(mint_id)
        if not mint_state:
            return
        mint_state["market"] = "pump_swap"
        mint_state["migration_pending"] = False
        mint_state["pump_swap_pool"] = pool_state.pool
        mint_state["pump_swap_creator"] = pool_state.creator
        mint_state["pump_swap_coin_creator"] = pool_state.coin_creator
        mint_state["pump_swap_mayhem_mode"] = bool(pool_state.is_mayhem_mode)
        if signature:
            mint_state["pump_swap_activation_signature"] = signature
        if slot is not None:
            mint_state["pump_swap_activation_slot"] = slot
        await self._phase2_record_decision(
            mint_id,
            self.holdings.get(mint_id, {}).get("owner"),
            "market_switch",
            "pump_swap_activated",
            decision_outcome={
                "market": "pump_swap",
                "pool": pool_state.pool,
                "signature": signature,
                "slot": slot,
            },
        )

    def _pump_swap_lookup_due(self, mint_id: str, *, now: float | None = None, force: bool = False) -> bool:
        if now is None:
            now = time.time()
        last_lookup = float(self.swap_folder.get(mint_id, {}).get("pump_swap_lookup_at", 0) or 0)
        interval = (
            self.pump_swap_pending_lookup_interval
            if force
            else self.pump_swap_lookup_interval
        )
        return (now - last_lookup) >= interval

    def _pump_swap_lookup_allowed(self, mint_id: str, *, force: bool = False) -> bool:
        mint_state = self.swap_folder.get(mint_id, {}) or {}
        if force:
            return True
        if mint_state.get("pump_swap_pool"):
            return True
        return bool(
            mint_state.get("migration_pending")
            or mint_state.get("market") == "pump_swap"
        )

    def _pump_swap_lookup_retry_after(self, mint_id: str) -> float:
        return float(self.swap_folder.get(mint_id, {}).get("pump_swap_lookup_retry_after", 0) or 0)

    def _set_pump_swap_lookup_retry_after(self, mint_id: str, delay_seconds: float, *, reason: str) -> float:
        retry_after = time.time() + max(0.0, float(delay_seconds))
        mint_state = self.swap_folder.get(mint_id)
        if mint_state is None:
            mint_state = {}
            self.swap_folder[mint_id] = mint_state
        mint_state["pump_swap_lookup_retry_after"] = retry_after
        mint_state["pump_swap_lookup_retry_reason"] = reason
        return retry_after

    def _clear_pump_swap_lookup_retry_after(self, mint_id: str) -> None:
        mint_state = self.swap_folder.get(mint_id)
        if mint_state is None:
            return
        mint_state.pop("pump_swap_lookup_retry_after", None)
        mint_state.pop("pump_swap_lookup_retry_reason", None)

    def _log_pump_swap_lookup_warning(self, mint_id: str, message: str) -> None:
        mint_state = self.swap_folder.get(mint_id)
        if mint_state is None:
            return
        now = time.time()
        last_warning = float(mint_state.get("pump_swap_lookup_warning_at", 0) or 0)
        if (now - last_warning) < self.pump_swap_lookup_warning_interval:
            return
        mint_state["pump_swap_lookup_warning_at"] = now
        logging.warning(message)

    @staticmethod
    def _is_rate_limited_rpc_error(exc: Exception) -> bool:
        if isinstance(exc, ClientResponseError):
            return exc.status == 429
        return "too many requests" in str(exc).lower()

    def _pump_swap_refresh_due(self, mint_id: str, *, now: float | None = None, force: bool = False) -> bool:
        if force:
            return True
        if now is None:
            now = time.time()
        last_refresh = float(self.swap_folder.get(mint_id, {}).get("pump_swap_refresh_at", 0) or 0)
        return (now - last_refresh) >= self.pump_swap_refresh_interval

    def _ensure_market_state(
        self,
        mint_id: str,
        *,
        price: Decimal,
        mc: Decimal | None = None,
        liquidity: float | None = None,
        price_usd: float | None = None,
        timestamp: int | None = None,
        unique_timestamp: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        mint_state = self.swap_folder[mint_id]
        state = mint_state.get("state")
        state_was_empty = not state
        if state is None:
            state = {}
            mint_state["state"] = state

        created_at = int(timestamp) if timestamp is not None else int(time.time())
        unique_time = unique_timestamp or f"{created_at}.000"
        if state.get("price") in (None, ""):
            state["price"] = price
        if state.get("open_price") in (None, ""):
            state["open_price"] = price
        try:
            current_high_price = Decimal(str(state.get("high_price", price) or price))
        except Exception:
            current_high_price = price
        state["high_price"] = max(current_high_price, price)
        if state.get("mc") in (None, ""):
            state["mc"] = mc if mc is not None else Decimal("0")
        if state.get("liquidity") in (None, ""):
            state["liquidity"] = liquidity if liquidity is not None else 0.0
        if state.get("price_usd") in (None, ""):
            state["price_usd"] = (
                price_usd if price_usd is not None else float(price * self.analyzer.sol_price_usd)
            )
        if state.get("last_tx_time") in (None, ""):
            state["last_tx_time"] = unique_time
        holders = state.get("holders")
        if not isinstance(holders, dict):
            holders = {}
            state["holders"] = holders
        price_history = state.get("price_history")
        if not isinstance(price_history, dict):
            price_history = {}
            state["price_history"] = price_history
        tx_counts = state.get("tx_counts")
        if not isinstance(tx_counts, dict):
            tx_counts = {}
            state["tx_counts"] = tx_counts
        tx_counts.setdefault("swaps", 0)
        tx_counts.setdefault("buys", 0)
        tx_counts.setdefault("sells", 0)
        state.setdefault("created", created_at)
        return state, state_was_empty

    def _next_unique_timestamp(self, mint_id: str, timestamp: int) -> str:
        if mint_id not in self.sub_second_counters:
            self.sub_second_counters[mint_id] = (timestamp, 0)
            counter = 0
        else:
            last_ts, counter = self.sub_second_counters[mint_id]
            if timestamp == last_ts:
                counter += 1
            else:
                counter = 0
        self.sub_second_counters[mint_id] = (timestamp, counter)
        return f"{timestamp}.{counter:03d}"

    def _mint_state_observation_age_seconds(self, state: dict[str, Any] | None) -> float:
        state = dict(state or {})
        price_history = state.get("price_history") or {}
        observed_timestamps: list[float] = []
        for raw_timestamp in price_history.keys():
            try:
                observed_timestamps.append(float(raw_timestamp))
            except (TypeError, ValueError):
                continue
        if observed_timestamps:
            return max(0.0, time.time() - min(observed_timestamps))

        created_at = state.get("created")
        try:
            created_value = float(created_at)
        except (TypeError, ValueError):
            return 0.0
        if created_value <= 0:
            return 0.0
        return max(0.0, time.time() - created_value)

    def _entry_signal_wait_reason(self, state: dict[str, Any] | None) -> tuple[bool, str, dict[str, Any]]:
        state = dict(state or {})
        tx_counts = state.get("tx_counts") or {}
        swaps = max(0, int(tx_counts.get("swaps", 0) or 0))
        age_seconds = self._mint_state_observation_age_seconds(state)
        liquidity = float(state.get("liquidity", 0) or 0)
        wait_reasons: list[str] = []
        if swaps < ENTRY_SIGNAL_MIN_SWAPS:
            wait_reasons.append(f"swaps {swaps}/{ENTRY_SIGNAL_MIN_SWAPS}")
        if age_seconds < ENTRY_SIGNAL_MIN_AGE_SECONDS:
            wait_reasons.append(f"age {age_seconds:.1f}s/{ENTRY_SIGNAL_MIN_AGE_SECONDS:.1f}s")
        return (
            bool(wait_reasons),
            ", ".join(wait_reasons),
            {
                "swaps": swaps,
                "age_seconds": round(age_seconds, 3),
                "liquidity": liquidity,
            },
        )

    async def _apply_pump_swap_snapshot(self, mint_id: str, snapshot, *, signature: str | None = None, slot: int | None = None):
        if mint_id not in self.swap_folder:
            return

        price = Decimal(str(snapshot.get("price", 0) or 0))
        if price <= 0:
            return

        timestamp = int(time.time())
        unique_timestamp = self._next_unique_timestamp(mint_id, timestamp)
        mc = await self._get_market_cap(price)
        price_usd = float(price * self.analyzer.sol_price_usd)
        liquidity_usd = float(
            (Decimal(str(snapshot.get("liquidity_sol", 0) or 0)) * self.analyzer.sol_price_usd)
        )

        state, _state_was_empty = self._ensure_market_state(
            mint_id,
            price=price,
            mc=mc,
            liquidity=liquidity_usd,
            price_usd=price_usd,
            timestamp=timestamp,
            unique_timestamp=unique_timestamp,
        )
        state["price"] = price
        state["mc"] = mc
        state["liquidity"] = liquidity_usd
        state["price_usd"] = price_usd
        state["last_tx_time"] = unique_timestamp
        state.setdefault("price_history", {})[unique_timestamp] = price
        state["high_price"] = max(Decimal(str(state.get("high_price", price) or price)), price)

        tx_counts = state.setdefault("tx_counts", {"swaps": 0, "buys": 0, "sells": 0})
        if signature and signature != self.swap_folder[mint_id].get("last_pump_swap_signature"):
            tx_counts["swaps"] = int(tx_counts.get("swaps", 0) or 0) + 1
            tx_counts["pump_swap_swaps"] = int(tx_counts.get("pump_swap_swaps", 0) or 0) + 1
            self.swap_folder[mint_id]["last_pump_swap_signature"] = signature

        self.swap_folder[mint_id]["market"] = "pump_swap"
        self.swap_folder[mint_id]["pump_swap_refresh_at"] = time.time()
        self.swap_folder[mint_id]["last_event_slot"] = slot

    async def _refresh_pump_swap_market_for_mint(
        self,
        mint_id: str,
        *,
        signature: str | None = None,
        slot: int | None = None,
        force_lookup: bool = False,
        force_refresh: bool = False,
    ):
        if mint_id not in self.swap_folder or self.pump_swap_market is None:
            return None

        async with self.pump_swap_refresh_locks[mint_id]:
            now = time.time()
            mint_state = self.swap_folder[mint_id]
            owner = (
                self.holdings.get(mint_id, {}).get("owner")
                or mint_state.get("owner")
                or mint_state.get("pump_swap_creator")
            )
            pool_state = None
            pool_address = mint_state.get("pump_swap_pool")

            if not pool_address and not self._pump_swap_lookup_allowed(mint_id, force=force_lookup):
                return None

            retry_after = self._pump_swap_lookup_retry_after(mint_id)
            if not pool_address and retry_after > now:
                return None

            if not pool_address and self._pump_swap_lookup_due(mint_id, now=now, force=force_lookup):
                mint_state["pump_swap_lookup_at"] = now
                try:
                    pool_state = await self.pump_swap_market.find_pool_for_mint(mint_id, creator=owner)
                except Exception as exc:
                    if self._is_rate_limited_rpc_error(exc):
                        retry_after = self._set_pump_swap_lookup_retry_after(
                            mint_id,
                            self.pump_swap_lookup_rate_limit_backoff_interval,
                            reason="rate_limited",
                        )
                        self._log_pump_swap_lookup_warning(
                            mint_id,
                            (
                                f"PumpSwap lookup rate-limited for {mint_id}; backing off "
                                f"until {time.strftime('%H:%M:%S', time.gmtime(retry_after))} UTC."
                            ),
                        )
                        return None
                    raise
                if pool_state is None:
                    self._set_pump_swap_lookup_retry_after(
                        mint_id,
                        self.pump_swap_lookup_miss_backoff_interval,
                        reason="pool_not_found",
                    )
                    return None
                self._clear_pump_swap_lookup_retry_after(mint_id)
                pool_address = pool_state.pool
                await self._activate_pump_swap_market(
                    mint_id,
                    pool_state,
                    signature=signature,
                    slot=slot,
                )

            if not pool_address:
                return None

            if not self._pump_swap_refresh_due(mint_id, now=now, force=force_refresh):
                return None

            snapshot = await self.pump_swap_market.fetch_price_snapshot(
                pool=pool_address,
                pool_state=pool_state,
            )
            await self._apply_pump_swap_snapshot(
                mint_id,
                snapshot,
                signature=signature,
                slot=slot,
            )
            return snapshot

    async def handle_pump_swap_log(self, log):
        if self.pump_swap_market is None:
            return

        tracked_mints = [
            mint_id
            for mint_id in self.holdings.keys()
            if mint_id in self.swap_folder
            and (
                self.swap_folder[mint_id].get("migration_pending")
                or self.swap_folder[mint_id].get("market") == "pump_swap"
                or self.swap_folder[mint_id].get("pump_swap_pool")
            )
        ]
        if not tracked_mints:
            return

        payload = await self.dexLogs.process_log(log)
        signature = payload.get("signature") if payload else None
        slot = payload.get("slot") if payload else None

        for mint_id in tracked_mints:
            await self._refresh_pump_swap_market_for_mint(
                mint_id,
                signature=signature,
                slot=slot,
                force_lookup=self.swap_folder[mint_id].get("migration_pending", False),
            )

    async def _estimate_pump_fun_priority_fee(self, mint_id: str, bonding_curve: str) -> tuple[int, int]:
        if self.pump_swap is None:
            return 0, 0
        try:
            if hasattr(self.pump_swap, "priority_fee_accounts_for_buy"):
                fee_accounts = self.pump_swap.priority_fee_accounts_for_buy(mint_id, bonding_curve)
            else:
                fee_accounts = [self.wallet, mint_id, bonding_curve]
            priority_micro_lamports = await estimate_priority_fee_micro_lamports(
                self.config.rpc.http_url,
                addresses=fee_accounts,
                compute_units=DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS,
            )
        except Exception as exc:
            logging.warning("Priority fee estimate failed for pump.fun trade %s: %s", mint_id, exc)
            return 0, 0
        return (
            priority_micro_lamports,
            priority_fee_lamports(
                priority_micro_lamports,
                DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS,
            ),
        )

    @staticmethod
    def _render_mev_tip_sol(mev_config: MevConfig) -> str:
        rendered = format(mev_config.tip_sol.normalize(), "f")
        return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered

    def _announce_mev_trade(self, *, side: str, mint_id: str, venue: str) -> None:
        if self.active_mev_config is None:
            return
        logging.info(
            "%sMEV mainnet %s armed for %s via %s | provider=%s tip=%s SOL (%s lamports).%s",
            cc.CYAN,
            side.upper(),
            mint_id,
            venue,
            self.active_mev_config.provider,
            self._render_mev_tip_sol(self.active_mev_config),
            self.active_mev_config.tip_lamports,
            cc.RESET,
        )

    async def _estimate_pump_swap_priority_fee(
        self,
        mint_id: str,
        *,
        pool: str | None = None,
        creator: str | None = None,
    ) -> tuple[int, int]:
        if self.pump_swap_executor is None:
            return 0, 0
        try:
            if hasattr(self.pump_swap_executor, "priority_fee_accounts_for_sell"):
                fee_accounts = self.pump_swap_executor.priority_fee_accounts_for_sell(
                    mint_address=mint_id,
                    pool=pool,
                    creator=creator,
                )
            else:
                fee_accounts = [self.wallet, mint_id, pool or "", creator or ""]
            priority_micro_lamports = await estimate_priority_fee_micro_lamports(
                self.config.rpc.http_url,
                addresses=fee_accounts,
                compute_units=DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS,
            )
        except Exception as exc:
            logging.warning("Priority fee estimate failed for pump.swap trade %s: %s", mint_id, exc)
            return 0, 0
        return (
            priority_micro_lamports,
            priority_fee_lamports(
                priority_micro_lamports,
                DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS,
            ),
        )

    async def _submit_pump_swap_sell(self, mint_id, amount, owner, fee, *, sim=False):
        if self.pump_swap_executor is None:
            return "pump_swap_unavailable"

        mint_state = self.swap_folder.get(mint_id, {})
        await self._refresh_pump_swap_market_for_mint(
            mint_id,
            force_lookup=not mint_state.get("pump_swap_pool"),
            force_refresh=True,
        )
        mint_state = self.swap_folder.get(mint_id, {})
        pool_address = mint_state.get("pump_swap_pool")
        if not pool_address:
            return "pump_swap_pool_unavailable"

        return await self.pump_swap_executor.pump_sell(
            mint_id,
            int(amount or 0),
            pool=pool_address,
            creator=mint_state.get("pump_swap_creator") or owner,
            sim=sim,
            priority_micro_lamports=fee,
            slippage=float(SLIPPAGE_AMOUNT),
        )

    def _normalize_sell_instruction_error(self, venue: str, result: str) -> tuple[str, str]:
        code = _parse_instruction_error_code(result)
        if venue == "pump_swap":
            if code == 6023:
                return (
                    "pump_swap_liquidity_rejected",
                    "PumpSwap rejected the exit because the pool does not have enough token-side liquidity for this sell size.",
                )
            if code == 6004:
                return (
                    "pump_swap_slippage_rejected",
                    "PumpSwap rejected the exit because price moved outside the configured slippage guard.",
                )
        elif venue == "pump_fun" and code == 6003:
            return (
                "pump_fun_slippage_rejected",
                "Pump.fun rejected the exit because the expected SOL out fell below the configured minimum.",
            )

        if code is None:
            return (
                "instruction_error",
                f"{venue} rejected the exit on-chain with an instruction error.",
            )
        return (
            f"instruction_error_{code}",
            f"{venue} rejected the exit on-chain with instruction error {code}.",
        )

    async def load_blacklist(self):
        try:
            os.makedirs(self.dex_dir, exist_ok=True)
            blacklist_path = os.path.join(self.dex_dir, "blacklist.txt")
            if os.path.exists(blacklist_path):
                with open(blacklist_path, "r", encoding="utf-8") as f:
                    BLACKLIST.extend(f.read().splitlines())
            else:
                logging.warning(f"Blacklist file not found at {blacklist_path}.")
        except Exception as e:
            logging.error(f"Error loading blacklist: {e}")

    async def add_to_blacklist(self, owner):
        try:
            BLACKLIST.append(owner)
            os.makedirs(self.dex_dir, exist_ok=True)
            blacklist_path = os.path.join(self.dex_dir, "blacklist.txt")
            with open(blacklist_path, "a", encoding="utf-8") as f:
                f.write(f"{owner}\n")
            logging.info(f"{cc.LIGHT_BLUE}Added {owner} to blacklist.{cc.RESET}")
        except Exception as e:
            logging.error(f"Error adding to blacklist: {e}")

    async def save_result(self, result):
        try:
            self.config.paths.results_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config.paths.results_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(result, indent=2) + "\n")
        except Exception as e:
            logging.error(f"Error saving result: {e}")

    def _phase2_session_id(self, mint_id: str) -> str | None:
        swap_state = self.swap_folder.get(mint_id, {})
        if "session_id" in swap_state:
            return swap_state["session_id"]
        holding = self.holdings.get(mint_id, {})
        return holding.get("session_id")

    def _phase2_last_event_fingerprint(self, mint_id: str) -> str | None:
        return self.swap_folder.get(mint_id, {}).get("last_event_fingerprint")

    async def _phase2_start_session(
        self,
        mint_id: str,
        owner: str,
        *,
        trust_level: int | None = None,
        open_reason: str = "leaderboard_match",
        session_context: dict | None = None,
    ) -> str | None:
        if self.phase2 is None:
            return None
        session_id = self._phase2_session_id(mint_id)
        if session_id:
            return session_id
        session_id = await self.phase2.start_trade_session(
            mint_id=mint_id,
            owner=owner,
            runtime_mode=self.runtime_mode,
            trust_level=trust_level,
            leaderboard_version=self.leaderboard_version,
            open_reason=open_reason,
            session_context=session_context or {},
        )
        self.swap_folder[mint_id]["session_id"] = session_id
        self.swap_folder[mint_id]["session_closed"] = False
        return session_id

    async def _phase2_close_session(self, mint_id: str, *, status: str, reason: str, **summary):
        if self.phase2 is None:
            return
        session_id = self._phase2_session_id(mint_id)
        if not session_id:
            return
        if self.swap_folder.get(mint_id, {}).get("session_closed"):
            return
        await self.phase2.close_trade_session(
            session_id,
            status=status,
            close_reason=reason,
            buy_cost_lamports=summary.get("buy_cost_lamports"),
            sell_proceeds_lamports=summary.get("sell_proceeds_lamports"),
            realized_profit_lamports=summary.get("realized_profit_lamports"),
        )
        if mint_id in self.swap_folder:
            self.swap_folder[mint_id]["session_closed"] = True

    async def _phase2_record_decision(
        self,
        mint_id: str,
        owner: str | None,
        decision_type: str,
        reason: str,
        *,
        decision_inputs: dict | None = None,
        decision_outcome: dict | None = None,
    ):
        if self.phase2 is None:
            return
        await self.phase2.record_strategy_decision(
            session_id=self._phase2_session_id(mint_id),
            mint_id=mint_id,
            owner=owner,
            decision_type=decision_type,
            reason=reason,
            raw_event_fingerprint=self._phase2_last_event_fingerprint(mint_id),
            decision_inputs=decision_inputs or {},
            decision_outcome=decision_outcome or {},
        )

    async def _phase2_record_holding_snapshot(self, mint_id: str, status: str):
        if self.phase2 is None:
            return
        session_id = self._phase2_session_id(mint_id)
        if not session_id:
            return
        holding = self.holdings.get(mint_id, {})
        await self.phase2.record_holding_snapshot(
            session_id=session_id,
            mint_id=mint_id,
            owner=holding.get("owner"),
            status=status,
            token_balance=int(holding.get("token_balance", 0) or 0),
            buy_price=holding.get("buy_price"),
            cost_basis_lamports=int(holding.get("cost_basis_lamports", 0) or 0) or None,
            buy_fee_lamports=int(holding.get("buy_fee_lamports", 0) or 0) or None,
            buy_wallet_balance=int(holding.get("buy_wallet_balance", 0) or 0) or None,
            snapshot_payload=holding,
        )

    async def _phase2_ensure_strategy_profile(self):
        if self.phase2 is None:
            return
        profile_payload = serialize_strategy_profile(self.strategy_profile)
        await self.phase2.ensure_strategy_profile(
            profile_key=self.strategy_profile.name,
            profile_name=self.strategy_profile.name,
            version=str(self.strategy_profile.version or ""),
            definition=profile_payload,
            metadata={"runtime_mode": self.runtime_mode},
        )

    async def _phase2_record_position_intent(
        self,
        *,
        mint_id: str,
        owner: str | None,
        status: str,
        intended_size_lamports: int | None = None,
        intended_token_amount: int | None = None,
        context: dict | None = None,
    ):
        if self.phase2 is None:
            return
        session_id = self._phase2_session_id(mint_id)
        if not session_id:
            return
        await self.phase2.record_position_intent(
            session_id=session_id,
            mint_id=mint_id,
            owner=owner,
            strategy_profile=self.strategy_profile.name,
            status=status,
            intended_size_lamports=intended_size_lamports,
            intended_token_amount=intended_token_amount,
            context=context or {},
        )

    async def _phase2_update_position_entry_fill(
        self,
        *,
        mint_id: str,
        status: str,
        executed_size_lamports: int | None = None,
        executed_token_amount: int | None = None,
        actual_buy_price: str | None = None,
        context: dict | None = None,
    ):
        if self.phase2 is None:
            return
        session_id = self._phase2_session_id(mint_id)
        if not session_id:
            return
        await self.phase2.update_position_entry_fill(
            session_id=session_id,
            status=status,
            executed_size_lamports=executed_size_lamports,
            executed_token_amount=executed_token_amount,
            actual_buy_price=actual_buy_price,
            context=context or {},
        )

    async def _phase2_close_position_journal(
        self,
        *,
        mint_id: str,
        status: str,
        exit_reason: str | None = None,
        realized_profit_lamports: int | None = None,
        context: dict | None = None,
    ):
        if self.phase2 is None:
            return
        session_id = self._phase2_session_id(mint_id)
        if not session_id:
            return
        await self.phase2.close_position_journal(
            session_id=session_id,
            status=status,
            exit_reason=exit_reason,
            realized_profit_lamports=realized_profit_lamports,
            context=context or {},
        )

    async def _phase2_record_risk_event(
        self,
        mint_id: str,
        owner: str | None,
        event_type: str,
        severity: str,
        detail: str,
        *,
        context: dict | None = None,
    ):
        if self.phase2 is None:
            return
        await self.phase2.record_risk_event(
            session_id=self._phase2_session_id(mint_id),
            mint_id=mint_id,
            owner=owner,
            event_type=event_type,
            severity=severity,
            detail=detail,
            context=context or {},
        )

    def _refresh_operator_state(self):
        self.operator_state = load_control_state(self.config.paths.operator_control_file)
        return self.operator_state

    def _entries_paused(self) -> bool:
        self._refresh_operator_state()
        return self._emergency_stop_requested() or bool(self.operator_state.get("pause_new_entries", False))

    def _creator_is_allowed(self, owner: str) -> tuple[bool, str | None]:
        control = self._refresh_operator_state()
        blacklist = set(BLACKLIST) | set(control.get("blacklist_creators", []) or [])
        whitelist = set(control.get("whitelist_creators", []) or [])
        if owner in blacklist:
            return False, "blacklist"
        if whitelist and owner not in whitelist:
            return False, "not_whitelisted"
        return True, None

    def _holding_counts_as_open(self, holding: dict[str, Any]) -> bool:
        return bool(
            int(holding.get("token_balance", 0) or 0) > 0
            or holding.get("pending_buy_tx")
        )

    def _session_skip_reason(self, mint_id: str, owner: str) -> str | None:
        creator_allowed, creator_reason = self._creator_is_allowed(owner)
        if not creator_allowed:
            return creator_reason or "operator_control"
        if self.updating:
            return "leaderboard_updating"
        other_holdings_open = any(
            holding_mint != mint_id and self._holding_counts_as_open(holding)
            for holding_mint, holding in self.holdings.items()
        )
        if SINGLE_LOCK and other_holdings_open:
            return "single_lock"
        return None

    def _count_open_positions(self) -> int:
        total = 0
        for holding in self.holdings.values():
            if int(holding.get("token_balance", 0) or 0) > 0 or holding.get("pending_buy_tx"):
                total += 1
        return total

    def _count_creator_positions(self, owner: str) -> int:
        total = 0
        for holding in self.holdings.values():
            if holding.get("owner") != owner:
                continue
            if int(holding.get("token_balance", 0) or 0) > 0 or holding.get("pending_buy_tx"):
                total += 1
        return total

    def _reserve_floor_lamports(self) -> int:
        return self._sol_to_lamports(self.config.risk.wallet_reserve_floor_sol)

    def _drawdown_stop_triggered(self) -> bool:
        return self.daily_realized_profit_lamports <= -self._sol_to_lamports(self.config.risk.daily_drawdown_stop_sol)

    async def _record_runtime_failure(self, mint_id: str, owner: str | None, detail: str, *, severity: str = "error", context: dict | None = None):
        failure = {
            "mint_id": mint_id,
            "owner": owner,
            "detail": detail,
            "severity": severity,
            "context": context or {},
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        self.recent_failures.appendleft(failure)
        await self._phase2_record_risk_event(
            mint_id,
            owner,
            "runtime_failure",
            severity,
            detail,
            context=context or {},
        )
        await self.alerts.broadcast("Dexter runtime failure", f"{mint_id}: {detail}")

    async def _publish_runtime_snapshot(self):
        control = self._refresh_operator_state()
        tracked_mints = set(self.active_sessions) | set(self.holdings)
        force_sell_mints = list(control.get("force_sell_mints", []) or [])
        pruned_force_sell_mints = [mint for mint in force_sell_mints if mint in tracked_mints]
        if pruned_force_sell_mints != force_sell_mints:
            control["force_sell_mints"] = pruned_force_sell_mints
            try:
                self.operator_state = await asyncio.to_thread(
                    save_control_state,
                    self.config.paths.operator_control_file,
                    control,
                )
                control = self.operator_state
            except Exception:
                logging.exception("Unable to persist operator control state while publishing the trader snapshot.")
        sessions = []
        for mint_id, task in self.active_sessions.items():
            holding = self.holdings.get(mint_id, {})
            mint_state = self.swap_folder.get(mint_id, {})
            sessions.append(
                {
                    "mint_id": mint_id,
                    "owner": mint_state.get("owner") or holding.get("owner"),
                    "market": mint_state.get("market", holding.get("market", "pump_fun")),
                    "status": "running" if not task.done() else "done",
                    "price": str((mint_state.get("state") or {}).get("price", holding.get("buy_price", "0"))),
                }
            )
        snapshot = {
            "component": "trader",
            "status": "stopped" if self.stop_event.is_set() else "running",
            "runtime_mode": self.runtime_mode,
            "network": self.config.runtime.network,
            "strategy_profile": self.strategy_profile.name,
            "strategy_version": self.strategy_profile.version,
            "wallet": self.wallet,
            "wallets": {
                "trading_wallet": self.wallet,
                "hot_wallet": self.hot_wallet,
                "treasury_wallet": self.treasury_wallet,
            },
            "exposure": {
                **self._build_exposure_summary(),
                "daily_realized_profit_lamports": int(self.daily_realized_profit_lamports),
            },
            "active_session_count": len(self.active_sessions),
            "pending_positions": len(self.pending_buy_spend),
            "wslogs": self._build_embedded_wslogs_state(),
            "operator": {
                "pause_new_entries": bool(control.get("pause_new_entries", False)),
                "blacklist_creators": list(control.get("blacklist_creators", []) or []),
                "whitelist_creators": list(control.get("whitelist_creators", []) or []),
                "watchlist": list(control.get("watchlist_mints", []) or []),
                "force_sell_mints": list(control.get("force_sell_mints", []) or []),
            },
            "active_sessions": sessions,
            "holdings_preview": [
                {
                    "mint_id": mint_id,
                    "owner": holding.get("owner"),
                    "token_balance": int(holding.get("token_balance", 0) or 0),
                    "market": holding.get("market") or self.swap_folder.get(mint_id, {}).get("market", "pump_fun"),
                    "buy_price": holding.get("buy_price"),
                    "current_price": str(((self.swap_folder.get(mint_id, {}) or {}).get("state") or {}).get("price", "")) or None,
                    "cost_basis_lamports": int(holding.get("cost_basis_lamports", 0) or 0),
                    "bonding_curve": (self.swap_folder.get(mint_id, {}) or {}).get("bonding_curve"),
                }
                for mint_id, holding in list(self.holdings.items())[:10]
            ],
            "daily_realized_pnl_lamports": int(self.daily_realized_profit_lamports),
            "reserved_lamports": self._reserved_wallet_balance(),
            "recent_failures": list(self.recent_failures),
        }
        self._sync_all_managed_positions()
        try:
            await asyncio.to_thread(
                publish_runtime_snapshot,
                self.config.paths.trader_snapshot_file,
                snapshot,
            )
        except Exception:
            logging.exception("Trader runtime snapshot publish failed unexpectedly.")

    async def _publish_runtime_snapshot_loop(self):
        while not self.stop_event.is_set():
            try:
                await self._publish_runtime_snapshot()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Trader snapshot loop iteration failed.")
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=5)
            except asyncio.TimeoutError:
                continue

    def _embedded_wslogs_enabled(self) -> bool:
        return bool(self.config.runtime.enable_wslogs)

    def _embedded_wslogs_should_run(self) -> bool:
        return bool(self._embedded_wslogs_enabled() and not self.session_seed)

    def _build_embedded_wslogs_state(self) -> dict[str, Any]:
        task = self.embedded_wslogs_task
        runtime = self.embedded_wslogs
        status = "disabled"
        reason = None

        if self._embedded_wslogs_enabled():
            if self.session_seed:
                status = "skipped"
                reason = "seeded_session"
            elif task is not None and not task.done():
                status = str(getattr(runtime, "current_status", "") or "starting")
            elif runtime is not None:
                status = str(getattr(runtime, "current_status", "") or "stopped")
            else:
                status = "stopped"

        task_state = "missing"
        if task is not None:
            if task.cancelled():
                task_state = "cancelled"
            elif task.done():
                task_state = "done"
            else:
                task_state = "running"

        return {
            "enabled": self._embedded_wslogs_enabled(),
            "requested": self._embedded_wslogs_should_run(),
            "status": status,
            "reason": reason,
            "task_state": task_state,
            "active": bool(task is not None and not task.done()),
            "subscribed": bool(getattr(runtime, "subscribed", False)),
            "processed_logs": int(getattr(runtime, "processed_logs", 0) or 0),
            "last_event_at": getattr(runtime, "last_event_at", None),
            "last_error": getattr(runtime, "last_error", None),
            "last_checkup_at": self.embedded_wslogs_last_checkup_at,
        }

    def _embedded_wslogs_is_healthy(self) -> bool:
        task = self.embedded_wslogs_task
        runtime = self.embedded_wslogs
        if task is None or runtime is None or task.done():
            return False

        runtime_stop_event = getattr(runtime, "stop_event", None)
        if runtime_stop_event is not None and runtime_stop_event.is_set():
            return False
        if getattr(runtime, "last_error", None):
            return False

        status = str(getattr(runtime, "current_status", "") or "").lower()
        return bool(
            getattr(runtime, "subscribed", False)
            and status in {"connected", "streaming", "processing", "monitoring", "running"}
        )

    def _start_embedded_wslogs(self) -> None:
        if not self._embedded_wslogs_enabled():
            logging.info("Embedded wsLogs auto-start is disabled by DEXTER_ENABLE_WSLOGS=false.")
            return
        if self.session_seed:
            logging.info("Embedded wsLogs auto-start skipped for the seeded-session runtime.")
            return
        if self.embedded_wslogs_task is not None and not self.embedded_wslogs_task.done():
            return

        self.embedded_wslogs = DexBetterLogs(
            self.ws_url,
            db_dsn=self.db_dsn,
            app_config=self.config,
            enable_market=True,
        )
        self.embedded_wslogs_task = asyncio.create_task(
            self._run_embedded_wslogs(),
            name="dexter.wslogs",
        )
        self.embedded_wslogs_watchdog_task = asyncio.create_task(
            self._embedded_wslogs_checkup_loop(),
            name="dexter.wslogs.checkup",
        )
        logging.info("Embedded wsLogs supervision enabled for this Dexter trader runtime.")

    async def _run_embedded_wslogs(self):
        if self.embedded_wslogs is None:
            return
        try:
            await self.embedded_wslogs.run(manage_signal_handlers=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            detail = f"Embedded wsLogs runtime failed: {exc}"
            logging.error(detail)
            traceback.print_exc()
            raise RuntimeError(detail) from exc

        if not self.stop_event.is_set():
            raise RuntimeError("Embedded wsLogs runtime exited unexpectedly.")

    async def _check_embedded_wslogs_once(self):
        if not self._embedded_wslogs_should_run():
            return
        task = self.embedded_wslogs_task
        runtime = self.embedded_wslogs
        if task is None or runtime is None:
            return

        if task.done():
            if self.stop_event.is_set():
                return
            if task.cancelled():
                detail = "Embedded wsLogs runtime was cancelled unexpectedly."
            else:
                exc = task.exception()
                detail = (
                    f"Embedded wsLogs runtime stopped unexpectedly: {exc}"
                    if exc is not None
                    else "Embedded wsLogs runtime stopped unexpectedly."
                )
            logging.error(detail)
            await self._record_runtime_failure(
                "wslogs",
                None,
                detail,
                context={"component": "collector", "wslogs": self._build_embedded_wslogs_state()},
            )
            raise RuntimeError(detail)

        last_error = getattr(runtime, "last_error", None)
        if last_error:
            if last_error != self.embedded_wslogs_last_reported_error:
                self.embedded_wslogs_last_reported_error = last_error
                detail = f"wsLogs reported error: {last_error}"
                logging.error(detail)
                await self._record_runtime_failure(
                    "wslogs",
                    None,
                    detail,
                    severity="warn",
                    context={"component": "collector", "wslogs": self._build_embedded_wslogs_state()},
                )
        else:
            self.embedded_wslogs_last_reported_error = None

        if self._embedded_wslogs_is_healthy():
            self.embedded_wslogs_last_checkup_at = _utc_now_iso()
            logging.info("Websocket logs are actively monitored, checkup complete")

    async def _embedded_wslogs_checkup_loop(self):
        while not self.stop_event.is_set():
            await self._check_embedded_wslogs_once()
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(),
                    timeout=self.embedded_wslogs_checkup_interval_seconds,
                )
            except asyncio.TimeoutError:
                continue

    async def _shutdown_embedded_wslogs(self):
        if self.embedded_wslogs is not None:
            self.embedded_wslogs._request_shutdown()

        current_task = asyncio.current_task()
        wait_tasks = [
            task
            for task in (self.embedded_wslogs_watchdog_task, self.embedded_wslogs_task)
            if task is not None and task is not current_task
        ]
        if wait_tasks:
            await asyncio.gather(*wait_tasks, return_exceptions=True)

    async def init_db_pool(self):
        if self.skip_database_init:
            logging.info("Skipping database pool initialization for the seeded session flow.")
            return
        self.pool = await asyncpg.create_pool(
            dsn=self.db_dsn,
            min_size=self.config.database.min_pool_size,
            max_size=self.config.database.max_pool_size,
        )
        if self.phase2 is not None:
            self.phase2.bind_pool(self.pool)
            await self.phase2.ensure_schema()
        logging.info("Connection pool initialized.")

    async def close_db_pool(self):
        if self.pool:
            await self.pool.close()
            logging.info("Connection pool closed.")

    def _seed_creator_profile(self):
        if not self.session_seed:
            return None

        owner = self.session_seed["owner"]
        profit_target_pct = Decimal(str(self.session_seed.get("profit_target_pct", PRICE_STEP_UNITS)))
        trust_level = int(self.session_seed.get("trust_level", 1) or 1)
        median_peak_market_cap = 50000 if trust_level >= 2 else 0
        return owner, {
            "mint_count": 1,
            "median_peak_market_cap": median_peak_market_cap,
            "median_success_ratio": float(max(Decimal(str(PRICE_STEP_UNITS)), profit_target_pct)),
            "performance_score": 1.0,
            "trust_factor": 1.0,
            "total_swaps": 0,
        }

    async def _prime_seed_session(self, *, start_monitor: bool = True):
        if not self.session_seed:
            return

        owner, creator_entry = self._seed_creator_profile()
        mint_id = self.session_seed["mint_id"]
        name = self.session_seed.get("name") or mint_id[:8]
        bonding_curve = self.session_seed.get("bonding_curve")
        trust_level = int(self.session_seed.get("trust_level", 1) or 1)
        buy_price = Decimal(str(self.session_seed.get("buy_price", "0") or "0"))
        token_balance = int(self.session_seed.get("token_balance", 0) or 0)
        shadow_position = bool(self.session_seed.get("shadow_position", self.shadow_mode))
        created_at = int(time.time())

        self.leaderboard = {owner: creator_entry}
        self.swap_folder[mint_id] = {
            "name": name,
            "owner": owner,
            "bonding_curve": bonding_curve,
            "market": self.session_seed.get("market", "pump_fun"),
            "seeded_session": True,
        }

        if buy_price > 0:
            self.swap_folder[mint_id]["state"] = {
                "price": buy_price,
                "open_price": buy_price,
                "high_price": buy_price,
                "mc": await self._get_market_cap(buy_price),
                "price_usd": float(buy_price * self.analyzer.sol_price_usd),
                "last_tx_time": f"{created_at}.000",
                "holders": {self.wallet: {"balance": token_balance, "balance_changes": [{"type": "buy", "price_was": buy_price}]}} if token_balance > 0 else {},
                "price_history": {f"{created_at}.000": buy_price},
                "tx_counts": {"swaps": 1 if token_balance > 0 else 0, "buys": 1 if token_balance > 0 else 0, "sells": 0},
                "created": created_at,
            }

        session_id = await self._phase2_start_session(
            mint_id,
            owner,
            trust_level=trust_level,
            open_reason="seeded_create_session",
            session_context={"seeded_session": True},
        )
        if token_balance > 0 and buy_price > 0:
            self._update_holding(
                mint_id,
                owner=owner,
                trust_level=trust_level,
                session_id=session_id,
                token_balance=token_balance,
                buy_price=buy_price,
                shadow_position=shadow_position,
                cost_basis_lamports=int(self.session_seed.get("cost_basis_lamports", 0) or 0) or None,
            )
            self._sync_managed_position(mint_id, status="open")
            await self._phase2_record_holding_snapshot(mint_id, "seeded_buy")

        await self._phase2_record_decision(
            mint_id,
            owner,
            "session_seeded",
            "create_cli_handoff",
            decision_outcome={
                "token_balance": token_balance,
                "buy_price": str(buy_price) if buy_price > 0 else None,
                "market": self.swap_folder[mint_id]["market"],
            },
        )

        if start_monitor and mint_id not in self.active_sessions:
            session_task = asyncio.create_task(self.monitor_mint_session(mint_id, owner))
            self.active_sessions[mint_id] = session_task

    async def subscribe(self, program=PUMP_FUN):
        while not self.stop_event.is_set():
            if self.leaderboard is not None:
                ws = None
                try:
                    async with websockets.connect(self.ws_url, ping_interval=2, ping_timeout=15) as ws:
                        await ws.send(json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "method": "logsSubscribe",
                                "id": 1,
                                "params": [
                                    {"mentions": [program]},
                                    {"commitment": "processed"}
                                ]
                            }))
                        response = json.loads(await ws.recv())

                        if 'result' in response:
                            logging.info("Dexter successfully connected to Solana's Network ✔")

                        async for message in ws:
                            if self.stop_event.is_set():
                                break
                            hMessage = json.loads(message)
                            await self.logs.put([hMessage, program])

                except websockets.ConnectionClosedError:
                    if self.stop_event.is_set():
                        break
                    logging.error(f"{cc.RED}Connection closed when subscribing to {program}.{cc.RESET}")
                    await asyncio.sleep(5)
                except TimeoutError:
                    if self.stop_event.is_set():
                        break
                    logging.error(f"{cc.RED}TimeoutError when subscribing to {program}.{cc.RESET}")
                    await asyncio.sleep(5)
                except Exception as e:
                    if self._is_expected_shutdown_error(e):
                        break
                    logging.error(f"{cc.RED}Error when subscribing to {program}, {e}{cc.RESET}")
                    traceback.print_exc()
                finally:
                    if ws:
                        await ws.close()
                    if not self.stop_event.is_set():
                        await asyncio.sleep(1)
            else:
                await asyncio.sleep(0.5)

    async def _process_peak_change(self, open_price, peak_price):
        peak_pct_change = 0.0
        if peak_price > 0 and open_price > 0:
            peak_pct_change = float(((peak_price - open_price) / open_price) * 100)
        return peak_pct_change

    def _update_rolling_window(self, rolling_buffer, current_time, price, swaps):
        while rolling_buffer and (current_time - rolling_buffer[0][0]).total_seconds() >= ROLLING_WINDOW_SIZE:
            rolling_buffer.popleft()
        rolling_buffer.append((current_time, price, swaps))

    async def _compute_price(self, vsr, vtr):
        vsr = Decimal(vsr) / Decimal(1e9)
        vtr = Decimal(vtr) / Decimal(1e6)
        if vtr == 0:
            return Decimal('0')
        return vsr / vtr

    async def _get_market_cap(self, current_price):
        price_in_usd = Decimal(current_price) * self.analyzer.sol_price_usd
        return self.analyzer.total_supply * price_in_usd

    def _compute_composite_score(self, rolling_buffer):
        """
            Compute the composite score for a token based on
            price trend and transaction momentum.
        """
        if len(rolling_buffer) < 2:
            return 0
        t0, price0, swaps0 = rolling_buffer[0]
        t1, price1, swaps1 = rolling_buffer[-1]
        dt = (t1 - t0).total_seconds()
        if dt == 0:
            return 0

        price_change_pct = 0
        if price0 > 0:
            price_change_pct = float(((price1 - price0) / price0) * 100)

        swaps_diff = float(swaps1 - swaps0)
        tx_momentum = (swaps_diff / dt) * 10

        raw_score = (PRICE_TREND_WEIGHT * Decimal(price_change_pct)) + \
                    (TX_MOMENTUM_WEIGHT * Decimal(tx_momentum))

        score = max(0, min(100, float(raw_score)))
        return score

    async def _validate_result(self, owner, result):
        if result in ["malicious", "sell>buy"]:
            await self.add_to_blacklist(owner)
            logging.info(f"{cc.RED}Blacklisted {owner} for {result}.{cc.RESET}")
        elif result in ["safe", "stagnant", "drop-time"]:
            logging.info(f"{cc.GREEN}Safe {owner} for {result}.{cc.RESET}")

    def _reserved_wallet_balance(self) -> int:
        return int(sum(self.pending_buy_spend.values()))

    def _available_wallet_balance(self) -> int:
        return max(0, int(self.wallet_balance - self._reserved_wallet_balance()))

    def _reserve_buy_spend(self, mint_id: str, lamports: int):
        self.pending_buy_spend[mint_id] = int(max(0, lamports))

    def _release_buy_spend(self, mint_id: str) -> int:
        return int(self.pending_buy_spend.pop(mint_id, 0))

    def _finalize_buy_spend(self, mint_id: str, confirmed_wallet_balance: int | None = None):
        reserved_cost = self._release_buy_spend(mint_id)
        if reserved_cost <= 0 and confirmed_wallet_balance is None:
            return

        if reserved_cost > 0:
            self._record_spend(mint_id, reserved_cost)

        old_balance = self.wallet_balance
        if confirmed_wallet_balance is not None:
            self.wallet_balance = int(confirmed_wallet_balance)
        elif reserved_cost > 0:
            self.wallet_balance = max(0, int(self.wallet_balance - reserved_cost))

        logging.info(
            f"Wallet difference: {old_balance} -> {self.wallet_balance} = "
            f"{self.wallet_balance - old_balance}"
        )

    def _reset_daily_spend_if_needed(self):
        today = datetime.datetime.now(datetime.timezone.utc).date()
        if today != self.daily_spend_date:
            self.daily_spend_date = today
            self.daily_spend_lamports = 0
            self.session_spend.clear()

    def _sol_to_lamports(self, value: Decimal) -> int:
        return int(value * Decimal("1000000000"))

    def _lamports_to_sol(self, lamports: int) -> Decimal:
        return Decimal(int(lamports)) / Decimal("1000000000")

    def _lamports_to_usd(self, lamports: int) -> Decimal:
        return self._lamports_to_sol(lamports) * Decimal(str(self.analyzer.sol_price_usd or 0))

    def _build_realized_profit_snapshot(self, cost_basis_lamports: int, proceeds_lamports: int) -> dict:
        cost_basis_lamports = int(cost_basis_lamports or 0)
        proceeds_lamports = int(proceeds_lamports or 0)
        realized_profit_lamports = proceeds_lamports - cost_basis_lamports
        realized_profit_pct = 0.0
        if cost_basis_lamports > 0:
            realized_profit_pct = float(
                (Decimal(realized_profit_lamports) / Decimal(cost_basis_lamports)) * Decimal("100")
            )
        return {
            "cost_basis_lamports": cost_basis_lamports,
            "proceeds_lamports": proceeds_lamports,
            "profit_lamports": realized_profit_lamports,
            "profit_sol": self._lamports_to_sol(realized_profit_lamports),
            "profit_usd": self._lamports_to_usd(realized_profit_lamports),
            "profit_pct": realized_profit_pct,
        }

    def _record_spend(self, mint_id: str, lamports: int):
        if lamports <= 0:
            return
        self._reset_daily_spend_if_needed()
        self.session_spend[mint_id] += int(lamports)
        self.daily_spend_lamports += int(lamports)

    def _entry_control_error(self, mint_id: str, owner: str, reserved_cost: int) -> str | None:
        open_positions = self._count_open_positions()
        if open_positions >= int(self.config.risk.max_concurrent_sessions):
            return f"max concurrent sessions reached ({open_positions} >= {self.config.risk.max_concurrent_sessions})"
        owner_positions = self._count_creator_positions(owner)
        if owner_positions >= int(self.config.risk.per_creator_max_sessions):
            return f"per-creator session limit reached for {owner} ({owner_positions} >= {self.config.risk.per_creator_max_sessions})"
        available_after_trade = self._available_wallet_balance() - int(reserved_cost)
        if self.runtime_mode == "live" and available_after_trade < self._reserve_floor_lamports():
            return (
                f"wallet reserve floor would be crossed ({available_after_trade} < {self._reserve_floor_lamports()} lamports)"
            )
        if self._drawdown_stop_triggered():
            return (
                f"daily drawdown stop reached ({self.daily_realized_profit_lamports} <= "
                f"-{self._sol_to_lamports(self.config.risk.daily_drawdown_stop_sol)})"
            )
        return None

    def _evaluate_entry_strategy(self, mint_id: str, owner: str):
        creator_entry = self.leaderboard.get(owner) if self.leaderboard else None
        token_state = (self.swap_folder.get(mint_id, {}) or {}).get("state") or {}
        return evaluate_entry(
            profile=self.strategy_profile,
            creator_entry=creator_entry,
            token_state=token_state,
            owner=owner,
            wallet_address=self.wallet,
        )

    def _enforce_spend_caps(self, mint_id: str, lamports: int) -> str | None:
        self._reset_daily_spend_if_needed()
        per_trade_cap = self._sol_to_lamports(self.config.risk.per_trade_sol_cap)
        session_cap = self._sol_to_lamports(self.config.risk.session_sol_cap)
        daily_cap = self._sol_to_lamports(self.config.risk.daily_sol_cap)

        if lamports > per_trade_cap:
            return (
                f"per-trade cap exceeded ({lamports} > {per_trade_cap} lamports, "
                f"{self.config.risk.per_trade_sol_cap} SOL cap)"
            )
        if self.session_spend[mint_id] + lamports > session_cap:
            return (
                f"session cap exceeded for {mint_id} "
                f"({self.session_spend[mint_id] + lamports} > {session_cap} lamports)"
            )
        if self.daily_spend_lamports + lamports > daily_cap:
            return (
                f"daily cap exceeded ({self.daily_spend_lamports + lamports} > "
                f"{daily_cap} lamports)"
            )
        return None

    def _emergency_stop_requested(self) -> bool:
        return self.config.runtime.emergency_stop_file.exists()

    def _update_holding(self, mint_id: str, **updates):
        holding = dict(self.holdings.get(mint_id, {}))
        int_fields = {
            "token_balance",
            "trust_level",
            "cost_basis_lamports",
            "buy_fee_lamports",
            "buy_wallet_balance",
        }
        for key, value in updates.items():
            if value is None:
                holding.pop(key, None)
                continue
            if key in int_fields:
                holding[key] = int(value)
            elif key == "buy_price":
                holding[key] = str(value)
            else:
                holding[key] = value
        holding["mode"] = self.execution_label
        self.holdings[mint_id] = holding
        return holding

    def _build_managed_position_record(
        self,
        mint_id: str,
        *,
        existing: dict[str, Any] | None = None,
        status: str | None = None,
        close_context: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        existing = dict(existing or {})
        holding = dict(self.holdings.get(mint_id, {}))
        swap_state = dict(self.swap_folder.get(mint_id, {}))
        token_state = dict(swap_state.get("state") or {})
        if not holding and not existing:
            return None

        record_status = status or existing.get("status") or ("open" if holding else "closed")
        token_balance = int(holding.get("token_balance", existing.get("token_balance", 0)) or 0)
        buy_price = holding.get("buy_price") or existing.get("buy_price")
        current_price = token_state.get("price")
        if current_price is None:
            current_price = existing.get("current_price") or existing.get("last_price") or buy_price

        record = {
            "mint_id": mint_id,
            "name": swap_state.get("name") or existing.get("name"),
            "owner": holding.get("owner") or swap_state.get("owner") or existing.get("owner"),
            "network": self.config.runtime.network,
            "runtime_mode": self.runtime_mode,
            "execution_mode": self.execution_label,
            "status": record_status,
            "market": (
                holding.get("market")
                or swap_state.get("market")
                or existing.get("market")
                or "pump_fun"
            ),
            "bonding_curve": swap_state.get("bonding_curve") or existing.get("bonding_curve"),
            "token_balance": token_balance,
            "buy_price": str(buy_price) if buy_price not in (None, "") else None,
            "current_price": str(current_price) if current_price not in (None, "") else None,
            "last_price": str(current_price) if current_price not in (None, "") else None,
            "cost_basis_lamports": int(holding.get("cost_basis_lamports", existing.get("cost_basis_lamports", 0)) or 0),
            "buy_fee_lamports": int(holding.get("buy_fee_lamports", existing.get("buy_fee_lamports", 0)) or 0),
            "buy_wallet_balance": int(holding.get("buy_wallet_balance", existing.get("buy_wallet_balance", 0)) or 0),
            "trust_level": int(holding.get("trust_level", existing.get("trust_level", 1)) or 1),
            "session_id": holding.get("session_id") or existing.get("session_id"),
            "shadow_position": bool(holding.get("shadow_position", existing.get("shadow_position", self.shadow_mode))),
            "opened_at": existing.get("opened_at") or _utc_now_iso(),
            "updated_at": _utc_now_iso(),
        }

        unrealized_lamports = _position_unrealized_profit_lamports(record)
        if unrealized_lamports is not None:
            record["unrealized_profit_lamports"] = int(unrealized_lamports)

        if close_context:
            record.update(close_context)
        return record

    def _sync_managed_position(self, mint_id: str, *, status: str | None = None) -> dict[str, Any] | None:
        store = load_managed_positions(self.config)
        positions = dict(store.get("positions") or {})
        record = self._build_managed_position_record(
            mint_id,
            existing=positions.get(mint_id),
            status=status,
        )
        if record is None:
            return None
        positions[mint_id] = record
        store["positions"] = positions
        save_managed_positions(self.config, store)
        return record

    def _sync_all_managed_positions(self) -> None:
        if not self.holdings:
            return
        store = load_managed_positions(self.config)
        positions = dict(store.get("positions") or {})
        changed = False
        for mint_id in self.holdings:
            record = self._build_managed_position_record(
                mint_id,
                existing=positions.get(mint_id),
                status="open",
            )
            if record is None:
                continue
            positions[mint_id] = record
            changed = True
        if changed:
            store["positions"] = positions
            save_managed_positions(self.config, store)

    def _mark_managed_position_closed(
        self,
        mint_id: str,
        *,
        reason: str,
        profit_snapshot: dict[str, Any] | None = None,
        sell_price: Decimal | None = None,
        sell_fee_lamports: int | None = None,
        sell_proceeds_lamports: int | None = None,
        tx_id: str | None = None,
    ) -> None:
        store = load_managed_positions(self.config)
        positions = dict(store.get("positions") or {})
        close_context = {
            "status": "closed",
            "closed_at": _utc_now_iso(),
            "close_reason": reason,
            "sell_price": str(sell_price) if sell_price not in (None, "") else None,
            "sell_fee_lamports": int(sell_fee_lamports or 0),
            "sell_proceeds_lamports": int(sell_proceeds_lamports or 0),
            "tx_id": tx_id,
        }
        if profit_snapshot:
            close_context["realized_profit_lamports"] = int(profit_snapshot.get("profit_lamports", 0) or 0)
            close_context["realized_profit_sol"] = str(profit_snapshot.get("profit_sol"))
            close_context["realized_profit_usd"] = str(profit_snapshot.get("profit_usd"))
            close_context["profit_pct"] = float(profit_snapshot.get("profit_pct", 0.0) or 0.0)

        record = self._build_managed_position_record(
            mint_id,
            existing=positions.get(mint_id),
            status="closed",
            close_context=close_context,
        )
        if record is None:
            return
        positions[mint_id] = record
        store["positions"] = positions
        save_managed_positions(self.config, store)

    async def _refresh_pump_fun_price_for_mint(self, mint_id: str) -> Decimal | None:
        if self.pump_swap is None:
            return None
        mint_state = self.swap_folder.get(mint_id, {})
        bonding_curve = mint_state.get("bonding_curve")
        if not bonding_curve:
            return None
        owner = (
            self.holdings.get(mint_id, {}).get("owner")
            or mint_state.get("owner")
        )
        try:
            from DexLab.pump_fun.pump_swap import PublicKey  # type: ignore
            curve_state, _creator_vault = await self.pump_swap.get_curve_context(
                PublicKey.from_string(bonding_curve),
                fallback_creator=owner,
                allow_creator_fallback=bool(owner),
            )
        except Exception:
            return None
        if curve_state is None:
            return None
        price = await self._compute_price(
            getattr(curve_state, "virtual_sol_reserves", 0),
            getattr(curve_state, "virtual_token_reserves", 0),
        )
        if price <= 0:
            return None
        state = mint_state.setdefault("state", {})
        timestamp = int(time.time())
        unique_timestamp = self._next_unique_timestamp(mint_id, timestamp)
        state["price"] = price
        state.setdefault("open_price", price)
        state["high_price"] = max(Decimal(str(state.get("high_price", price))), price)
        state["mc"] = await self._get_market_cap(price)
        state["price_usd"] = float(price * self.analyzer.sol_price_usd)
        state["last_tx_time"] = unique_timestamp
        state.setdefault("price_history", {})[unique_timestamp] = price
        mint_state["market"] = mint_state.get("market", "pump_fun")
        return price

    async def _estimate_shadow_buy_fill(
        self,
        mint_id: str,
        *,
        bonding_curve: str | None,
        owner: str,
        lamports: int,
        fallback_price: Decimal,
    ) -> tuple[int, Decimal]:
        if self.pump_swap is not None and bonding_curve:
            try:
                from DexLab.pump_fun.pump_swap import PublicKey  # type: ignore
            except Exception:
                PublicKey = None  # type: ignore[assignment]
            if PublicKey is not None:
                try:
                    curve_state, _creator_vault = await self.pump_swap.get_curve_context(
                        PublicKey.from_string(bonding_curve),
                        fallback_creator=owner,
                        allow_creator_fallback=True,
                    )
                    estimated_tokens = self.pump_swap.quote_buy_exact_sol_in_tokens_out(curve_state, lamports)
                    shadow_price = fallback_price
                    if estimated_tokens > 0:
                        shadow_price = Decimal(str(lamports)) / Decimal("1000000000")
                        shadow_price = shadow_price / (Decimal(int(estimated_tokens)) / Decimal("1000000"))
                        return int(estimated_tokens), shadow_price
                except Exception:
                    pass

        shadow_price = fallback_price if fallback_price > 0 else Decimal("0.000000001")
        estimated_tokens = 1
        if shadow_price > 0:
            estimated_tokens = max(
                1,
                int(((Decimal(lamports) / Decimal("1000000000")) / shadow_price) * Decimal("1000000")),
            )
        return estimated_tokens, shadow_price

    def _build_exposure_summary(self):
        self._reset_daily_spend_if_needed()

        active_positions = 0
        live_positions = 0
        shadow_positions = 0
        pending_positions = 0

        for holding in self.holdings.values():
            token_balance = int(holding.get("token_balance", 0) or 0)
            pending_buy_tx = holding.get("pending_buy_tx")

            if token_balance <= 0 and pending_buy_tx:
                pending_positions += 1
                continue

            if token_balance <= 0 and not holding.get("shadow_position"):
                continue

            active_positions += 1
            if holding.get("shadow_position"):
                shadow_positions += 1
            else:
                live_positions += 1

        session_spend = {
            mint_id: lamports
            for mint_id, lamports in self.session_spend.items()
            if lamports > 0
        }

        return {
            "active_positions": active_positions,
            "live_positions": live_positions,
            "shadow_positions": shadow_positions,
            "pending_positions": pending_positions,
            "reserved_lamports": self._reserved_wallet_balance(),
            "wallet_balance": int(self.wallet_balance),
            "available_lamports": self._available_wallet_balance(),
            "daily_spend_lamports": int(self.daily_spend_lamports),
            "session_spend": session_spend,
        }

    def _log_exposure_summary(self, context: str):
        summary = self._build_exposure_summary()
        session_items = sorted(summary["session_spend"].items())
        session_preview = ", ".join(f"{mint}:{lamports}" for mint, lamports in session_items[:5]) or "none"
        if len(session_items) > 5:
            session_preview = f"{session_preview}, +{len(session_items) - 5} more"

        logging.info(
            "Exposure %s | positions=%s live=%s shadow=%s pending=%s reserved=%s wallet=%s available=%s daily_spend=%s sessions=%s",
            context,
            summary["active_positions"],
            summary["live_positions"],
            summary["shadow_positions"],
            summary["pending_positions"],
            summary["reserved_lamports"],
            summary["wallet_balance"],
            summary["available_lamports"],
            summary["daily_spend_lamports"],
            session_preview,
        )

    async def _report_exposure_loop(self):
        while not self.stop_event.is_set():
            if self.holdings or self.pending_buy_spend or self.daily_spend_lamports > 0:
                self._log_exposure_summary("heartbeat")
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=60)
            except asyncio.TimeoutError:
                continue

    async def _resolve_position_for_shutdown(self, mint_id: str):
        holding = dict(self.holdings.get(mint_id, {}))
        token_balance = int(holding.get("token_balance", 0) or 0)
        buy_price = Decimal(str(holding.get("buy_price", "0") or "0"))
        pending_buy_tx = holding.get("pending_buy_tx")

        if token_balance > 0 and buy_price > 0:
            return holding, token_balance, buy_price

        if not pending_buy_tx or not self.swaps:
            return holding, token_balance, buy_price

        logging.warning(
            "Resolving pending buy for %s during shutdown using transaction %s.",
            mint_id,
            pending_buy_tx,
        )
        results = await self.swaps.get_swap_tx(
            pending_buy_tx,
            mint_id,
            tx_type="buy",
            max_retries=2,
            retry_interval=0.5,
        )
        if not results or isinstance(results, str):
            return holding, token_balance, buy_price

        token_balance = int(results.get("balance", 0) or 0)
        buy_price = Decimal(str(results.get("price", 0) or "0"))
        confirmed_wallet_balance = results.get("sol_balance")
        confirmed_cost_lamports = int(results.get("cost_lamports", 0) or 0)
        confirmed_buy_fee_lamports = int(results.get("fee_lamports", 0) or 0)
        reserved_cost = int(self.pending_buy_spend.get(mint_id, 0) or 0)

        if token_balance > 0 and buy_price > 0:
            if mint_id in self.pending_buy_spend:
                self._finalize_buy_spend(mint_id, confirmed_wallet_balance)
            holding = self._update_holding(
                mint_id,
                token_balance=token_balance,
                buy_price=buy_price,
                cost_basis_lamports=confirmed_cost_lamports or reserved_cost,
                buy_fee_lamports=confirmed_buy_fee_lamports or None,
                buy_wallet_balance=confirmed_wallet_balance,
                pending_buy_tx=None,
            )
            self._sync_managed_position(mint_id, status="open")
            session_id = self._phase2_session_id(mint_id)
            buy_order_id = holding.get("buy_order_id")
            if self.phase2 is not None and session_id is not None and buy_order_id is not None:
                await self.phase2.update_order(
                    int(buy_order_id),
                    status="filled",
                    tx_id=pending_buy_tx,
                )
                await self.phase2.record_fill(
                    order_id=int(buy_order_id),
                    session_id=session_id,
                    mint_id=mint_id,
                    side="buy",
                    tx_id=pending_buy_tx,
                    fill_price=str(buy_price),
                    token_amount=token_balance,
                    cost_lamports=int(holding.get("cost_basis_lamports", 0) or 0),
                    fee_lamports=int(holding.get("buy_fee_lamports", 0) or 0),
                    wallet_balance=confirmed_wallet_balance,
                    context={"shutdown_resolution": True},
                )
                await self._phase2_update_position_entry_fill(
                    mint_id=mint_id,
                    status="filled",
                    executed_size_lamports=int(holding.get("cost_basis_lamports", 0) or 0),
                    executed_token_amount=token_balance,
                    actual_buy_price=str(buy_price),
                    context={"shutdown_resolution": True},
                )
                await self._phase2_record_holding_snapshot(mint_id, "buy_confirmed")
        return holding, token_balance, buy_price

    async def _liquidate_open_positions(self, reason: str):
        if not self.holdings:
            return

        unresolved = []
        failed = []
        self._log_exposure_summary(f"{reason}-start")

        for mint_id in list(self.holdings.keys()):
            holding = dict(self.holdings.get(mint_id, {}))
            owner = holding.get("owner", "unknown")
            trust_level = int(holding.get("trust_level", 1) or 1)
            holding, token_balance, buy_price = await self._resolve_position_for_shutdown(mint_id)

            if token_balance <= 0 or buy_price <= 0:
                released_lamports = self._release_buy_spend(mint_id)
                if released_lamports > 0:
                    logging.warning(
                        "Released %s lamports reserved for %s during %s because no confirmed position was found.",
                        released_lamports,
                        mint_id,
                        reason,
                    )
                self.holdings.pop(mint_id, None)
                self._mark_managed_position_closed(
                    mint_id,
                    reason=f"{reason}_unresolved",
                    sell_proceeds_lamports=0,
                )
                await self._phase2_close_session(mint_id, status="cleared", reason=f"{reason}_unresolved")
                self.swap_folder.pop(mint_id, None)
                unresolved.append(mint_id)
                continue

            result = await self.sell(mint_id, token_balance, reason, owner, trust_level, buy_price)
            if result == "migrated_monitoring":
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "shutdown_sell_deferred",
                    reason,
                    decision_outcome={"result": result},
                )
                unresolved.append(mint_id)
                continue
            if mint_id in self.holdings:
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "shutdown_sell_failed",
                    reason,
                    decision_outcome={"result": result or "unknown"},
                )
                failed.append(f"{mint_id}:{result or 'unknown'}")
                continue
            self.swap_folder.pop(mint_id, None)

        if unresolved:
            logging.warning(
                "Cleared unresolved positions during %s: %s",
                reason,
                ", ".join(unresolved),
            )
        if failed:
            logging.warning(
                "Positions still open after %s liquidation attempt: %s",
                reason,
                ", ".join(failed),
            )

        self._log_exposure_summary(f"{reason}-end")

    async def get_latest_price(self, mint_id: str) -> Decimal:
        """
        Fetch the latest price for a given mint ID from swap folder.
        """
        try:
            if mint_id in self.swap_folder:
                state = self.swap_folder[mint_id].get("state", {})
                price = state.get("price", Decimal('0'))
                return price
            logging.info(f"No price data available for {mint_id}. Returning 0.")
            return Decimal('0')
        except Exception as e:
            logging.error(f"Error fetching latest price for {mint_id}: {e}")
            return Decimal('0')

    async def buy(self, mint_id, trust_level, owner):
        try:
            session_id = await self._phase2_start_session(
                mint_id,
                owner,
                trust_level=trust_level,
                session_context={"wallet": self.wallet},
            )
            await self._phase2_ensure_strategy_profile()
            if self._entries_paused():
                logging.warning(
                    f"{cc.RED}Emergency stop or operator pause is active. "
                    f"Skipping new buy for {mint_id}.{cc.RESET}"
                )
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "buy_blocked",
                    "entries_paused",
                    decision_inputs={
                        "emergency_stop_file": str(self.config.runtime.emergency_stop_file),
                        "operator_pause": bool(self.operator_state.get("pause_new_entries", False)),
                    },
                )
                await self._phase2_record_risk_event(mint_id, owner, "buy_blocked", "warn", "entries_paused")
                return "emergency_stop"

            self._update_holding(
                mint_id,
                owner=owner,
                trust_level=trust_level,
                shadow_position=False,
                session_id=session_id,
            )
            bonding_curve = self.swap_folder.get(mint_id, {})["bonding_curve"]

            if not bonding_curve:
                logging.info(f"{cc.RED}Bonding curve not found for {mint_id}.{cc.RESET}")
                self.holdings.pop(mint_id, None)
                return
            
            # Here change buy price
            amount = AMOUNT_BUY_TL_1 if trust_level == 1 else AMOUNT_BUY_TL_2
            lamports = await usd_to_lamports(float(amount), self.analyzer.sol_price_usd)
            fee, estimated_fee_lamports = await self._estimate_pump_fun_priority_fee(
                mint_id,
                bonding_curve,
            )
            reserved_cost = lamports + estimated_fee_lamports
            cap_error = self._enforce_spend_caps(mint_id, reserved_cost)
            if cap_error:
                self.holdings.pop(mint_id, None)
                logging.warning(f"{cc.RED}Skipping buy for {mint_id}: {cap_error}.{cc.RESET}")
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "buy_blocked",
                    "spend_cap_exceeded",
                    decision_inputs={"reserved_cost": reserved_cost},
                    decision_outcome={"error": cap_error},
                )
                await self._phase2_record_risk_event(mint_id, owner, "buy_blocked", "warn", cap_error)
                return "spend_cap_exceeded"

            exposure_error = self._entry_control_error(mint_id, owner, reserved_cost)
            if exposure_error:
                self.holdings.pop(mint_id, None)
                logging.warning(f"{cc.RED}Skipping buy for {mint_id}: {exposure_error}.{cc.RESET}")
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "buy_blocked",
                    "risk_control",
                    decision_inputs={"reserved_cost": reserved_cost},
                    decision_outcome={"error": exposure_error},
                )
                await self._phase2_record_risk_event(mint_id, owner, "buy_blocked", "warn", exposure_error)
                return "risk_control"

            strategy_eval = self._evaluate_entry_strategy(mint_id, owner)
            if not strategy_eval.should_buy:
                self.holdings.pop(mint_id, None)
                logging.info(
                    f"{cc.YELLOW}Strategy kept Dexter out of {mint_id}: "
                    f"{', '.join(strategy_eval.blocking_reasons) or 'no entry signal yet'} "
                    f"(score={strategy_eval.total_score:.2f}, creator={strategy_eval.creator_score:.2f}, token={strategy_eval.token_score:.2f}).{cc.RESET}"
                )
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "buy_blocked",
                    "strategy_v2",
                    decision_inputs={"trust_level": trust_level, "reserved_cost": reserved_cost},
                    decision_outcome=strategy_eval.to_dict(),
                )
                await self._phase2_record_risk_event(
                    mint_id,
                    owner,
                    "strategy_block",
                    "warn",
                    "; ".join(strategy_eval.blocking_reasons) or "strategy_v2",
                    context=strategy_eval.to_dict(),
                )
                return "strategy_v2_blocked"

            if self.runtime_mode == "read_only":
                self.holdings.pop(mint_id, None)
                logging.info(f"{cc.YELLOW}Read-only mode blocked buy for {mint_id}.{cc.RESET}")
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "buy_blocked",
                    "read_only",
                    decision_inputs={"reserved_cost": reserved_cost},
                )
                return "read_only"

            available_balance = self._available_wallet_balance()
            if self.runtime_mode == "live" and available_balance <= reserved_cost:
                self.holdings.pop(mint_id, None)
                logging.info(
                    f"{cc.RED}Insufficient balance for {mint_id}, confirmed_wallet: "
                    f"{self.wallet_balance}, reserved: {self._reserved_wallet_balance()}, "
                    f"available: {available_balance}, required: {reserved_cost}.{cc.RESET}"
                )
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "buy_blocked",
                    "insufficient_balance",
                    decision_inputs={
                        "wallet_balance": self.wallet_balance,
                        "reserved_balance": self._reserved_wallet_balance(),
                        "available_balance": available_balance,
                        "required_lamports": reserved_cost,
                    },
                )
                return
            
            price = await self.get_latest_price(mint_id)
            if price <= 0:
                logging.info(
                    f"{cc.YELLOW}Local price snapshot is unavailable for {mint_id}; "
                    f"continuing with the on-chain bonding-curve quote.{cc.RESET}"
                )
                price = Decimal('0')

            slippage = float(SLIPPAGE_AMOUNT) # type: ignore
            order_id = None
            if self.phase2 is not None and session_id is not None:
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "buy",
                    "entry_signal",
                    decision_inputs={
                        "trust_level": trust_level,
                        "requested_lamports": lamports,
                        "reserved_cost": reserved_cost,
                        "local_price": str(price),
                        "slippage": slippage,
                        "strategy_profile": self.strategy_profile.name,
                    },
                    decision_outcome=strategy_eval.to_dict(),
                )
                await self._phase2_record_position_intent(
                    mint_id=mint_id,
                    owner=owner,
                    status="entry_signal",
                    intended_size_lamports=reserved_cost,
                    context={
                        "strategy_profile": self.strategy_profile.name,
                        "strategy_version": self.strategy_profile.version,
                        "evaluation": strategy_eval.to_dict(),
                        "market": "pump_fun",
                    },
                )
                order_id = await self.phase2.record_order(
                    session_id=session_id,
                    mint_id=mint_id,
                    side="buy",
                    venue="pump_fun",
                    status="attempting",
                    requested_lamports=reserved_cost,
                    context={
                        "trust_level": trust_level,
                        "local_price": str(price),
                        "slippage": slippage,
                    },
                )

            if self.shadow_mode:
                if self.simulation_mode and self.pump_swap is not None:
                    simulation_result = await self.pump_swap.pump_buy(
                        mint_id,
                        bonding_curve,
                        lamports,
                        owner,
                        sim=True,
                        priority_micro_lamports=fee,
                        slippage=slippage,
                    )
                    value = getattr(simulation_result, "value", None)
                    if value is not None and getattr(value, "err", None):
                        self.holdings.pop(mint_id, None)
                        if self.phase2 is not None and order_id is not None:
                            await self.phase2.update_order(
                                order_id,
                                status="failed",
                                context={"simulation_error": str(value.err)},
                            )
                        logging.warning(
                            f"{cc.RED}Simulation buy failed for {mint_id}: {value.err}.{cc.RESET}"
                        )
                        return "simulation_failed"

                estimated_tokens, shadow_price = await self._estimate_shadow_buy_fill(
                    mint_id,
                    bonding_curve=bonding_curve,
                    owner=owner,
                    lamports=lamports,
                    fallback_price=price if price > 0 else Decimal("0.000000001"),
                )

                self._update_holding(
                    mint_id,
                    owner=owner,
                    trust_level=trust_level,
                    shadow_position=True,
                    token_balance=estimated_tokens,
                    buy_price=shadow_price,
                    cost_basis_lamports=reserved_cost,
                    buy_fee_lamports=estimated_fee_lamports,
                    pending_buy_tx=None,
                    buy_order_id=order_id,
                )
                self._sync_managed_position(mint_id, status="open")
                self._record_spend(mint_id, reserved_cost)
                if self.wallet_balance > 0:
                    self.wallet_balance = max(0, int(self.wallet_balance - reserved_cost))
                if self.phase2 is not None and session_id is not None and order_id is not None:
                    await self.phase2.update_order(order_id, status="filled")
                    await self.phase2.record_fill(
                        order_id=order_id,
                        session_id=session_id,
                        mint_id=mint_id,
                        side="buy",
                        fill_price=str(shadow_price),
                        token_amount=estimated_tokens,
                        cost_lamports=reserved_cost,
                        fee_lamports=estimated_fee_lamports,
                        context={"mode": self.execution_label, "shadow_position": True},
                    )
                    await self._phase2_update_position_entry_fill(
                        mint_id=mint_id,
                        status="filled",
                        executed_size_lamports=reserved_cost,
                        executed_token_amount=estimated_tokens,
                        actual_buy_price=str(shadow_price),
                        context={"mode": self.execution_label, "shadow_position": True},
                    )
                    await self._phase2_record_holding_snapshot(mint_id, f"{self.runtime_mode}_filled")

                logging.info(
                    f"{cc.WHITE}{self.execution_label} buy armed for {mint_id} "
                    f"(local_price: {shadow_price:.10f}, trust_level: {trust_level}).{cc.RESET}"
                )
                await self.save_result(
                    {
                        "type": "buy",
                        "mode": self.execution_label,
                        "mint_id": mint_id,
                        "owner": owner,
                        "price": str(shadow_price),
                        "cost_lamports": reserved_cost,
                        "fee_lamports": estimated_fee_lamports,
                        "trust_level": trust_level,
                    }
                )
                self._log_exposure_summary(f"{self.execution_label}-buy")
                return "shadow_filled"

            self._announce_mev_trade(side="buy", mint_id=mint_id, venue="pump_fun")
            tx_id = await self.pump_swap.pump_buy(
                mint_id, 
                bonding_curve,
                lamports,
                owner,
                sim=False,
                priority_micro_lamports=fee,
                slippage=slippage
            )
            if tx_id == "migrated":
                logging.info(f"{cc.RED}Bonding curve migrated for {mint_id}. Ending session.{cc.RESET}")
                self.holdings.pop(mint_id, None)
                if self.phase2 is not None and order_id is not None:
                    await self.phase2.update_order(order_id, status="cancelled", context={"result": tx_id})
                    await self._phase2_close_position_journal(
                        mint_id=mint_id,
                        status="cancelled",
                        exit_reason="migrated_before_entry",
                        context={"result": tx_id},
                    )
                return "migrated"

            if tx_id == "zero_quote":
                logging.info(f"{cc.RED}Skipping buy for {mint_id}: quote resolved to zero output.{cc.RESET}")
                self.holdings.pop(mint_id, None)
                if self.phase2 is not None and order_id is not None:
                    await self.phase2.update_order(order_id, status="failed", context={"result": tx_id})
                    await self._phase2_close_position_journal(
                        mint_id=mint_id,
                        status="failed",
                        exit_reason="zero_quote",
                        context={"result": tx_id},
                    )
                await self._phase2_record_risk_event(mint_id, owner, "buy_failed", "warn", "zero_quote")
                return "zero_quote"

            if tx_id == "creator_vault_unavailable":
                logging.info(
                    f"{cc.YELLOW}Skipping buy for {mint_id}: bonding-curve creator data "
                    f"is not readable on RPC yet. Retrying session.{cc.RESET}"
                )
                self.holdings.pop(mint_id, None)
                if self.phase2 is not None and order_id is not None:
                    await self.phase2.update_order(order_id, status="failed", context={"result": tx_id})
                    await self._phase2_close_position_journal(
                        mint_id=mint_id,
                        status="failed",
                        exit_reason="creator_vault_unavailable",
                        context={"result": tx_id},
                    )
                await self._phase2_record_risk_event(mint_id, owner, "buy_failed", "warn", "creator_vault_unavailable")
                return "creator_vault_unavailable"
            
            if self.time_start != 0:
                logging.info(f"Full time taken to buy: {time.time() - self.time_start}s")

            if tx_id == "PriceTooHigh":
                logging.info(f"{cc.RED}Price too high for {mint_id}. Ending session.{cc.RESET}")
                self.holdings.pop(mint_id, None)
                if self.phase2 is not None and order_id is not None:
                    await self.phase2.update_order(order_id, status="failed", context={"result": tx_id})
                    await self._phase2_close_position_journal(
                        mint_id=mint_id,
                        status="failed",
                        exit_reason="PriceTooHigh",
                        context={"result": tx_id},
                    )
                await self._phase2_record_risk_event(mint_id, owner, "buy_failed", "warn", "PriceTooHigh")
                return "PriceTooHigh"

            self._reserve_buy_spend(mint_id, reserved_cost)
            self._update_holding(
                mint_id,
                owner=owner,
                trust_level=trust_level,
                shadow_position=False,
                buy_price=price if price > 0 else None,
                pending_buy_tx=tx_id,
                buy_order_id=order_id,
            )
            self._sync_managed_position(mint_id, status="pending")
            if self.phase2 is not None and order_id is not None:
                await self.phase2.update_order(order_id, status="submitted", tx_id=tx_id)
                await self._phase2_record_position_intent(
                    mint_id=mint_id,
                    owner=owner,
                    status="submitted",
                    intended_size_lamports=reserved_cost,
                    context={"tx_id": tx_id, "market": "pump_fun"},
                )
            logging.info(
                f"{cc.WHITE}Submitted buy for {mint_id} "
                f"(local_price: {price:.10f}, trust_level: {trust_level}, "
                f"slippage: {slippage}, tx: {tx_id}).{cc.RESET}"
            )
            await self.save_result(
                {
                    "type": "buy",
                    "mode": self.runtime_mode,
                    "mint_id": mint_id,
                    "owner": owner,
                    "price": str(price),
                    "trust_level": trust_level,
                    "tx_id": tx_id,
                }
            )
            self._log_exposure_summary("buy-submitted")
            return tx_id
        
        except Exception as e:
            self._release_buy_spend(mint_id)
            self.holdings.pop(mint_id, None)
            if self.phase2 is not None and order_id is not None:
                await self.phase2.update_order(
                    order_id,
                    status="failed",
                    context={"exception": str(e)},
                )
                await self._phase2_close_position_journal(
                    mint_id=mint_id,
                    status="failed",
                    exit_reason="buy_exception",
                    context={"exception": str(e)},
                )
            await self._phase2_record_risk_event(mint_id, owner, "buy_failed", "error", str(e))
            logging.error(f"Buy transaction failed: {e}")
            traceback.print_exc()
            await self._record_runtime_failure(mint_id, owner, f"buy exception: {e}")
            return "buy_failed"

    async def sell(self, mint_id, amount, reason, owner, trust_level, buy_price):
        try:
            logging.info(f"{cc.LIGHT_MAGENTA}Initiating sell for {mint_id}.")
            session_id = self._phase2_session_id(mint_id)
            holding = self.holdings.get(mint_id, {})
            market_source = holding.get("market") or self.swap_folder.get(mint_id, {}).get("market", "pump_fun")
            sell_venue = "pump_swap" if market_source == "pump_swap" else "pump_fun"

            mint_state = self.swap_folder.get(mint_id, {})
            bonding_curve = mint_state.get("bonding_curve")
            if not bonding_curve and market_source != "pump_swap" and not mint_state.get("migration_pending"):
                logging.info(f"{cc.RED}Bonding curve not found for {mint_id}.{cc.RESET}")
                self.holdings.pop(mint_id, None)
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "sell_blocked",
                    "missing_bonding_curve",
                )
                return

            if self.runtime_mode == "read_only":
                logging.info(f"{cc.YELLOW}Read-only mode blocked sell for {mint_id}.{cc.RESET}")
                self.holdings.pop(mint_id, None)
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "sell_blocked",
                    "read_only",
                )
                return "read_only"
            
            if market_source == "pump_swap" or self.swap_folder.get(mint_id, {}).get("migration_pending"):
                fee, estimated_sell_fee_lamports = await self._estimate_pump_swap_priority_fee(
                    mint_id,
                    pool=mint_state.get("pump_swap_pool"),
                    creator=mint_state.get("pump_swap_creator") or owner,
                )
            else:
                fee, estimated_sell_fee_lamports = await self._estimate_pump_fun_priority_fee(
                    mint_id,
                    bonding_curve,
                )
            sell_explanation = explain_sell(
                profile=self.strategy_profile,
                reason=reason,
                buy_price=buy_price,
                current_price=(mint_state.get("state") or {}).get("price"),
                token_state=mint_state.get("state") or {},
            )

            order_id = None
            if self.phase2 is not None and session_id is not None:
                await self._phase2_record_decision(
                    mint_id,
                    owner,
                    "sell",
                    reason,
                    decision_inputs={
                        "token_amount": amount,
                        "buy_price": str(buy_price),
                        "trust_level": trust_level,
                    },
                    decision_outcome=sell_explanation,
                )
                order_id = await self.phase2.record_order(
                    session_id=session_id,
                    mint_id=mint_id,
                    side="sell",
                    venue=sell_venue,
                    status="attempting",
                    requested_token_amount=int(amount or 0),
                    context={"reason": reason, "buy_price": str(buy_price), "market": market_source},
                )
            if holding.get("shadow_position"):
                # Shadow positions were never sent on-chain, so exits stay local even
                # when the current runtime mode signs/simulates transactions.
                if market_source == "pump_swap" or self.swap_folder.get(mint_id, {}).get("migration_pending"):
                    await self._refresh_pump_swap_market_for_mint(
                        mint_id,
                        force_lookup=not self.swap_folder.get(mint_id, {}).get("pump_swap_pool"),
                        force_refresh=True,
                    )
                else:
                    await self._refresh_pump_fun_price_for_mint(mint_id)

                price = await self.get_latest_price(mint_id)
                if price <= 0:
                    price = Decimal(str(holding.get("buy_price", "0")))

                price_change_pct = await self._process_peak_change(buy_price, price)
                cost_basis_lamports = int(holding.get("cost_basis_lamports", 0) or 0)
                if cost_basis_lamports <= 0:
                    cost_basis_lamports = int(self.session_spend.get(mint_id, 0) or 0)
                gross_exit_lamports = int(
                    (Decimal(amount) / Decimal("1000000")) * price * Decimal("1000000000")
                )
                estimated_proceeds_lamports = gross_exit_lamports - estimated_sell_fee_lamports
                profit_snapshot = self._build_realized_profit_snapshot(
                    cost_basis_lamports,
                    estimated_proceeds_lamports,
                )
                logging.info(
                    f"{cc.LIGHT_MAGENTA}{self.execution_label} sell completed for {mint_id} | "
                    f"estimated_pnl: {profit_snapshot['profit_sol']:+.9f} SOL "
                    f"({profit_snapshot['profit_usd']:+.6f} USD, {profit_snapshot['profit_pct']:+.2f}%) | "
                    f"price_change: {price_change_pct:+.2f}%{cc.RESET}"
                )
                self.daily_realized_profit_lamports += int(profit_snapshot["profit_lamports"])
                if self.wallet_balance > 0:
                    self.wallet_balance = max(0, int(self.wallet_balance + estimated_proceeds_lamports))
                if self.phase2 is not None and session_id is not None and order_id is not None:
                    await self.phase2.update_order(order_id, status="filled")
                    await self.phase2.record_fill(
                        order_id=order_id,
                        session_id=session_id,
                        mint_id=mint_id,
                        side="sell",
                        fill_price=str(price),
                        token_amount=int(amount or 0),
                        proceeds_lamports=estimated_proceeds_lamports,
                        fee_lamports=estimated_sell_fee_lamports,
                        wallet_delta_lamports=estimated_proceeds_lamports,
                        wallet_balance=self.wallet_balance,
                        context={"mode": self.execution_label, "shadow_position": True},
                    )
                    await self._phase2_close_position_journal(
                        mint_id=mint_id,
                        status="closed",
                        exit_reason=reason,
                        realized_profit_lamports=int(profit_snapshot["profit_lamports"]),
                        context={"mode": self.execution_label, "shadow_position": True},
                    )
                self._mark_managed_position_closed(
                    mint_id,
                    reason=reason,
                    profit_snapshot=profit_snapshot,
                    sell_price=price,
                    sell_fee_lamports=estimated_sell_fee_lamports,
                    sell_proceeds_lamports=estimated_proceeds_lamports,
                )
                self.holdings.pop(mint_id, None)
                await self.save_result(
                    {
                        "type": "sell",
                        "mode": self.execution_label,
                        "mint_id": mint_id,
                        "owner": owner,
                        "profit": profit_snapshot["profit_pct"],
                        "price_change_pct": price_change_pct,
                        "realized_profit_lamports": profit_snapshot["profit_lamports"],
                        "realized_profit_sol": str(profit_snapshot["profit_sol"]),
                        "realized_profit_usd": str(profit_snapshot["profit_usd"]),
                        "buy_cost_lamports": cost_basis_lamports,
                        "sell_proceeds_lamports": estimated_proceeds_lamports,
                        "buy_fee_lamports": int(holding.get("buy_fee_lamports", 0) or 0),
                        "sell_fee_lamports": estimated_sell_fee_lamports,
                        "buy_price": str(buy_price),
                        "sell_price": str(price),
                        "reason": reason,
                    }
                )
                await self._phase2_close_session(
                    mint_id,
                    status="closed",
                    reason=reason,
                    buy_cost_lamports=cost_basis_lamports,
                    sell_proceeds_lamports=estimated_proceeds_lamports,
                    realized_profit_lamports=profit_snapshot["profit_lamports"],
                )
                await self._validate_result(owner, reason)
                self._log_exposure_summary(f"{self.execution_label}-sell")
                return "shadow_sold"
            
            if mint_id in self.holdings:
                tx_id_sell = None
                if market_source == "pump_swap" or self.swap_folder.get(mint_id, {}).get("migration_pending"):
                    self._announce_mev_trade(side="sell", mint_id=mint_id, venue="pump_swap")
                    tx_id_sell = await self._submit_pump_swap_sell(
                        mint_id,
                        amount,
                        owner,
                        fee,
                        sim=False,
                    )
                    sell_venue = "pump_swap"
                else:
                    self._announce_mev_trade(side="sell", mint_id=mint_id, venue="pump_fun")
                    tx_id_sell = await self.pump_swap.pump_sell(
                        mint_id,
                        bonding_curve,
                        amount,
                        0,
                        owner,
                        False,
                        fee
                    )
                    if tx_id_sell == "migrated":
                        await self._mark_pump_swap_pending(
                            mint_id,
                            reason="pump_fun_sell_migrated",
                        )
                        await self._refresh_pump_swap_market_for_mint(
                            mint_id,
                            force_lookup=True,
                            force_refresh=True,
                        )
                        self._announce_mev_trade(side="sell", mint_id=mint_id, venue="pump_swap")
                        tx_id_sell = await self._submit_pump_swap_sell(
                            mint_id,
                            amount,
                            owner,
                            fee,
                            sim=False,
                        )
                        sell_venue = "pump_swap"
                        if tx_id_sell in {"pump_swap_unavailable", "pump_swap_pool_unavailable"}:
                            logging.info(
                                f"{cc.YELLOW}Bonding curve migrated for {mint_id} to PumpSwapAMM. "
                                f"Switching session price tracking to PumpSwap.{cc.RESET}"
                            )
                            if self.phase2 is not None and order_id is not None:
                                await self.phase2.update_order(order_id, status="cancelled", context={"result": tx_id_sell})
                            await self._phase2_record_risk_event(mint_id, owner, "migration_pending_exit", "warn", tx_id_sell)
                            return "migrated_monitoring"
                        logging.info(
                            f"{cc.YELLOW}Bonding curve migrated for {mint_id} to PumpSwapAMM. "
                            f"Submitting exit through PumpSwap.{cc.RESET}"
                        )

                if tx_id_sell == "missing_ata":
                    logging.info(f"{cc.RED}Skipping sell for {mint_id}: associated token account is missing.{cc.RESET}")
                    if self.phase2 is not None and order_id is not None:
                        await self.phase2.update_order(order_id, status="failed", context={"result": tx_id_sell})
                    await self._phase2_record_risk_event(mint_id, owner, "sell_failed", "warn", "missing_ata")
                    return "missing_ata"

                if tx_id_sell == "creator_vault_unavailable":
                    logging.info(
                        f"{cc.YELLOW}Skipping sell for {mint_id}: bonding-curve creator data "
                        f"is not readable on RPC yet.{cc.RESET}"
                    )
                    if self.phase2 is not None and order_id is not None:
                        await self.phase2.update_order(order_id, status="failed", context={"result": tx_id_sell})
                    await self._phase2_record_risk_event(mint_id, owner, "sell_failed", "warn", "creator_vault_unavailable")
                    return "creator_vault_unavailable"

                if tx_id_sell == "pump_swap_pool_unavailable":
                    logging.info(f"{cc.YELLOW}Skipping sell for {mint_id}: PumpSwap pool is not discoverable yet.{cc.RESET}")
                    if self.phase2 is not None and order_id is not None:
                        await self.phase2.update_order(order_id, status="cancelled", context={"result": tx_id_sell})
                    await self._phase2_record_risk_event(mint_id, owner, "migration_pending_exit", "warn", "pump_swap_pool_unavailable")
                    return "migrated_monitoring"

                if tx_id_sell in {"pump_swap_unavailable", "unexpected_pool", "zero_quote"}:
                    logging.info(f"{cc.RED}Skipping sell for {mint_id}: {tx_id_sell}.{cc.RESET}")
                    if self.phase2 is not None and order_id is not None:
                        await self.phase2.update_order(order_id, status="failed", context={"result": tx_id_sell})
                    await self._phase2_record_risk_event(mint_id, owner, "sell_failed", "warn", str(tx_id_sell))
                    return tx_id_sell

                if self.phase2 is not None and order_id is not None:
                    await self.phase2.update_order(order_id, status="submitted", tx_id=tx_id_sell)

                results = await self.swaps.get_swap_tx(
                    tx_id_sell,
                    mint_id,
                    max_retries=int(self.config.execution.confirmation_retry_limit),
                    tx_type="sell",
                )

                if isinstance(results, str) and results.startswith("InstructionError"):
                    normalized_result, detail = self._normalize_sell_instruction_error(sell_venue, results)
                    instruction_error_code = _parse_instruction_error_code(results)
                    logging.warning(f"{cc.YELLOW}Sell failed for {mint_id}: {detail}{cc.RESET}")
                    if self.phase2 is not None and order_id is not None:
                        await self.phase2.update_order(
                            order_id,
                            status="failed",
                            context={
                                "result": normalized_result,
                                "instruction_error": results,
                                "instruction_error_code": instruction_error_code,
                            },
                        )
                    self._update_holding(mint_id, sell_retry_count=0)
                    await self._phase2_record_risk_event(
                        mint_id,
                        owner,
                        "sell_failed",
                        "error",
                        normalized_result,
                        context={
                            "instruction_error": results,
                            "instruction_error_code": instruction_error_code,
                            "venue": sell_venue,
                        },
                    )
                    return normalized_result

                confirmed_wallet_balance = results.get("wallet_balance")
                if confirmed_wallet_balance is None:
                    confirmed_wallet_balance = results.get("balance")
                wallet_delta_lamports = results.get("wallet_delta_lamports")
                price = Decimal(str(results.get("price", 0) or "0"))
                sell_fee_lamports = int(results.get("fee_lamports", 0) or 0)

                if price <= 0 and wallet_delta_lamports and amount:
                    try:
                        price = (
                            Decimal(int(wallet_delta_lamports)) / Decimal("1000000000")
                        ) / (
                            Decimal(int(amount)) / Decimal("1000000")
                        )
                    except Exception:
                        price = await self.get_latest_price(mint_id)

                if confirmed_wallet_balance is None or wallet_delta_lamports is None:
                    logging.info(f"{cc.BLINK}{cc.RED}Sell failed for {mint_id}, increase your priority fee or check if you have sufficient balance.{cc.RESET}")
                    if self.phase2 is not None and order_id is not None:
                        await self.phase2.update_order(order_id, status="failed", context={"result": "sell_unconfirmed"})
                    await self._phase2_record_risk_event(mint_id, owner, "sell_failed", "error", "sell_unconfirmed")
                    return "sell_unconfirmed"

                price_change_pct = 0.0
                if price > 0:
                    price_change_pct = await self._process_peak_change(buy_price, price)
                cost_basis_lamports = int(holding.get("cost_basis_lamports", 0) or 0)
                if cost_basis_lamports <= 0:
                    cost_basis_lamports = int(self.session_spend.get(mint_id, 0) or 0)
                profit_snapshot = self._build_realized_profit_snapshot(
                    cost_basis_lamports,
                    int(wallet_delta_lamports),
                )
                logging.info(
                    f"{cc.LIGHT_MAGENTA}Sold {amount} of {mint_id} | "
                    f"realized_pnl: {profit_snapshot['profit_sol']:+.9f} SOL "
                    f"({profit_snapshot['profit_usd']:+.6f} USD, {profit_snapshot['profit_pct']:+.2f}%) | "
                    f"price_change: {price_change_pct:+.2f}%{cc.RESET}"
                )
                self.daily_realized_profit_lamports += int(profit_snapshot["profit_lamports"])

                old_balance = self.wallet_balance
                self.wallet_balance = int(confirmed_wallet_balance)
                logging.info(f"Wallet difference: {old_balance} -> {self.wallet_balance} = {self.wallet_balance - old_balance}")
                if self.phase2 is not None and session_id is not None and order_id is not None:
                    await self.phase2.update_order(order_id, status="filled", tx_id=tx_id_sell)
                    await self.phase2.record_fill(
                        order_id=order_id,
                        session_id=session_id,
                        mint_id=mint_id,
                        side="sell",
                        tx_id=tx_id_sell,
                        fill_price=str(price) if price > 0 else None,
                        token_amount=int(amount or 0),
                        proceeds_lamports=int(wallet_delta_lamports),
                        fee_lamports=sell_fee_lamports,
                        wallet_delta_lamports=int(wallet_delta_lamports),
                        wallet_balance=int(confirmed_wallet_balance),
                        context={"reason": reason},
                    )
                    await self._phase2_close_position_journal(
                        mint_id=mint_id,
                        status="closed",
                        exit_reason=reason,
                        realized_profit_lamports=int(profit_snapshot["profit_lamports"]),
                        context={"tx_id": tx_id_sell, "venue": sell_venue},
                    )
                self._mark_managed_position_closed(
                    mint_id,
                    reason=reason,
                    profit_snapshot=profit_snapshot,
                    sell_price=price if price > 0 else None,
                    sell_fee_lamports=sell_fee_lamports,
                    sell_proceeds_lamports=int(wallet_delta_lamports),
                    tx_id=tx_id_sell,
                )
                self.holdings.pop(mint_id, None)
                await self.save_result(
                    {
                        "type": "sell",
                        "mode": self.runtime_mode,
                        "mint_id": mint_id,
                        "owner": owner,
                        "profit": profit_snapshot["profit_pct"],
                        "price_change_pct": price_change_pct,
                        "realized_profit_lamports": profit_snapshot["profit_lamports"],
                        "realized_profit_sol": str(profit_snapshot["profit_sol"]),
                        "realized_profit_usd": str(profit_snapshot["profit_usd"]),
                        "buy_cost_lamports": cost_basis_lamports,
                        "sell_proceeds_lamports": int(wallet_delta_lamports),
                        "buy_fee_lamports": int(holding.get("buy_fee_lamports", 0) or 0),
                        "sell_fee_lamports": sell_fee_lamports,
                        "buy_price": str(buy_price),
                        "sell_price": str(price) if price > 0 else None,
                        "reason": reason,
                        "tx_id": tx_id_sell,
                        "venue": sell_venue,
                    }
                )
                await self._phase2_close_session(
                    mint_id,
                    status="closed",
                    reason=reason,
                    buy_cost_lamports=cost_basis_lamports,
                    sell_proceeds_lamports=int(wallet_delta_lamports),
                    realized_profit_lamports=profit_snapshot["profit_lamports"],
                )
                await self._validate_result(owner, reason)
                self._log_exposure_summary("sell-complete")
                return "sold"

        except Exception as e:
            logging.error(f"Sell transaction failed: {e}")
            traceback.print_exc()
            await self._record_runtime_failure(mint_id, owner, f"sell exception: {e}")

    async def set_trust_level(self, creator):
        """
            Trust level is determined by the creator's mint count and median peak market cap.
            If creator has only 1 mint, trust level is 1.
            If creator has median peak market cap >= 50k, trust level is 2.
            If creator has median peak market cap >= 0, trust level is 1.
        """
        trust_level = 0
        if creator["mint_count"] == 1:
            trust_level = 1
        elif creator["median_peak_market_cap"] >= 50000:
            trust_level = 2
        elif creator["median_peak_market_cap"] >= 0:
            trust_level = 1
        return trust_level

    async def monitor_mint_session(self, mint_id, owner):
        skip_reason = self._session_skip_reason(mint_id, owner)
        if skip_reason:
            logging.info(f"{cc.YELLOW}Owner {owner} is blacklisted, a single lock is enabled, or leaderboard is updating rn. Skipping session.{cc.RESET}")
            await self._phase2_record_decision(mint_id, owner, "session_skipped", skip_reason)
            await self._phase2_record_risk_event(mint_id, owner, "session_skipped", "warn", skip_reason)
            await self._phase2_close_session(mint_id, status="skipped", reason=skip_reason)
            self.swap_folder.pop(mint_id, None)
            return

        logging.info(f"{cc.BLUE}Session started for {mint_id} (owner: {owner}). Monitoring...{cc.RESET}")

        # Determine trust_level
        trust_level = 0
        creator = self.leaderboard.get(owner)
        trust_level = await self.set_trust_level(creator)
        profit_range = Decimal(str(round(creator["median_success_ratio"], 2)))

        # increments in multiples of 10
        max_target = profit_range * PROFIT_MARGIN
        increments = [PRICE_STEP_UNITS]
        step_unit = PRICE_STEP_UNITS
        val = increments[0] + step_unit
        while val <= max_target:
            increments.append(val)
            val += step_unit

        current_target_step = 0
        to_sell = increments[current_target_step] if increments else Decimal('10')

        last_price = None
        last_price_change_time = datetime.datetime.now(datetime.timezone.utc)
        ref_price_drop = None
        last_buys_count = 0
        last_buys_timestamp = datetime.datetime.now(datetime.timezone.utc)
        last_increment_time = None
        token_balance = 0
        selfBuyPrice = Decimal('0')
        buy_retry = 0
        rolling_buffer = collections.deque()
        skip_if_done = False
        buy_tx_id = ""
        increment_threshold = Decimal('25')
        session_started_at = time.monotonic()
        last_entry_wait_reason = None

        while True:
            try:
                if self.stop_event.is_set():
                    break
                control_state = self._refresh_operator_state()
                if mint_id in self.swap_folder:
                    row = self.swap_folder[mint_id].get("state")
                    if not row:
                        wait_elapsed = time.monotonic() - session_started_at
                        if wait_elapsed >= ENTRY_SIGNAL_MAX_WAIT_SECONDS:
                            logging.info(
                                f"{cc.YELLOW}Ending session for {mint_id}: no market state arrived within "
                                f"{wait_elapsed:.1f}s.{cc.RESET}"
                            )
                            await self._phase2_record_decision(
                                mint_id,
                                owner,
                                "session_closed",
                                "market_state_timeout",
                            )
                            await self._phase2_close_session(
                                mint_id,
                                status="closed",
                                reason="market_state_timeout",
                            )
                            break
                        await asyncio.sleep(0.1)
                        continue

                    if self.swap_folder[mint_id].get("market") == "pump_swap" or self.swap_folder[mint_id].get("migration_pending"):
                        await self._refresh_pump_swap_market_for_mint(mint_id)
                        row = self.swap_folder[mint_id].get("state") or row

                    name = self.swap_folder[mint_id]["name"]
                    price = row.get('price', Decimal('0'))
                    db_history = row.get('price_history') or {}
                    tx_counts = row.get('tx_counts') or {}
                    holders = row.get('holders') or {}
                    swaps = tx_counts.get("swaps", 0)
                    buys = tx_counts.get("buys", 0)
                    sells = tx_counts.get("sells", 0)
                    open_price = row.get('open_price', Decimal('0'))
                    peak_price = row.get('high_price', Decimal('0'))

                    if mint_id in set(control_state.get("force_sell_mints", []) or []) and mint_id in self.holdings:
                        logging.info(f"{cc.YELLOW}Operator force-sell requested for {mint_id}.{cc.RESET}")
                        sell_result = await self.sell(mint_id, int(self.holdings[mint_id].get("token_balance", 0) or 0), "operator_force_sell", owner, trust_level if 'trust_level' in locals() else 1, Decimal(str(self.holdings[mint_id].get("buy_price", "0") or "0")))
                        if mint_id in set(self.operator_state.get("force_sell_mints", []) or []):
                            self.operator_state["force_sell_mints"] = [
                                item for item in self.operator_state.get("force_sell_mints", []) if item != mint_id
                            ]
                            self.operator_state = save_control_state(self.config.paths.operator_control_file, self.operator_state)
                        if sell_result != "migrated_monitoring":
                            break
                        await asyncio.sleep(0.25)
                        continue

                    # Buy if not already
                    if mint_id not in self.holdings:
                        should_wait_for_signal, entry_wait_reason, entry_signal_context = self._entry_signal_wait_reason(row)
                        if should_wait_for_signal:
                            wait_elapsed = time.monotonic() - session_started_at
                            if wait_elapsed >= ENTRY_SIGNAL_MAX_WAIT_SECONDS:
                                logging.info(
                                    f"{cc.YELLOW}Ending session for {mint_id} without an entry after "
                                    f"{wait_elapsed:.1f}s ({entry_wait_reason}).{cc.RESET}"
                                )
                                await self._phase2_record_decision(
                                    mint_id,
                                    owner,
                                    "session_closed",
                                    "entry_signal_timeout",
                                    decision_outcome=entry_signal_context,
                                )
                                await self._phase2_close_session(
                                    mint_id,
                                    status="closed",
                                    reason="entry_signal_timeout",
                                )
                                break
                            if entry_wait_reason != last_entry_wait_reason:
                                logging.info(
                                    f"{cc.LIGHT_GRAY}Waiting for a usable entry signal on {mint_id}: "
                                    f"{entry_wait_reason}.{cc.RESET}"
                                )
                                last_entry_wait_reason = entry_wait_reason
                            await asyncio.sleep(0.25)
                            continue

                        last_entry_wait_reason = None
                        result = await self.buy(mint_id, trust_level, owner)
                        if result == "PriceTooHigh":
                            logging.info(f"{cc.RED}Ending session for {mint_id} due to high buy price.{cc.RESET}")
                            await self._phase2_close_session(mint_id, status="closed", reason="PriceTooHigh")
                            break
                        if result in {None, "buy_failed"}:
                            logging.info(f"{cc.RED}Ending session for {mint_id} because the buy attempt failed.{cc.RESET}")
                            await self._phase2_close_session(mint_id, status="failed", reason="buy_failed")
                            break
                        if result in {"read_only", "emergency_stop", "spend_cap_exceeded", "risk_control", "strategy_v2_blocked"}:
                            await self._phase2_close_session(mint_id, status="blocked", reason=result)
                            break
                        if result == "zero_quote":
                            await asyncio.sleep(0.25)
                            continue
                        if result == "creator_vault_unavailable":
                            await asyncio.sleep(0.25)
                            continue
                        buy_tx_id = result
                        continue  # Jump straight to next iteration so we fetch our tx

                    if not skip_if_done:
                        confirmed_wallet_balance = None
                        confirmed_cost_lamports = 0
                        confirmed_buy_fee_lamports = 0
                        current_holding = self.holdings.get(mint_id, {})
                        if current_holding.get("shadow_position"):
                            selfBuyPrice = Decimal(str(current_holding.get("buy_price", "0")))
                            token_balance = int(current_holding.get("token_balance", 0))
                            self._update_holding(
                                mint_id,
                                owner=owner,
                                trust_level=trust_level,
                                token_balance=token_balance,
                                buy_price=selfBuyPrice,
                            )
                            skip_if_done = True
                        else:
                            we = holders.get(self.wallet, {})
                            balance = we.get("balance", 0)
                            balance_changes = we.get("balance_changes", [])
                            if balance_changes:
                                for bc in balance_changes:
                                    if bc.get("type", "") == "buy":
                                        selfBuyPrice = Decimal(bc.get("price_was", 0))
                                        token_balance = balance
                                        break
                            else:
                                selfBuyPrice = Decimal('0')

                            if buy_retry >= int(self.config.execution.confirmation_retry_limit):
                                logging.info(f"{cc.RED}Buy retry exceeded for {mint_id}.\nTrying a fallback...{cc.RESET}")
                                results = await self.swaps.get_swap_tx(buy_tx_id, mint_id, tx_type="buy")
                                if not results or isinstance(results, str):
                                    self._release_buy_spend(mint_id)
                                    self.holdings.pop(mint_id, None)
                                    logging.info(f"{cc.BLINK}Buy was not successful :( {mint_id}{cc.RESET}")
                                    break
                                token_balance = int(results.get("balance", 0))
                                selfBuyPrice = Decimal(results.get("price", 0))
                                confirmed_wallet_balance = results.get("sol_balance")
                                confirmed_cost_lamports = int(results.get("cost_lamports", 0) or 0)
                                confirmed_buy_fee_lamports = int(results.get("fee_lamports", 0) or 0)
                                if token_balance <= 0 or selfBuyPrice <= 0:
                                    self._release_buy_spend(mint_id)
                                    self.holdings.pop(mint_id, None)
                                    break
                            if token_balance <= 0 or selfBuyPrice <= 0:
                                buy_retry += 1
                                logging.info(f"{cc.YELLOW}No balance or couldn't get buy price for {mint_id}, retry {buy_retry}.{cc.RESET}")
                                await asyncio.sleep(0.5)
                                continue

                            reserved_cost = int(self.pending_buy_spend.get(mint_id, 0) or 0)
                            cost_basis_lamports = (
                                confirmed_cost_lamports
                                or int(current_holding.get("cost_basis_lamports", 0) or 0)
                                or reserved_cost
                            )
                            if mint_id in self.pending_buy_spend:
                                self._finalize_buy_spend(mint_id, confirmed_wallet_balance)
                            self._update_holding(
                                mint_id,
                                owner=owner,
                                trust_level=trust_level,
                                token_balance=token_balance,
                                buy_price=selfBuyPrice,
                                cost_basis_lamports=cost_basis_lamports,
                                buy_fee_lamports=confirmed_buy_fee_lamports or None,
                                buy_wallet_balance=confirmed_wallet_balance,
                                pending_buy_tx=None,
                            )
                            self._sync_managed_position(mint_id, status="open")
                            session_id = self._phase2_session_id(mint_id)
                            buy_order_id = current_holding.get("buy_order_id")
                            if self.phase2 is not None and session_id is not None and buy_order_id is not None:
                                await self.phase2.update_order(
                                    int(buy_order_id),
                                    status="filled",
                                    tx_id=buy_tx_id or current_holding.get("pending_buy_tx"),
                                )
                                await self.phase2.record_fill(
                                    order_id=int(buy_order_id),
                                    session_id=session_id,
                                    mint_id=mint_id,
                                    side="buy",
                                    tx_id=buy_tx_id or current_holding.get("pending_buy_tx"),
                                    fill_price=str(selfBuyPrice),
                                    token_amount=token_balance,
                                    cost_lamports=cost_basis_lamports,
                                    fee_lamports=confirmed_buy_fee_lamports,
                                    wallet_balance=confirmed_wallet_balance,
                                    context={"mode": self.runtime_mode},
                                )
                                await self._phase2_update_position_entry_fill(
                                    mint_id=mint_id,
                                    status="filled",
                                    executed_size_lamports=cost_basis_lamports,
                                    executed_token_amount=token_balance,
                                    actual_buy_price=str(selfBuyPrice),
                                    context={"mode": self.runtime_mode},
                                )
                                await self._phase2_record_holding_snapshot(mint_id, "buy_confirmed")
                            await self.save_result(
                                {
                                    "type": "buy_confirmed",
                                    "mode": self.runtime_mode,
                                    "mint_id": mint_id,
                                    "owner": owner,
                                    "price": str(selfBuyPrice),
                                    "token_balance": token_balance,
                                    "cost_lamports": cost_basis_lamports,
                                    "fee_lamports": confirmed_buy_fee_lamports,
                                    "tx_id": buy_tx_id or current_holding.get("pending_buy_tx"),
                                }
                            )
                            self._log_exposure_summary("buy-confirmed")

                    # personal peak change from buy price
                    selfPeakChange = await self._process_peak_change(selfBuyPrice, peak_price)
                    # global peak
                    peak_pct_change = await self._process_peak_change(open_price, peak_price)

                    if self._emergency_stop_requested():
                        logging.warning(f"{cc.RED}Emergency stop detected. Exiting position for {mint_id}.{cc.RESET}")
                        sell_result = await self.sell(mint_id, token_balance, "emergency-stop", owner, trust_level, selfBuyPrice)
                        if sell_result != "migrated_monitoring":
                            break
                        await asyncio.sleep(0.25)
                        continue

                    current_time = datetime.datetime.now(datetime.timezone.utc)
                    time_since_last_change = (current_time - last_price_change_time).total_seconds()

                    if ref_price_drop is None or peak_price > ref_price_drop:
                        ref_price_drop = peak_price

                    malicious = False
                    if ref_price_drop is not None and price < (ref_price_drop * Decimal('0.5')):
                        malicious = True

                    # Drop-Time
                    if buys > last_buys_count:
                        last_buys_count = buys
                        last_buys_timestamp = datetime.datetime.now(datetime.timezone.utc)
                        
                    time_since_last_buy = (current_time - last_buys_timestamp).total_seconds()
                    is_drop_time = (time_since_last_buy >= DROP_TIME)

                    # Condition priority
                    if sells > buys:
                        condition_level = "sells>buys"
                    if malicious:
                        condition_level = "malicious"
                    elif is_drop_time:
                        condition_level = "drop-time"
                    else:
                        condition_level = "safe"

                    # Adjusted increments so they don't blow up if open->buy was huge
                    open_to_buy_diff = Decimal('0')
                    if open_price > 0 and selfBuyPrice > open_price:
                        open_to_buy_diff = Decimal(
                            ((selfBuyPrice - open_price) / open_price) * 100
                        )

                    if not skip_if_done:
                        if len(increments) > 1 and open_to_buy_diff > 0:
                            personal_range = profit_range - open_to_buy_diff
                            if personal_range < 0:
                                personal_range = Decimal('0')
                            personal_factor = personal_range / open_to_buy_diff if open_to_buy_diff > 0 else Decimal('0')
                            new_increments = [inc for inc in increments if inc <= personal_factor * 100]
                            if new_increments and new_increments != increments:
                                increments = new_increments
                                if current_target_step >= len(increments):
                                    current_target_step = len(increments) - 1
                                if current_target_step < 0:
                                    current_target_step = 0
                            elif not new_increments:
                                increments = [PRICE_STEP_UNITS]
                                current_target_step = 0
                        skip_if_done = True

                    # update rolling window and compute composite
                    self._update_rolling_window(rolling_buffer, current_time, price, swaps)
                    composite_score = self._compute_composite_score(rolling_buffer)

                    # Decide increments
                    next_step_possible = (current_target_step < len(increments) - 1)
                    can_increment = False

                    if condition_level in ["malicious", "drop-time"]:
                        current_target_step = 0
                    elif condition_level == "sells>buys":
                        if composite_score < DECREMENT_THRESHOLD:
                            current_target_step = 0
                    else:
                        if next_step_possible:
                            current_incr = increments[current_target_step]
                            next_incr = increments[current_target_step + 1]
                            difference = next_incr - current_incr
                            half_of_jump = difference * Decimal('0.5')
                            partial_threshold = (current_incr + half_of_jump) if current_incr != increments[0] else half_of_jump

                            if composite_score > increment_threshold:
                                if Decimal(str(selfPeakChange)) >= partial_threshold:
                                    if last_increment_time is None:
                                        can_increment = True
                                    else:
                                        time_since_increment = (current_time - last_increment_time).total_seconds()
                                        if time_since_increment > INCREMENT_COOLDOWN: # increment_cooldown
                                            can_increment = True

                        if can_increment:
                            current_target_step += 1
                            last_increment_time = current_time

                    if current_target_step >= len(increments):
                        current_target_step = len(increments) - 1
                    if current_target_step < 0:
                        current_target_step = 0

                    to_sell = increments[current_target_step]

                    # Sell if selfPeakChange >= to_sell or malicious or drop_time
                    if (Decimal(str(selfPeakChange)) >= to_sell) or malicious or is_drop_time:
                        logging.info(f"""{cc.LIGHT_GREEN}Selling {mint_id} at {str(selfPeakChange)}, is malicious: {malicious}, is drop-time: {is_drop_time}.{cc.RESET}""")
                        sell_result = await self.sell(mint_id, token_balance, condition_level, owner, trust_level, selfBuyPrice)
                        if sell_result == "migrated_monitoring":
                            await asyncio.sleep(0.25)
                            continue
                        logging.info(f"{cc.LIGHT_GRAY}Token has been sold, stopping the session.{cc.RESET}")
                        break

                    changed_price = (last_price is None or price != last_price)
                    if changed_price:
                        last_price = price
                        last_price_change_time = current_time
                        logging.info(
                            f"""
                            {cc.MAGENTA}[Updated {mint_id}]
                            {cc.LIGHT_WHITE}Name: {name}
                            {cc.DARK_GRAY}Hist len: {len(db_history)}
                            {cc.LIGHT_CYAN}Price: {price}
                            {cc.LIGHT_MAGENTA}MC: {row.get('mc')}
                            {cc.LIGHT_RED}Peak: {peak_price:.10f} | Open:{open_price:.10f} | Change:{peak_pct_change:.2f}%
                            Cond: {condition_level}, to_sell:{to_sell}%
                            composite_score:{composite_score:.2f}
                            last_tx_diff:{time_since_last_change:.2f}s
                            ourPeakChange: {selfPeakChange}%
                            OpenToBuyDiff: {open_to_buy_diff}
                            Increments: {increments}
                            Buys:{buys} Sells:{sells} Swaps:{swaps}{cc.RESET}
                            """
                        )

                    # Stagnant checks
                    """
                        If price hasn't changed in 30 minutes, sell.
                        If price is < 3e-8 and time since last change > 13s, sell.
                    """
                    if time_since_last_change > 1800:
                        logging.info(f"{cc.YELLOW}{mint_id} stagnant(no price change>30m). Stop.{cc.RESET}")
                        sell_result = await self.sell(mint_id, token_balance, "stagnant", owner, trust_level, selfBuyPrice)
                        if sell_result != "migrated_monitoring":
                            break
                        await asyncio.sleep(0.25)
                        continue

                    if price < Decimal('0.0000000300') and time_since_last_change > STAGNANT_UNDER_PRICE: # Stagnant and low price
                        logging.info(f"{cc.YELLOW}{mint_id} stagnant(price<3e-8 & tx>13s). Stop.{cc.RESET}")
                        sell_result = await self.sell(mint_id, token_balance, "malicious", owner, trust_level, selfBuyPrice)
                        if sell_result != "migrated_monitoring":
                            break
                        await asyncio.sleep(0.25)
                        continue

                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                logging.info(f"{cc.YELLOW}{mint_id} cancelled.{cc.RESET}")
                break
            except Exception as e:
                logging.info(f"{cc.RED}Error session {mint_id}: {e}{cc.RESET}")
                await self._record_runtime_failure(mint_id, owner, f"session exception: {e}", severity="warn")
                await asyncio.sleep(1)

        position_still_tracked = mint_id in self.holdings
        self.active_sessions.pop(mint_id, None)
        if position_still_tracked:
            logging.warning(
                "Session ended for %s while a position is still tracked. Preserving state for operator recovery or shutdown liquidation.",
                mint_id,
            )
            await self._phase2_record_decision(
                mint_id,
                owner,
                "session_preserved",
                "position_still_tracked",
            )
        else:
            await self._phase2_close_session(mint_id, status="closed", reason="session_complete")
            self.swap_folder.pop(mint_id, None)
            self._release_buy_spend(mint_id)
        logging.info(f"{cc.BLUE}Session ended {mint_id}.{cc.RESET}")

    async def update_leaderboard(self):
        while not self.stop_event.is_set():
            try:
                if len(self.holdings) > 0:
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=15)
                    except asyncio.TimeoutError:
                        pass
                    continue

                self.updating = True
                
                await self.load_blacklist()
                
                logging.info(f"{cc.WHITE}Starting leaderboard update...{cc.RESET}")
                await self.analyzer.analyze_market()
                self.leaderboard = await asyncio.to_thread(self.analyzer.process_results_sync, False)
                if self.phase2 is not None:
                    self.leaderboard_version = await self.phase2.record_leaderboard_generation(
                        self.leaderboard,
                        source="dexter.trader",
                        metadata={"runtime_mode": self.runtime_mode},
                    )
                    await self.phase2.prune_raw_event_retention()

                logging.info(f"{cc.WHITE}Leaderboard updated successfully.{cc.RESET}")
                logging.info(f"{cc.LIGHT_RED}Leaderboard Creators: {len(self.leaderboard)}{cc.RESET}")

                # Sort the leaderboard by performance score
                sorted_leaderboard = sorted(
                    self.leaderboard.items(),
                    key=lambda x: x[1]['performance_score'],
                    reverse=True
                )

                logging.info(f"{cc.LIGHT_CYAN}Top 10 Leaderboard Entries:{cc.RESET}")
                for rank, (creator, entry) in enumerate(sorted_leaderboard[:10], start=1):
                    logging.info(
                        f"{cc.MAGENTA}"
                        f"Rank {rank}: Creator: {creator}, "
                        f"Performance Score: {entry['performance_score']:.2f}, "
                        f"Trust Factor: {entry['trust_factor']:.2f}, "
                        f"Total Mints: {entry['mint_count']}, "
                        f"Total Swaps: {entry['total_swaps']}, "
                        f"Median Market Cap: {entry['median_peak_market_cap']:.2f} USD"
                        f"{cc.RESET}"
                    )

                # Save the sorted leaderboard to a file
                current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
                self.config.paths.leaderboard_file.parent.mkdir(parents=True, exist_ok=True)
                with open(self.config.paths.leaderboard_file, "a", encoding="utf-8") as file:
                    file.write(f"{current_time}-\n")
                    for creator, entry in sorted_leaderboard:
                        file.write(f"{json.dumps({creator: entry}, indent=2)}\n")

            except Exception as e:
                logging.error(f"Error updating leaderboard: {e}")
                traceback.print_exc()
            self.updating = False
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=LEADERBOARD_UPDATE_INTERVAL * 60) # type: ignore
            except asyncio.TimeoutError:
                pass

    async def run(self):
        try:
            dex_welcome()
            self.dexLogs = DexBetterLogs(self.ws_url, enable_market=False)
            await self.init_db_pool()
            await self._phase2_ensure_strategy_profile()

            if self._emergency_stop_requested():
                message = (
                    f"Emergency stop file is present at {self.config.runtime.emergency_stop_file}. "
                    "New buys are disabled until it is cleared."
                )
                if self.runtime_mode == "live":
                    raise RuntimeError(f"{message} Refusing to start live trading.")
                logging.warning(message)

            if self.runtime_mode != "read_only":
                self.session = ClientSession()
                self.async_client = AsyncClient(self.http_url)
                self.pump_swap_market = PumpSwapMarket(self.session, self.http_url)

            if self.execution_mode in {"paper", "simulate", "live"} and self.config.rpc.private_key:
                self.pump_swap = PumpFun(
                    self.session,
                    self.config.rpc.private_key,
                    async_client=self.async_client,
                    mev_config=self.active_mev_config,
                )
                self.pump_swap_executor = PumpSwapExecutor(
                    self.pump_swap_market,
                    self.config.rpc.private_key,
                    async_client=self.async_client,
                    mev_config=self.active_mev_config,
                )
            elif self.execution_mode in {"paper", "simulate", "live"}:
                logging.warning(
                    "No PRIVATE_KEY is configured, so Dexter cannot fetch on-chain shadow quotes for %s mode.",
                    self.execution_label,
                )

            if self.runtime_mode == "live":
                self.swaps = SolanaSwaps(
                    self,
                    private_key=self.privkey,
                    wallet_address=self.wallet,
                    rpc_endpoint=self.http_url,
                    api_key=self.http_url,
                )
                self.wallet_balance = await self.swaps.fetch_wallet_balance_sol()
                logging.info(f"{cc.CYAN}{cc.BRIGHT}Initialized wallet {self.wallet} with: {self.wallet_balance} lamports")
                if not self.live_send_enabled:
                    logging.warning(
                        "Trader runtime mode is live on %s, but transaction submission is in %s; no transactions will be sent.",
                        self.config.runtime.network,
                        self.execution_label,
                    )
                elif self.config.mev.enabled and self.config.runtime.network != "mainnet":
                    logging.warning("USE_MEV is enabled, but Dexter will ignore MEV routing on devnet.")
                elif self.active_mev_config is not None:
                    logging.info(
                        "Mainnet MEV sender enabled | provider=%s tip=%s SOL (%s lamports).",
                        self.active_mev_config.provider,
                        self._render_mev_tip_sol(self.active_mev_config),
                        self.active_mev_config.tip_lamports,
                    )
            else:
                logging.info(
                    f"{cc.YELLOW}Trader runtime mode is {self.execution_label}; live sends are disabled.{cc.RESET}"
                )

            if self.runtime_mode == "live" and not self.config.runtime.close_positions_on_shutdown:
                logging.warning(
                    "DEXTER_CLOSE_POSITIONS_ON_SHUTDOWN=false. Open live positions will require manual handling if the trader stops."
                )

            logging.info(
                "Risk caps | per_trade=%s SOL session=%s SOL daily=%s SOL close_positions_on_shutdown=%s emergency_stop_file=%s",
                self.config.risk.per_trade_sol_cap,
                self.config.risk.session_sol_cap,
                self.config.risk.daily_sol_cap,
                self.config.runtime.close_positions_on_shutdown,
                self.config.runtime.emergency_stop_file,
            )
            self._log_exposure_summary("startup")
            self._start_embedded_wslogs()
            await self._publish_runtime_snapshot()

            runtime_tasks = [
                self.subscribe(),
                self.subscribe(PUMP_SWAP),
                self.handle_market(),
                self._report_exposure_loop(),
                self._publish_runtime_snapshot_loop(),
            ]
            if self.embedded_wslogs_task is not None:
                runtime_tasks.append(self.embedded_wslogs_task)
            if self.embedded_wslogs_watchdog_task is not None:
                runtime_tasks.append(self.embedded_wslogs_watchdog_task)

            if self.session_seed:
                await self._prime_seed_session()
            else:
                runtime_tasks.insert(3, self.update_leaderboard())

            await asyncio.gather(*runtime_tasks)
        except asyncio.CancelledError:
            await self.close()
        except KeyboardInterrupt:
            await self.close()
        except Exception:
            await self.close()
            raise
        finally:
            for mint_id, task in self.active_sessions.items():
                task.cancel()

    async def close(self):
        if self.is_closing:
            return
        self.is_closing = True
        try:
            logging.info(f"{cc.CYAN}Closing Dexter...{cc.RESET}")
            self.stop_event.set()
            await self._shutdown_embedded_wslogs()

            if self.active_tasks:
                logging.info("Stopping all active log-handling tasks...")
                active_log_tasks = list(self.active_tasks)
                for task in active_log_tasks:
                    task.cancel()
                await asyncio.gather(*active_log_tasks, return_exceptions=True)
                self.active_tasks.clear()

            if self.active_sessions:
                active_tasks = list(self.active_sessions.values())
                for task in active_tasks:
                    task.cancel()
                await asyncio.gather(*active_tasks, return_exceptions=True)

            if self.config.runtime.close_positions_on_shutdown and self.holdings:
                logging.warning(
                    "DEXTER_CLOSE_POSITIONS_ON_SHUTDOWN is set. Attempting to liquidate %s tracked position(s).",
                    len(self.holdings),
                )
                await self._liquidate_open_positions("shutdown")
            elif self.holdings:
                logging.warning(
                    "Shutdown completed with %s tracked position(s) still open because DEXTER_CLOSE_POSITIONS_ON_SHUTDOWN=false.",
                    len(self.holdings),
                )

            self._log_exposure_summary("shutdown")
            await self._publish_runtime_snapshot()
            if self.pump_swap:
                await self.pump_swap.close()
                self.pump_swap = None
                self.async_client = None
                self.session = None
            await self.close_db_pool()
            if self.swaps:
                await self.swaps.close()
                await self.swaps.async_client.close()
                self.swaps = None
            if self.async_client:
                await self.async_client.close()
            if self.dexLogs and not self.dexLogs.session.closed:
                await self.dexLogs.session.close()
            if self.session and not self.session.closed:
                await self.session.close()
            await asyncio.sleep(0.1)
        except Exception as e:
            logging.error(f"Error closing Dexter: {e}")
            traceback.print_exc()


async def _fetch_wallet_balance_lamports_async(config) -> int | None:
    private_key = config.wallets.trading_private_key or config.rpc.private_key
    if not private_key:
        return None
    client = AsyncClient(config.rpc.http_url)
    try:
        keypair = Keypair.from_bytes(base58.b58decode(private_key))
        response = await client.get_balance(keypair.pubkey())
        return int(response.value) if getattr(response, "value", None) is not None else None
    except Exception:
        return None
    finally:
        await client.close()


def fetch_wallet_balance_lamports(config) -> int | None:
    try:
        return asyncio.run(_fetch_wallet_balance_lamports_async(config))
    except Exception:
        return None


def summarize_managed_positions(config) -> dict[str, int]:
    positions = list((load_managed_positions(config).get("positions") or {}).values())
    return {
        "open": sum(1 for item in positions if item.get("status") == "open"),
        "pending": sum(1 for item in positions if item.get("status") == "pending"),
        "closed": sum(1 for item in positions if item.get("status") == "closed"),
        "total": len(positions),
    }


async def _refresh_recovered_position_price(trader: Dexter, mint_id: str, position: dict[str, Any]) -> None:
    market = position.get("market", "pump_fun")
    if market == "pump_swap" or position.get("market") == "pump_swap":
        await trader._refresh_pump_swap_market_for_mint(
            mint_id,
            force_lookup=True,
            force_refresh=True,
        )
        return
    await trader._refresh_pump_fun_price_for_mint(mint_id)


async def _liquidate_managed_position_async(
    config,
    position: dict[str, Any],
    *,
    reason: str = "manual_manage_sell",
) -> dict[str, Any]:
    seed = {
        "mint_id": position["mint_id"],
        "owner": position["owner"],
        "name": position.get("name"),
        "bonding_curve": position.get("bonding_curve"),
        "trust_level": int(position.get("trust_level", 1) or 1),
        "buy_price": position.get("buy_price") or position.get("current_price") or position.get("last_price"),
        "token_balance": int(position.get("token_balance", 0) or 0),
        "cost_basis_lamports": int(position.get("cost_basis_lamports", 0) or 0),
        "market": position.get("market", "pump_fun"),
        "shadow_position": bool(position.get("shadow_position", False)),
        "skip_database": True,
    }
    trader = Dexter(config, session_seed=seed)
    trader.phase2 = None
    trader.skip_database_init = True

    try:
        if trader.runtime_mode != "read_only":
            trader.session = ClientSession()
            trader.async_client = AsyncClient(trader.http_url)
            trader.pump_swap_market = PumpSwapMarket(trader.session, trader.http_url)

        if trader.execution_mode in {"paper", "simulate", "live"} and config.rpc.private_key:
            trader.pump_swap = PumpFun(
                trader.session,
                config.rpc.private_key,
                async_client=trader.async_client,
            )
            trader.pump_swap_executor = PumpSwapExecutor(
                trader.pump_swap_market,
                config.rpc.private_key,
                async_client=trader.async_client,
            )

        if trader.runtime_mode == "live":
            trader.swaps = SolanaSwaps(
                trader,
                private_key=trader.privkey,
                wallet_address=trader.wallet,
                rpc_endpoint=trader.http_url,
                api_key=trader.http_url,
            )
            trader.wallet_balance = await trader.swaps.fetch_wallet_balance_sol()

        await trader._prime_seed_session(start_monitor=False)
        await _refresh_recovered_position_price(trader, seed["mint_id"], position)

        holding = trader.holdings.get(seed["mint_id"], {})
        buy_price = _decimal_from_value(
            holding.get("buy_price")
            or position.get("buy_price")
            or position.get("current_price")
            or position.get("last_price")
        )
        result = await trader.sell(
            seed["mint_id"],
            int(holding.get("token_balance", seed["token_balance"]) or 0),
            reason,
            seed["owner"],
            int(holding.get("trust_level", seed["trust_level"]) or 1),
            buy_price,
        )
        updated_store = load_managed_positions(config)
        updated_position = (updated_store.get("positions") or {}).get(seed["mint_id"], {})
        return {
            "result": result,
            "position": updated_position,
            "network": trader.config.runtime.network,
            "mode": trader.execution_label,
        }
    finally:
        if trader.swaps:
            await trader.swaps.close()
            await trader.swaps.async_client.close()
            trader.swaps = None
        if trader.pump_swap:
            await trader.pump_swap.close()
            trader.pump_swap = None
        if trader.async_client:
            await trader.async_client.close()
            trader.async_client = None
        if trader.session and not trader.session.closed:
            await trader.session.close()
            trader.session = None


def liquidate_managed_position(
    config,
    position: dict[str, Any],
    *,
    reason: str = "manual_manage_sell",
) -> dict[str, Any]:
    return asyncio.run(_liquidate_managed_position_async(config, position, reason=reason))


def run(mode_override=None, network_override=None, session_seed=None):
    config = load_config(mode_override, network_override=network_override)
    if not (session_seed and session_seed.get("skip_database")):
        ensure_local_postgres_running(config)
    errors, warnings = validate_config(config, "trader")
    if session_seed and session_seed.get("skip_database"):
        errors = [error for error in errors if "DATABASE_URL" not in error and "DB_*" not in error]
    if errors:
        raise RuntimeError("; ".join(errors))
    ensure_directories(config)
    configure_logging(config.paths.log_dir)
    for warning in warnings:
        logging.warning(warning)
    log_startup_summary(logging.getLogger("dexter.trader"), config, "trader")

    dexter = Dexter(config, session_seed=session_seed)
    asyncio.run(dexter.run())
    return 0


@dataclass(frozen=True)
class TuiOption:
    label: str
    value: str
    detail: str = ""


@dataclass(frozen=True)
class SettingsFieldSpec:
    key: str
    label: str
    detail: str
    kind: str = "text"
    options: tuple[str, ...] = ()
    secret: bool = False
    aliases: tuple[str, ...] = ()


def _legacy_tui_settings_fields() -> dict[str, SettingsFieldSpec]:
    return {
        key: SettingsFieldSpec(
            key=spec.key,
            label=spec.label,
            detail=spec.detail,
            kind=spec.kind,
            options=spec.options,
        )
        for key, spec in legacy_settings.LEGACY_SETTINGS_SPECS.items()
    }


def _legacy_tui_settings_sections() -> tuple[tuple[str, tuple[str, ...]], ...]:
    sections: list[tuple[str, tuple[str, ...]]] = []
    featured = tuple(
        key
        for key in legacy_settings.LEGACY_SETTINGS_FEATURED_KEYS
        if key in legacy_settings.LEGACY_SETTINGS_SPECS
    )
    if featured:
        sections.append(("Trading Quick Tweaks", featured))
    featured_set = set(featured)
    for section_name in legacy_settings.LEGACY_SETTINGS_SECTION_ORDER:
        keys = tuple(
            key
            for key, spec in legacy_settings.LEGACY_SETTINGS_SPECS.items()
            if spec.section == section_name and key not in featured_set
        )
        if keys:
            sections.append((section_name, keys))
    return tuple(sections)


TUI_SETTINGS_FIELDS: dict[str, SettingsFieldSpec] = {
    "DEXTER_NETWORK": SettingsFieldSpec(
        "DEXTER_NETWORK",
        "Default network",
        "Devnet uses Dexter's built-in Solana devnet RPCs. Mainnet uses HTTP_URL and WS_URL from .env.",
        kind="choice",
        options=("devnet", "mainnet"),
    ),
    "DEXTER_RUNTIME_MODE": SettingsFieldSpec(
        "DEXTER_RUNTIME_MODE",
        "Default runtime mode",
        "Read only never trades, paper simulates locally, simulate signs and simulates, live can send on-chain where allowed.",
        kind="choice",
        options=("read_only", "paper", "simulate", "live"),
    ),
    "PRIVATE_KEY": SettingsFieldSpec(
        "PRIVATE_KEY",
        "Primary private key",
        "Default signer Dexter uses for create, buy, sell, and balance lookup.",
        secret=True,
    ),
    "DEXTER_TRADING_PRIVATE_KEY": SettingsFieldSpec(
        "DEXTER_TRADING_PRIVATE_KEY",
        "Trading private key",
        "Optional explicit override for the trading signer. Leave empty to fall back to PRIVATE_KEY.",
        secret=True,
    ),
    "DEXTER_HOT_PRIVATE_KEY": SettingsFieldSpec(
        "DEXTER_HOT_PRIVATE_KEY",
        "Hot private key",
        "Optional second wallet label for future operator flows and snapshots.",
        secret=True,
    ),
    "DEXTER_TREASURY_ADDRESS": SettingsFieldSpec(
        "DEXTER_TREASURY_ADDRESS",
        "Treasury address",
        "Optional reporting destination label. This does not execute trades by itself.",
    ),
    "HTTP_URL": SettingsFieldSpec(
        "HTTP_URL",
        "Mainnet HTTP RPC",
        "Used only on mainnet. Devnet ignores this and uses Dexter's fixed devnet endpoints.",
    ),
    "WS_URL": SettingsFieldSpec(
        "WS_URL",
        "Mainnet WebSocket RPC",
        "Used only on mainnet. Devnet ignores this and uses Dexter's fixed devnet endpoints.",
    ),
    "DATABASE_URL": SettingsFieldSpec(
        "DATABASE_URL",
        "Database URL",
        "Preferred database setting. If present, Dexter uses this instead of the DB_* fields.",
        secret=True,
    ),
    "DEXTER_ENABLE_WSLOGS": SettingsFieldSpec(
        "DEXTER_ENABLE_WSLOGS",
        "Auto-start wsLogs",
        "When true, trader mode supervises the embedded wsLogs collector.",
        kind="bool",
    ),
    "DEXTER_DATASTORE_ENABLED": SettingsFieldSpec(
        "DEXTER_DATASTORE_ENABLED",
        "Research data storage",
        "Turns on the normalized replay, export, dashboard, and richer recovery layer.",
        kind="bool",
        aliases=("DEXTER_PHASE2_ENABLED",),
    ),
    "DEXTER_CLOSE_POSITIONS_ON_SHUTDOWN": SettingsFieldSpec(
        "DEXTER_CLOSE_POSITIONS_ON_SHUTDOWN",
        "Close live positions on shutdown",
        "When true, live mode attempts best-effort liquidation during shutdown.",
        kind="bool",
    ),
    "DEXTER_MAINNET_DRY_RUN": SettingsFieldSpec(
        "DEXTER_MAINNET_DRY_RUN",
        "Mainnet dry run",
        "Keep this true to simulate mainnet live sends instead of submitting them.",
        kind="bool",
    ),
    "DEXTER_ALLOW_MAINNET_LIVE": SettingsFieldSpec(
        "DEXTER_ALLOW_MAINNET_LIVE",
        "Allow live mainnet sends",
        "This only matters when mainnet dry run is false.",
        kind="bool",
    ),
    "DEXTER_STRATEGY_PROFILE": SettingsFieldSpec(
        "DEXTER_STRATEGY_PROFILE",
        "Strategy profile",
        "Default strategy profile Dexter uses for entry evaluation.",
        kind="choice",
        options=("aggressive", "balanced", "conservative"),
    ),
    "DEXTER_PER_TRADE_SOL_CAP": SettingsFieldSpec(
        "DEXTER_PER_TRADE_SOL_CAP",
        "Per-trade SOL cap",
        "Maximum SOL spend per new entry.",
        kind="decimal",
    ),
    "DEXTER_SESSION_SOL_CAP": SettingsFieldSpec(
        "DEXTER_SESSION_SOL_CAP",
        "Session SOL cap",
        "Maximum SOL spend for a single session.",
        kind="decimal",
    ),
    "DEXTER_DAILY_SOL_CAP": SettingsFieldSpec(
        "DEXTER_DAILY_SOL_CAP",
        "Daily SOL cap",
        "Gross UTC-day spend ceiling across all entries.",
        kind="decimal",
    ),
    "DEXTER_DAILY_DRAWDOWN_STOP_SOL": SettingsFieldSpec(
        "DEXTER_DAILY_DRAWDOWN_STOP_SOL",
        "Daily drawdown stop",
        "Stops new entries after enough realized losses in the UTC day.",
        kind="decimal",
    ),
    "DEXTER_MAX_CONCURRENT_SESSIONS": SettingsFieldSpec(
        "DEXTER_MAX_CONCURRENT_SESSIONS",
        "Max concurrent sessions",
        "Maximum active sessions Dexter can hold at once.",
        kind="int",
    ),
    "DEXTER_PER_CREATOR_MAX_SESSIONS": SettingsFieldSpec(
        "DEXTER_PER_CREATOR_MAX_SESSIONS",
        "Max sessions per creator",
        "Maximum simultaneous sessions from the same creator.",
        kind="int",
    ),
    "DEXTER_WALLET_RESERVE_FLOOR_SOL": SettingsFieldSpec(
        "DEXTER_WALLET_RESERVE_FLOOR_SOL",
        "Wallet reserve floor",
        "Minimum SOL Dexter tries to keep unspent in the wallet.",
        kind="decimal",
    ),
    "DEXTER_DESKTOP_NOTIFICATIONS": SettingsFieldSpec(
        "DEXTER_DESKTOP_NOTIFICATIONS",
        "Desktop notifications",
        "Enable local `notify-send` alerts for runtime failures and notable setup events on this machine.",
        kind="bool",
    ),
    "DEXTER_TELEGRAM_BOT_TOKEN": SettingsFieldSpec(
        "DEXTER_TELEGRAM_BOT_TOKEN",
        "Telegram bot token",
        "Optional Telegram bot token for alerts. While Dexter stays open, message the bot /id to auto-fill the chat id.",
        secret=True,
    ),
    "DEXTER_TELEGRAM_CHAT_ID": SettingsFieldSpec(
        "DEXTER_TELEGRAM_CHAT_ID",
        "Telegram chat id",
        "Optional Telegram destination chat id for alerts. Dexter can auto-fill this after the bot replies to /id.",
        secret=True,
    ),
    "DEXTER_DISCORD_WEBHOOK_URL": SettingsFieldSpec(
        "DEXTER_DISCORD_WEBHOOK_URL",
        "Discord webhook",
        "Optional Discord webhook URL for alerts.",
        secret=True,
    ),
    "DEXTER_LOG_DIR": SettingsFieldSpec(
        "DEXTER_LOG_DIR",
        "Log directory",
        "Directory where Dexter writes its local logs.",
        kind="path",
    ),
    "DEXTER_STATE_DIR": SettingsFieldSpec(
        "DEXTER_STATE_DIR",
        "State directory",
        "Directory where runtime snapshots and the recovery store live.",
        kind="path",
    ),
    "DEXTER_DATASTORE_EXPORT_DIR": SettingsFieldSpec(
        "DEXTER_DATASTORE_EXPORT_DIR",
        "Research export directory",
        "Destination for replay and export artifacts.",
        kind="path",
        aliases=("DEXTER_PHASE2_EXPORT_DIR",),
    ),
}
TUI_SETTINGS_FIELDS.update(_legacy_tui_settings_fields())

TUI_SETTINGS_SECTIONS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "Quick Setup",
        (
            "DEXTER_NETWORK",
            "DEXTER_RUNTIME_MODE",
            "PRIVATE_KEY",
            "DATABASE_URL",
            "HTTP_URL",
            "WS_URL",
        ),
    ),
    *_legacy_tui_settings_sections(),
    (
        "Wallets & RPC",
        (
            "PRIVATE_KEY",
            "DEXTER_TRADING_PRIVATE_KEY",
            "DEXTER_HOT_PRIVATE_KEY",
            "DEXTER_TREASURY_ADDRESS",
            "HTTP_URL",
            "WS_URL",
        ),
    ),
    (
        "Runtime & Safety",
        (
            "DEXTER_ENABLE_WSLOGS",
            "DEXTER_DATASTORE_ENABLED",
            "DEXTER_CLOSE_POSITIONS_ON_SHUTDOWN",
            "DEXTER_MAINNET_DRY_RUN",
            "DEXTER_ALLOW_MAINNET_LIVE",
        ),
    ),
    (
        "Risk & Strategy",
        (
            "DEXTER_STRATEGY_PROFILE",
            "DEXTER_PER_TRADE_SOL_CAP",
            "DEXTER_SESSION_SOL_CAP",
            "DEXTER_DAILY_SOL_CAP",
            "DEXTER_DAILY_DRAWDOWN_STOP_SOL",
            "DEXTER_MAX_CONCURRENT_SESSIONS",
            "DEXTER_PER_CREATOR_MAX_SESSIONS",
            "DEXTER_WALLET_RESERVE_FLOOR_SOL",
        ),
    ),
    (
        "Alerts & Paths",
        (
            "DEXTER_DESKTOP_NOTIFICATIONS",
            "DEXTER_TELEGRAM_BOT_TOKEN",
            "DEXTER_TELEGRAM_CHAT_ID",
            "DEXTER_DISCORD_WEBHOOK_URL",
            "DEXTER_LOG_DIR",
            "DEXTER_STATE_DIR",
            "DEXTER_DATASTORE_EXPORT_DIR",
        ),
    ),
)


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _coalesce_env_value(env_values: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = (env_values.get(key) or "").strip()
        if value:
            return value
    return ""


def _field_env_value(spec: SettingsFieldSpec, env_values: dict[str, str], example_env: dict[str, str]) -> str:
    for key in (spec.key, *spec.aliases):
        value = (env_values.get(key) or "").strip()
        if value:
            return value
    for key in (spec.key, *spec.aliases):
        value = (example_env.get(key) or "").strip()
        if value:
            return value
    return ""


def database_config_needs_setup(env_values: dict[str, str]) -> bool:
    database_url = (env_values.get("DATABASE_URL") or "").strip()
    if database_url:
        return env_value_needs_setup("DATABASE_URL", database_url)
    db_password = (env_values.get("DB_PASSWORD") or "").strip()
    if db_password:
        return env_value_needs_setup("DB_PASSWORD", db_password)
    return False


def env_value_needs_setup(key: str, value: str | None) -> bool:
    text = (value or "").strip()
    if not text:
        return True
    lowered = text.lower()
    if key in {"PRIVATE_KEY", "DEXTER_TRADING_PRIVATE_KEY", "DEXTER_HOT_PRIVATE_KEY"}:
        return lowered in {"replace-me", "changeme", "your-private-key", "your_private_key"}
    if key in {"DATABASE_URL", "POSTGRES_ADMIN_DSN"}:
        return "replace-me" in lowered or "<password>" in lowered
    if key in {"DB_PASSWORD", "POSTGRES_ADMIN_PASSWORD"}:
        return "replace-me" in lowered
    if key in {"HTTP_URL", "WS_URL"}:
        return "replace-me" in lowered or "example.invalid" in lowered
    if key in {"DEXTER_TELEGRAM_BOT_TOKEN", "DEXTER_TELEGRAM_CHAT_ID", "DEXTER_DISCORD_WEBHOOK_URL"}:
        return lowered in {"replace-me", "changeme"}
    return False


def build_run_argv(*, target: str, mode: str, network: str, doctor_first: bool) -> list[str]:
    argv = ["run", "--target", target, "--mode", mode, "--network", network]
    if doctor_first:
        argv.append("--doctor-first")
    return argv


def build_seeded_create_argv(form: dict[str, Any]) -> list[str]:
    mint = str(form["mint"])
    argv = [
        "create",
        "--mode",
        str(form["mode"]),
        "--network",
        str(form["network"]),
        "--mint",
        mint,
        "--owner",
        str(form["owner"]),
        "--name",
        str(form.get("name") or mint[:8]),
        "--market",
        str(form.get("market") or "pump_fun"),
        "--token-balance",
        str(form.get("token_balance") or "0"),
        "--cost-basis-lamports",
        str(form.get("cost_basis_lamports") or "0"),
        "--trust-level",
        str(form.get("trust_level") or "1"),
    ]
    if form.get("bonding_curve"):
        argv.extend(["--bonding-curve", str(form["bonding_curve"])])
    if form.get("buy_price"):
        argv.extend(["--buy-price", str(form["buy_price"])])
    if bool(form.get("load_database")):
        argv.append("--load-database")
    if bool(form.get("dry_run")):
        argv.append("--dry-run")
    return argv


def build_onchain_create_argv(form: dict[str, Any]) -> list[str]:
    argv = [
        "create",
        "--mode",
        str(form["mode"]),
        "--network",
        str(form["network"]),
        "--name",
        str(form["name"]),
        "--symbol",
        str(form["symbol"]),
        "--buy-sol",
        str(form.get("buy_sol") or "0.01"),
    ]
    if form.get("uri"):
        argv.extend(["--uri", str(form["uri"])])
    elif form.get("image"):
        argv.extend(["--image", str(form["image"])])
        if form.get("description"):
            argv.extend(["--description", str(form["description"])])

    tx_action = str(form.get("tx_action") or "send_and_follow")
    if tx_action == "build_only":
        argv.append("--dry-run")
    elif tx_action == "simulate":
        argv.append("--simulate-tx")
    elif bool(form.get("no_follow")):
        argv.append("--no-follow")

    if bool(form.get("load_database")):
        argv.append("--load-database")
    return argv


def build_manage_argv(*, mode: str, network: str, sell_mint: str | None = None, sell_all: bool = False) -> list[str]:
    argv = ["manage", "--mode", mode, "--network", network]
    if sell_all:
        argv.append("--sell-all")
    elif sell_mint:
        argv.extend(["--sell-mint", sell_mint])
    return argv


def onboarding_fields_for_intent(
    intent: str,
    *,
    mode: str,
    network: str,
    env_values: dict[str, str],
) -> list[str]:
    keys: list[str] = []
    wallet_value = _coalesce_env_value(env_values, "DEXTER_TRADING_PRIVATE_KEY", "PRIVATE_KEY")

    if intent == "startup":
        if env_value_needs_setup("PRIVATE_KEY", wallet_value):
            keys.append("PRIVATE_KEY")
        if database_config_needs_setup(env_values):
            keys.append("DATABASE_URL")
        if network == "mainnet":
            if env_value_needs_setup("HTTP_URL", env_values.get("HTTP_URL")):
                keys.append("HTTP_URL")
            if env_value_needs_setup("WS_URL", env_values.get("WS_URL")):
                keys.append("WS_URL")
        return _dedupe_strings(keys)

    needs_wallet = (
        intent == "create:onchain"
        or (intent in {"run:trade", "create:seeded", "manage:sell", "manage:sell_all"} and mode in {"simulate", "live"})
    )
    needs_database = intent in {"run:trade", "run:collector", "run:analyze", "create:onchain", "create:seeded:database"}

    if needs_wallet and env_value_needs_setup("PRIVATE_KEY", wallet_value):
        keys.append("PRIVATE_KEY")
    if needs_database and database_config_needs_setup(env_values):
        keys.append("DATABASE_URL")
    if network == "mainnet":
        if env_value_needs_setup("HTTP_URL", env_values.get("HTTP_URL")):
            keys.append("HTTP_URL")
        if env_value_needs_setup("WS_URL", env_values.get("WS_URL")):
            keys.append("WS_URL")
    return _dedupe_strings(keys)


def validation_fields_from_messages(messages: list[str], *, network: str) -> list[str]:
    fields: list[str] = []
    for message in messages:
        if "PRIVATE_KEY" in message or "TRADING_PRIVATE_KEY" in message:
            fields.append("PRIVATE_KEY")
        if "DATABASE_URL" in message or "DB_*" in message:
            fields.append("DATABASE_URL")
        if "HTTP RPC URL" in message and network == "mainnet":
            fields.append("HTTP_URL")
        if "WebSocket RPC URL" in message and network == "mainnet":
            fields.append("WS_URL")
    return _dedupe_strings(fields)


class InteractiveDexterCLI:
    def __init__(self):
        self.balance_cache: int | None = None
        self.balance_checked_at = 0.0
        self.balance_cache_key: tuple[str, str, str] | None = None
        self.balance_refresh_key: tuple[str, str, str] | None = None
        self.balance_refresh_thread: threading.Thread | None = None
        self.balance_refresh_lock = threading.Lock()
        self.position_counts_cache: dict[str, int] | None = None
        self.position_counts_cache_key: tuple[str, int] | None = None
        self.screen = None
        self._click_regions: list[tuple[int, int, int, int, str]] = []
        self.example_env = read_env_values(Path(DEX_DIR) / ".env.example")
        self.status_message = ""
        self.telegram_command_service: TelegramCommandService | None = None

    def _load_config(self, *, mode: str | None = None, network: str | None = None):
        return load_config(mode, network_override=network)

    def _telegram_service_state_path(self) -> Path:
        env_values = read_env_values()
        state_dir = (
            env_values.get("DEXTER_STATE_DIR")
            or self.example_env.get("DEXTER_STATE_DIR")
            or str(Path(DEX_DIR) / "dev" / "state")
        )
        return Path(state_dir).expanduser() / "telegram-command-service.json"

    def _refresh_telegram_command_service(self) -> None:
        env_values = read_env_values()
        token = (env_values.get("DEXTER_TELEGRAM_BOT_TOKEN") or "").strip()
        state_path = self._telegram_service_state_path()
        if self.telegram_command_service is None:
            self.telegram_command_service = TelegramCommandService(
                bot_token=token,
                state_path=state_path,
            )
            if token:
                self.telegram_command_service.start()
            return
        self.telegram_command_service.state_path = state_path
        self.telegram_command_service.refresh(token)

    def _shutdown_background_services(self) -> None:
        if self.telegram_command_service is not None:
            self.telegram_command_service.stop()

    def _invalidate_tui_caches(self, *, clear_balance: bool = True) -> None:
        load_config.cache_clear()
        self.position_counts_cache = None
        self.position_counts_cache_key = None
        if clear_balance:
            self.balance_cache = None
        self.balance_checked_at = 0.0
        self.balance_cache_key = None
        self.balance_refresh_key = None

    def _safe_config(self, *, mode: str | None = None, network: str | None = None):
        try:
            return self._load_config(mode=mode, network=network), None
        except Exception as exc:
            return None, str(exc)

    def _wallet_label(self, config) -> str:
        private_key = config.wallets.trading_private_key or config.rpc.private_key
        if not private_key:
            return "wallet=(unset)"
        try:
            pubkey = Keypair.from_bytes(base58.b58decode(private_key)).pubkey()
            return f"wallet={_render_short_key(str(pubkey))}"
        except Exception:
            return "wallet=(invalid)"

    def _managed_position_counts(self, config) -> dict[str, int]:
        path = _managed_positions_path(config)
        try:
            mtime_ns = path.stat().st_mtime_ns
        except OSError:
            mtime_ns = -1
        cache_key = (str(path), int(mtime_ns))
        if self.position_counts_cache_key != cache_key or self.position_counts_cache is None:
            self.position_counts_cache = summarize_managed_positions(config)
            self.position_counts_cache_key = cache_key
        return dict(self.position_counts_cache)

    def _start_balance_refresh(self, config, cache_key: tuple[str, str, str]) -> None:
        with self.balance_refresh_lock:
            if (
                self.balance_refresh_thread is not None
                and self.balance_refresh_thread.is_alive()
                and self.balance_refresh_key == cache_key
            ):
                return

            self.balance_refresh_key = cache_key

            def worker() -> None:
                try:
                    balance = fetch_wallet_balance_lamports(config)
                except Exception:
                    balance = None
                with self.balance_refresh_lock:
                    self.balance_cache = balance
                    self.balance_checked_at = time.time()
                    self.balance_cache_key = cache_key
                    self.balance_refresh_key = None
                    self.balance_refresh_thread = None

            thread = threading.Thread(
                target=worker,
                name="dexter-tui-balance-refresh",
                daemon=True,
            )
            self.balance_refresh_thread = thread
            thread.start()

    def _balance_line(self, config) -> str:
        cache_key = (
            config.runtime.network,
            config.runtime.mode,
            config.wallets.trading_private_key or config.rpc.private_key or "",
        )
        cache_stale = (time.time() - self.balance_checked_at) > 10
        cache_miss = self.balance_cache_key != cache_key
        if cache_miss:
            self.balance_cache = None
            self.balance_checked_at = 0.0
            self.balance_cache_key = cache_key

        if cache_stale or cache_miss:
            self._start_balance_refresh(config, cache_key)

        if self.balance_cache is None:
            if self.balance_refresh_key == cache_key:
                return "balance=(refreshing)"
            return "balance=(unavailable)"
        balance_sol = Decimal(int(self.balance_cache)) / Decimal("1000000000")
        return f"balance={balance_sol:.6f} SOL"

    def _run_legacy(self, argv: list[str]) -> int:
        import dexter_cli as dexter_commands

        return int(dexter_commands.legacy_main(argv))

    def _init_screen(self, stdscr) -> None:
        self.screen = stdscr
        self._refresh_telegram_command_service()
        try:
            curses.curs_set(0)
        except curses.error:
            pass
        self.screen.keypad(True)
        curses.mousemask(self._menu_mousemask())
        curses.mouseinterval(120)
        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_CYAN, -1)
            curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_CYAN)
            curses.init_pair(3, curses.COLOR_GREEN, -1)
            curses.init_pair(4, curses.COLOR_YELLOW, -1)
            curses.init_pair(5, curses.COLOR_RED, -1)
            curses.init_pair(6, curses.COLOR_WHITE, -1)

    def _attr(self, name: str) -> int:
        if not curses.has_colors():
            return {
                "selected": curses.A_REVERSE | curses.A_BOLD,
                "muted": curses.A_DIM,
                "title": curses.A_BOLD,
                "error": curses.A_BOLD,
                "warning": curses.A_BOLD,
                "success": curses.A_BOLD,
            }.get(name, 0)
        return {
            "title": curses.color_pair(1) | curses.A_BOLD,
            "selected": curses.color_pair(2) | curses.A_BOLD,
            "success": curses.color_pair(3) | curses.A_BOLD,
            "warning": curses.color_pair(4) | curses.A_BOLD,
            "error": curses.color_pair(5) | curses.A_BOLD,
            "muted": curses.color_pair(6) | curses.A_DIM,
        }.get(name, 0)

    def _write(self, y: int, x: int, text: str, attr: int = 0) -> None:
        if self.screen is None:
            return
        height, width = self.screen.getmaxyx()
        if y < 0 or y >= height or x >= width:
            return
        clipped_width = max(0, width - x - 1)
        if clipped_width <= 0:
            return
        try:
            self.screen.addnstr(y, x, text, clipped_width, attr)
        except curses.error:
            return

    def _wrap(self, text: str, width: int) -> list[str]:
        if width <= 4:
            return [text[: max(0, width)]]
        return textwrap.wrap(text, width=width) or [""]

    def _register_click(self, y1: int, x1: int, y2: int, x2: int, value: str) -> None:
        self._click_regions.append((y1, x1, y2, x2, value))

    def _clicked_value(self, y: int, x: int) -> str | None:
        for top, left, bottom, right, value in self._click_regions:
            if top <= y <= bottom and left <= x <= right:
                return value
        return None

    def _menu_mousemask(self) -> int:
        mask = 0
        for name in (
            "BUTTON1_PRESSED",
            "BUTTON1_RELEASED",
            "BUTTON1_CLICKED",
            "BUTTON1_DOUBLE_CLICKED",
            "BUTTON1_TRIPLE_CLICKED",
        ):
            mask |= int(getattr(curses, name, 0) or 0)
        return mask or curses.ALL_MOUSE_EVENTS

    def _mouse_activates_selection(self, mouse_state: int) -> bool:
        activation_mask = 0
        for name in (
            "BUTTON1_RELEASED",
            "BUTTON1_CLICKED",
            "BUTTON1_DOUBLE_CLICKED",
            "BUTTON1_TRIPLE_CLICKED",
        ):
            activation_mask |= int(getattr(curses, name, 0) or 0)
        return bool(mouse_state & activation_mask)

    def _flush_pending_input(self) -> None:
        try:
            curses.flushinp()
        except curses.error:
            pass

    def _restore_screen_after_command(self) -> None:
        try:
            curses.reset_prog_mode()
        except curses.error:
            return
        if self.screen is not None:
            try:
                self.screen.refresh()
            except curses.error:
                pass
        self._flush_pending_input()
        try:
            curses.curs_set(0)
        except curses.error:
            pass

    def _draw_header(self, title: str, config, *, config_error: str | None = None) -> int:
        if self.screen is None:
            return 0
        self.screen.erase()
        self._click_regions = []
        height, width = self.screen.getmaxyx()
        border = "═" * max(8, width - 4)
        self._write(1, 2, "DEXTER 3.0", self._attr("title"))
        self._write(2, 2, title, self._attr("selected"))
        if config is not None:
            counts = self._managed_position_counts(config)
            summary = (
                f"network={config.runtime.network}  mode={config.runtime.mode}  "
                f"{self._wallet_label(config)}  {self._balance_line(config)}  "
                f"positions=open:{counts['open']} pending:{counts['pending']} closed:{counts['closed']}"
            )
            self._write(4, 2, summary, self._attr("muted"))
        elif config_error:
            self._write(4, 2, f"config_error={config_error}", self._attr("error"))
        self._write(5, 2, border[: max(8, width - 4)], self._attr("muted"))
        return 7

    def _draw_footer(self, lines: list[str]) -> None:
        if self.screen is None:
            return
        height, width = self.screen.getmaxyx()
        for offset, line in enumerate(reversed(lines)):
            y = height - 2 - offset
            if y <= 0:
                break
            self._write(y, 2, line[: max(0, width - 4)], self._attr("muted"))

    def _show_splash_tui(self) -> None:
        if self.screen is None:
            return
        art = [
            "██████╗ ███████╗██╗  ██╗████████╗███████╗██████╗ ",
            "██╔══██╗██╔════╝╚██╗██╔╝╚══██╔══╝██╔════╝██╔══██╗",
            "██║  ██║█████╗   ╚███╔╝    ██║   █████╗  ██████╔╝",
            "██║  ██║██╔══╝   ██╔██╗    ██║   ██╔══╝  ██╔══██╗",
            "██████╔╝███████╗██╔╝ ██╗   ██║   ███████╗██║  ██║",
            "╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝",
        ]
        taglines = (
            f"BLOODY FAST.",
        )
        height, width = self.screen.getmaxyx()
        steps = 12
        for idx in range(steps + 1):
            self.screen.erase()
            top = max(1, (height - len(art) - len(taglines) - 6) // 2)
            for line_index, line in enumerate(art):
                x = max(2, (width - len(line)) // 2)
                self._write(top + line_index, x, line, self._attr("title"))
            for line_index, line in enumerate(taglines):
                x = max(2, (width - len(line)) // 2)
                self._write(top + len(art) + 1 + line_index, x, line, self._attr("muted"))
            progress = idx / steps
            filled = int(progress * 28)
            bar = "[" + ("#" * filled) + (" " * (28 - filled)) + "]"
            status = f"{CLI_SPLASH_FRAMES[idx % len(CLI_SPLASH_FRAMES)]} loading {bar}"
            self._write(top + len(art) + len(taglines) + 3, max(2, (width - len(status)) // 2), status, self._attr("success"))
            self.screen.refresh()
            time.sleep(0.05)

    def _default_index(self, options: list[TuiOption], value: str | None) -> int:
        if value is None:
            return 0
        for index, option in enumerate(options):
            if option.value == value:
                return index
        return 0

    def _build_choice_rows(
        self,
        options: list[TuiOption],
        *,
        body_width: int,
    ) -> tuple[list[tuple[str, int | None, int, str]], list[tuple[int, int]]]:
        rows: list[tuple[str, int | None, int, str]] = []
        option_bounds: list[tuple[int, int]] = []

        for index, option in enumerate(options):
            start = len(rows)
            rows.append(("label", index, 4, option.label))
            if option.detail:
                for wrapped in self._wrap(option.detail, max(12, body_width - 6)):
                    rows.append(("detail", index, 8, wrapped))
            rows.append(("spacer", None, 0, ""))
            option_bounds.append((start, len(rows) - 1))

        return rows, option_bounds

    def _scroll_choice_view(
        self,
        option_bounds: list[tuple[int, int]],
        *,
        selected: int,
        scroll_top: int,
        visible_rows: int,
    ) -> int:
        if not option_bounds or visible_rows <= 0:
            return 0
        start, end = option_bounds[selected]
        if start < scroll_top:
            return start
        if end >= (scroll_top + visible_rows):
            return max(0, end - visible_rows + 1)
        return max(0, scroll_top)

    def _choose(
        self,
        title: str,
        options: list[TuiOption],
        *,
        intro: list[str] | None = None,
        config=None,
        config_error: str | None = None,
        default_value: str | None = None,
        allow_escape: bool = True,
    ) -> str | None:
        if self.screen is None:
            return None
        selected = self._default_index(options, default_value)
        scroll_top = 0
        while True:
            y = self._draw_header(title, config, config_error=config_error)
            height, width = self.screen.getmaxyx()
            body_width = max(24, width - 8)
            if intro:
                for line in intro:
                    for wrapped in self._wrap(line, body_width):
                        self._write(y, 2, wrapped)
                        y += 1
                y += 1
            footer_lines = ["Use ↑/↓ to move, Enter to select, click to choose."]
            if allow_escape:
                footer_lines.append("Press q or Esc to go back.")
            if self.status_message:
                footer_lines.append(self.status_message)
            rows, option_bounds = self._build_choice_rows(options, body_width=body_width)
            visible_rows = max(1, height - y - len(footer_lines) - 2)
            if len(rows) > visible_rows:
                insert_at = len(footer_lines) - 1 if self.status_message else len(footer_lines)
                footer_lines.insert(insert_at, "")
            visible_rows = max(1, height - y - len(footer_lines) - 2)
            scroll_top = self._scroll_choice_view(
                option_bounds,
                selected=selected,
                scroll_top=scroll_top,
                visible_rows=visible_rows,
            )
            max_scroll_top = max(0, len(rows) - visible_rows)
            scroll_top = min(scroll_top, max_scroll_top)
            hidden_above = scroll_top > 0
            hidden_below = (scroll_top + visible_rows) < len(rows)
            if len(rows) > visible_rows:
                hints: list[str] = []
                if hidden_above:
                    hints.append("↑ above")
                if hidden_below:
                    hints.append("↓ below")
                footer_lines[-2 if self.status_message else -1] = (
                    f"More content: {'  '.join(hints)}" if hints else "More content is available."
                )
            visible_row_slice = rows[scroll_top:scroll_top + visible_rows]
            for row_offset, (kind, option_index, indent, text) in enumerate(visible_row_slice):
                row_y = y + row_offset
                if kind == "spacer":
                    continue
                if option_index is None:
                    continue
                option = options[option_index]
                if kind == "label":
                    prefix = ">" if option_index == selected else " "
                    rendered = f"{prefix} {text}"
                    attr = self._attr("selected") if option_index == selected else 0
                else:
                    rendered = text
                    attr = self._attr("title") if option_index == selected else self._attr("muted")
                self._write(row_y, indent, rendered, attr)
                self._register_click(
                    row_y,
                    indent,
                    row_y,
                    min(width - 3, indent + len(rendered)),
                    option.value,
                )
            self._draw_footer(footer_lines)
            self.screen.refresh()
            key = self.screen.get_wch()
            if key in (curses.KEY_UP, "k"):
                selected = (selected - 1) % len(options)
                continue
            if key in (curses.KEY_DOWN, "j"):
                selected = (selected + 1) % len(options)
                continue
            if key in ("\n", "\r") or key == curses.KEY_ENTER:
                return options[selected].value
            if key == curses.KEY_MOUSE:
                try:
                    _, mouse_x, mouse_y, _, mouse_state = curses.getmouse()
                except curses.error:
                    continue
                clicked = self._clicked_value(mouse_y, mouse_x)
                if clicked is None:
                    continue
                for index, option in enumerate(options):
                    if option.value == clicked:
                        selected = index
                        break
                if self._mouse_activates_selection(mouse_state):
                    self._flush_pending_input()
                    return clicked
                continue
            if allow_escape and key in ("\x1b", "q", "Q"):
                return None

    def _input_text(
        self,
        title: str,
        *,
        default: str = "",
        detail: str = "",
        required: bool = False,
        secret: bool = False,
        validator=None,
    ) -> str | None:
        if self.screen is None:
            return None
        value = default
        error = ""
        while True:
            current_config, current_config_error = self._safe_config()
            y = self._draw_header(title, current_config, config_error=current_config_error)
            width = self.screen.getmaxyx()[1]
            body_width = max(24, width - 8)
            if detail:
                for wrapped in self._wrap(detail, body_width):
                    self._write(y, 2, wrapped)
                    y += 1
                y += 1
            rendered = "*" * len(value) if secret and value else value
            self._write(y, 2, "Input", self._attr("title"))
            self._write(y + 1, 4, rendered or "(empty)", self._attr("selected"))
            if error:
                self._write(y + 3, 2, error, self._attr("error"))
            self._draw_footer(
                [
                    "Type to edit, Backspace to erase, Enter to save.",
                    "Press Esc to cancel this field.",
                ]
            )
            self.screen.refresh()
            key = self.screen.get_wch()
            if key in ("\n", "\r") or key == curses.KEY_ENTER:
                candidate = value.strip()
                if required and not candidate:
                    error = "A value is required here."
                    continue
                if validator is not None:
                    validation_error = validator(candidate)
                    if validation_error:
                        error = validation_error
                        continue
                return candidate
            if key == "\x1b":
                return None
            if key in (curses.KEY_BACKSPACE, "\b", "\x7f"):
                value = value[:-1]
                error = ""
                continue
            if isinstance(key, str) and key.isprintable():
                value += key
                error = ""

    def _confirm(self, title: str, detail: str, *, default_yes: bool = True) -> bool | None:
        options = [
            TuiOption("Yes", "yes", "Proceed with this action."),
            TuiOption("No", "no", "Leave things as they are."),
        ]
        config, config_error = self._safe_config()
        choice = self._choose(
            title,
            options,
            intro=[detail],
            config=config,
            config_error=config_error,
            default_value="yes" if default_yes else "no",
        )
        if choice is None:
            return None
        return choice == "yes"

    def _message(self, title: str, lines: list[str], *, button_label: str = "Back") -> None:
        config, config_error = self._safe_config()
        self._choose(
            title,
            [TuiOption(button_label, "back")],
            intro=lines,
            config=config,
            config_error=config_error,
        )

    def _suspend_for_command(self, argv: list[str], *, prompt: str) -> int:
        if self.screen is None:
            return self._run_legacy(argv)
        curses.def_prog_mode()
        try:
            curses.endwin()
        except curses.error:
            pass
        exit_code = 1
        prompt_interrupted = False
        try:
            exit_code = self._run_legacy(argv)
        except Exception as exc:
            print(f"Dexter command failed: {exc}")
            traceback.print_exc()
            exit_code = 1
        try:
            input(f"\n{prompt.format(exit_code=exit_code)}")
        except EOFError:
            pass
        except KeyboardInterrupt:
            prompt_interrupted = True
        finally:
            self._invalidate_tui_caches()
            self._restore_screen_after_command()
        if prompt_interrupted:
            self.status_message = "Command prompt interrupted. Returned to Dexter."
        return exit_code

    def _display_env_value(self, spec: SettingsFieldSpec, env_values: dict[str, str]) -> str:
        value = _field_env_value(spec, env_values, self.example_env)
        if not value and spec.key in legacy_settings.LEGACY_SETTINGS_SPECS:
            current_value = legacy_settings.snapshot_legacy_settings().get(spec.key)
            if current_value is not None:
                value = str(current_value)
        if not value:
            return "(unset)"
        if spec.secret:
            return _render_short_key(value)
        return value

    def _validate_field_value(self, spec: SettingsFieldSpec, raw: str) -> str | None:
        if spec.kind == "choice" and raw not in spec.options:
            return f"Choose one of: {', '.join(spec.options)}"
        if spec.kind == "bool" and raw not in {"true", "false"}:
            return "Choose true or false."
        if spec.kind == "int" and raw:
            try:
                int(raw)
            except ValueError:
                return "Enter a whole number."
        if spec.kind == "decimal" and raw:
            try:
                Decimal(raw)
            except Exception:
                return "Enter a decimal number."
        return None

    def _save_env_updates(self, updates: dict[str, str]) -> bool:
        try:
            update_env_file(updates)
            legacy_settings.reload_and_sync_loaded_modules()
            self._invalidate_tui_caches()
            self._refresh_telegram_command_service()
            self.status_message = f"Saved {', '.join(updates)} to .env and refreshed Dexter config."
            if "DEXTER_TELEGRAM_BOT_TOKEN" in updates and updates["DEXTER_TELEGRAM_BOT_TOKEN"].strip():
                self.status_message += " Message the bot /id while Dexter is open to auto-fill the chat id."
            return True
        except Exception as exc:
            self.status_message = f"Could not save config: {exc}"
            self._message("Save Failed", [str(exc)])
            return False

    def _edit_env_field(self, spec: SettingsFieldSpec, *, required: bool = False) -> bool:
        env_values = read_env_values()
        current = _field_env_value(spec, env_values, self.example_env)
        if spec.kind == "choice":
            options = [TuiOption(option, option, spec.detail) for option in spec.options]
            selected = self._choose(
                spec.label,
                options,
                intro=[spec.detail],
                config=self._safe_config()[0],
                config_error=self._safe_config()[1],
                default_value=current or (spec.options[0] if spec.options else None),
            )
            if selected is None:
                return False
            new_value = selected
        elif spec.kind == "bool":
            choice = self._choose(
                spec.label,
                [
                    TuiOption("true", "true", "Enable this setting."),
                    TuiOption("false", "false", "Disable this setting."),
                ],
                intro=[spec.detail],
                config=self._safe_config()[0],
                config_error=self._safe_config()[1],
                default_value=(current or "false").lower(),
            )
            if choice is None:
                return False
            new_value = choice
        else:
            new_value = self._input_text(
                spec.label,
                default=current,
                detail=spec.detail,
                required=required,
                secret=spec.secret,
                validator=lambda raw: self._validate_field_value(spec, raw),
            )
            if new_value is None:
                return False

        if spec.key == "DEXTER_ALLOW_MAINNET_LIVE" and new_value == "true":
            confirmed = self._confirm(
                "Confirm Mainnet Live Unlock",
                "This removes one of Dexter's mainnet send safety gates. Leave DEXTER_MAINNET_DRY_RUN=true unless you fully intend to send on-chain.",
                default_yes=False,
            )
            if confirmed is not True:
                return False
        if spec.key == "DEXTER_MAINNET_DRY_RUN" and new_value == "false":
            confirmed = self._confirm(
                "Disable Mainnet Dry Run",
                "Mainnet dry run=false allows real mainnet sends when the separate live unlock is also true.",
                default_yes=False,
            )
            if confirmed is not True:
                return False

        if self._save_env_updates({spec.key: new_value}):
            config, config_error = self._safe_config()
            if config_error:
                self._message("Config Needs Attention", [config_error])
            elif config is not None:
                errors, warnings = validate_config(config, "trader")
                if errors:
                    self.status_message = errors[0]
                elif warnings:
                    self.status_message = warnings[0]
            return True
        return False

    def _resolved_defaults(self, *, mode: str | None = None, network: str | None = None) -> tuple[str, str]:
        env_values = read_env_values()
        resolved_network = (network or env_values.get("DEXTER_NETWORK") or self.example_env.get("DEXTER_NETWORK") or "devnet").strip().lower() or "devnet"
        resolved_mode = (mode or env_values.get("DEXTER_RUNTIME_MODE") or self.example_env.get("DEXTER_RUNTIME_MODE") or "read_only").strip().lower() or "read_only"
        if resolved_network not in {"devnet", "mainnet"}:
            resolved_network = "devnet"
        if resolved_mode not in {"read_only", "paper", "simulate", "live"}:
            resolved_mode = "read_only"
        return resolved_mode, resolved_network

    def _run_onboarding(
        self,
        intent: str,
        *,
        field_keys: list[str] | None = None,
        mode: str | None = None,
        network: str | None = None,
    ) -> bool:
        resolved_mode, resolved_network = self._resolved_defaults(mode=mode, network=network)
        env_values = read_env_values()
        keys = field_keys or onboarding_fields_for_intent(
            intent,
            mode=resolved_mode,
            network=resolved_network,
            env_values=env_values,
        )
        keys = [key for key in keys if key in TUI_SETTINGS_FIELDS]
        if not keys:
            self.status_message = "Onboarding is already covered for this flow."
            return True
        for key in keys:
            spec = TUI_SETTINGS_FIELDS[key]
            saved = self._edit_env_field(spec, required=True)
            if not saved:
                return False
            resolved_mode, resolved_network = self._resolved_defaults(mode=mode, network=network)
        self.status_message = "Onboarding updated Dexter's .env successfully."
        return True

    def _ensure_intent_ready(
        self,
        intent: str,
        *,
        mode: str,
        network: str,
        skip_database: bool = False,
    ) -> bool:
        while True:
            env_values = read_env_values()
            missing_keys = onboarding_fields_for_intent(intent, mode=mode, network=network, env_values=env_values)
            if skip_database:
                missing_keys = [key for key in missing_keys if key != "DATABASE_URL"]
            if missing_keys and not self._run_onboarding(intent, field_keys=missing_keys, mode=mode, network=network):
                return False
            config, config_error = self._safe_config(mode=mode, network=network)
            if config_error:
                action = self._choose(
                    "Configuration Error",
                    [
                        TuiOption("Open Quick Setup", "configure", "Edit the core runtime, wallet, database, and RPC settings."),
                        TuiOption("Back", "back", "Return without launching this action."),
                    ],
                    intro=[config_error],
                    config=None,
                    config_error=config_error,
                    allow_escape=False,
                )
                if action != "configure":
                    return False
                self._edit_settings_section("Quick Setup", dict(TUI_SETTINGS_SECTIONS)["Quick Setup"])
                continue
            component = {
                "run:trade": "trader",
                "run:collector": "collector",
                "run:analyze": "analyze",
                "create:onchain": "trader",
                "create:seeded": "trader",
                "create:seeded:database": "trader",
            }.get(intent)
            if component is None or config is None:
                return True
            errors, warnings = validate_config(config, component)
            if skip_database:
                errors = [error for error in errors if "DATABASE_URL" not in error and "DB_*" not in error]
            mapped_fields = validation_fields_from_messages(errors, network=network)
            if mapped_fields:
                if not self._run_onboarding(intent, field_keys=mapped_fields, mode=mode, network=network):
                    return False
                continue
            if errors:
                action = self._choose(
                    "Configuration Blocks This Action",
                    [
                        TuiOption("Open Settings", "configure", "Review and adjust Dexter's configuration inside the TUI."),
                        TuiOption("Back", "back", "Return without launching."),
                    ],
                    intro=errors,
                    config=config,
                    allow_escape=False,
                )
                if action != "configure":
                    return False
                self._configure_menu()
                continue
            if warnings:
                self.status_message = warnings[0]
            return True

    def _edit_settings_section(self, title: str, field_keys: tuple[str, ...]) -> None:
        while True:
            env_values = read_env_values()
            options = [
                TuiOption(
                    f"{TUI_SETTINGS_FIELDS[key].label}: {self._display_env_value(TUI_SETTINGS_FIELDS[key], env_values)}",
                    key,
                    TUI_SETTINGS_FIELDS[key].detail,
                )
                for key in field_keys
            ]
            options.append(TuiOption("Back", "back", "Return to the previous menu."))
            choice = self._choose(
                title,
                options,
                intro=["Select a setting to edit. Changes save straight into .env and refresh Dexter immediately."],
                config=self._safe_config()[0],
                config_error=self._safe_config()[1],
            )
            if choice in {None, "back"}:
                return
            self._edit_env_field(TUI_SETTINGS_FIELDS[choice])

    def _configure_menu(self) -> None:
        section_map = dict(TUI_SETTINGS_SECTIONS)
        while True:
            options = [
                TuiOption(name, name, f"{len(keys)} editable settings in this section.")
                for name, keys in TUI_SETTINGS_SECTIONS
            ]
            options.extend(
                [
                    TuiOption("Run onboarding", "onboarding", "Guide Dexter through any missing or placeholder setup values."),
                    TuiOption("Back", "back", "Return to the main menu."),
                ]
            )
            choice = self._choose(
                "Configure Dexter",
                options,
                intro=["Dexter's TUI writes back to .env, reloads config, and keeps the terminal session in sync."],
                config=self._safe_config()[0],
                config_error=self._safe_config()[1],
            )
            if choice in {None, "back"}:
                return
            if choice == "onboarding":
                self._run_onboarding("startup")
                continue
            self._edit_settings_section(choice, section_map[choice])

    def _show_help(self) -> None:
        config, config_error = self._safe_config()
        lines = [
            "Modes",
            "read_only observes only and never opens or closes positions.",
            "paper runs the full strategy loop with simulated positions and local PnL.",
            "simulate signs and simulates transactions when the runtime can.",
            "live sends on devnet, and on mainnet only when both safety gates are explicitly unlocked.",
            "",
            "Controls",
            "Use ↑/↓ or j/k to move, Enter to select, and the mouse to click menu rows.",
            "",
            "Configuration",
            "PRIVATE_KEY is the default signer Dexter uses for create, buy, sell, and balance lookup.",
            "DEXTER_TRADING_PRIVATE_KEY overrides the trading signer when you want a separate key.",
            "DATABASE_URL is Dexter's preferred database setting inside the TUI.",
            "Open positions and recovery history are persisted to the managed positions store in Dexter's state directory.",
        ]
        if config is not None:
            lines.append("")
            lines.append(f"Recovery store: {_managed_positions_path(config)}")
        self._message("Help", lines, button_label="Return")

    def _run_menu(self) -> None:
        base_config, config_error = self._safe_config()
        default_mode, default_network = self._resolved_defaults()
        target = self._choose(
            "Run Dexter",
            [
                TuiOption("Trader", "trade", "Launch the main trader runtime. This can auto-start wsLogs when enabled."),
                TuiOption("Collector", "collector", "Run the wsLogs collector on its own."),
                TuiOption("Analyze", "analyze", "Run the creator analyzer."),
                TuiOption("Back", "back", "Return to the main menu."),
            ],
            intro=["Choose what Dexter should launch."],
            config=base_config,
            config_error=config_error,
            default_value="trade",
        )
        if target in {None, "back"}:
            return
        mode = self._choose(
            "Runtime Mode",
            [TuiOption(item, item) for item in ("read_only", "paper", "simulate", "live")],
            intro=["Pick the execution mode for this launch."],
            config=base_config,
            config_error=config_error,
            default_value=base_config.runtime.mode if base_config is not None else default_mode,
        )
        if mode is None:
            return
        network = self._choose(
            "Network",
            [TuiOption(item, item) for item in ("devnet", "mainnet")],
            intro=["Choose the network for this command."],
            config=base_config,
            config_error=config_error,
            default_value=base_config.runtime.network if base_config is not None else default_network,
        )
        if network is None:
            return
        doctor_default = "yes" if target != "analyze" else "no"
        doctor_choice = self._choose(
            "Run Doctor First?",
            [
                TuiOption("Yes", "yes", "Validate config, database, wallet, and RPC reachability first."),
                TuiOption("No", "no", "Launch directly with the current configuration."),
            ],
            intro=["Dexter can check the environment before starting the runtime."],
            config=base_config,
            config_error=config_error,
            default_value=doctor_default,
        )
        if doctor_choice is None:
            return
        if not self._ensure_intent_ready(f"run:{target}", mode=mode, network=network):
            return
        argv = build_run_argv(target=target, mode=mode, network=network, doctor_first=doctor_choice == "yes")
        self._suspend_for_command(argv, prompt="Command finished with exit code {exit_code}. Press Enter to return to Dexter...")

    def _build_seeded_create_form(self, base_config) -> dict[str, Any] | None:
        mint = self._input_text(
            "Seeded Session Mint",
            detail="Mint address for the existing position Dexter should take over.",
            required=True,
        )
        if mint is None:
            return None
        owner = self._input_text(
            "Seeded Session Owner",
            detail="Creator or owner address associated with this session.",
            required=True,
        )
        if owner is None:
            return None
        bonding_curve = self._input_text(
            "Bonding Curve",
            default="",
            detail="Optional Pump.fun bonding curve address while the token is still on Pump.fun.",
        )
        if bonding_curve is None:
            return None
        name = self._input_text(
            "Display Name",
            default=mint[:8],
            detail="Friendly label Dexter should show for this position.",
            required=True,
        )
        if name is None:
            return None
        market = self._choose(
            "Initial Market",
            [
                TuiOption("pump_fun", "pump_fun", "Start the session on Pump.fun."),
                TuiOption("pump_swap", "pump_swap", "Start the session on PumpSwap."),
            ],
            intro=["Choose the market Dexter should assume for this takeover."],
            config=base_config,
            default_value="pump_fun",
        )
        if market is None:
            return None
        buy_price = self._input_text(
            "Buy Price",
            default="",
            detail="Optional token price at entry so the seeded session can start with a current cost basis.",
        )
        if buy_price is None:
            return None
        token_balance = self._input_text(
            "Token Balance",
            default="0",
            detail="Raw token balance already held for this seeded position.",
            required=True,
            validator=lambda raw: self._validate_field_value(SettingsFieldSpec("token_balance", "", "", kind="int"), raw),
        )
        if token_balance is None:
            return None
        cost_basis = self._input_text(
            "Cost Basis Lamports",
            default="0",
            detail="Optional seeded cost basis used for PnL reporting.",
            required=True,
            validator=lambda raw: self._validate_field_value(SettingsFieldSpec("cost_basis_lamports", "", "", kind="int"), raw),
        )
        if cost_basis is None:
            return None
        trust_level = self._input_text(
            "Trust Level",
            default="1",
            detail="Initial trust level for this seeded session.",
            required=True,
            validator=lambda raw: self._validate_field_value(SettingsFieldSpec("trust_level", "", "", kind="int"), raw),
        )
        if trust_level is None:
            return None
        default_mode, default_network = self._resolved_defaults()
        mode = self._choose(
            "Runtime Mode",
            [TuiOption(item, item) for item in ("read_only", "paper", "simulate", "live")],
            intro=["Choose the runtime Dexter should use after seeding this session."],
            config=base_config,
            default_value=base_config.runtime.mode if base_config is not None else default_mode,
        )
        if mode is None:
            return None
        network = self._choose(
            "Network",
            [TuiOption(item, item) for item in ("devnet", "mainnet")],
            intro=["Choose the network for this seeded session."],
            config=base_config,
            default_value=base_config.runtime.network if base_config is not None else default_network,
        )
        if network is None:
            return None
        launch_choice = self._choose(
            "Launch Dexter Now?",
            [
                TuiOption("Launch now", "launch", "Start Dexter immediately after seeding the session."),
                TuiOption("Dry run only", "dry_run", "Preview the seeded handoff without launching Dexter."),
            ],
            intro=["Choose whether Dexter should start immediately or just print the seeded payload."],
            config=base_config,
            default_value="launch",
        )
        if launch_choice is None:
            return None
        load_database = False
        if launch_choice != "dry_run":
            load_database_choice = self._choose(
                "Database Setup During Handoff",
                [
                    TuiOption("Stay DB-free", "skip", "Recommended when Dexter can hand this seeded session straight into the runtime."),
                    TuiOption("Run database bootstrap", "load", "Only needed if you want Dexter's standard PostgreSQL-backed startup path."),
                ],
                intro=["Seeded sessions do not need database bootstrap unless you explicitly want the standard DB-backed handoff."],
                config=base_config,
                default_value="skip",
            )
            if load_database_choice is None:
                return None
            load_database = load_database_choice == "load"
        return {
            "mint": mint,
            "owner": owner,
            "bonding_curve": bonding_curve,
            "name": name,
            "market": market,
            "buy_price": buy_price,
            "token_balance": token_balance,
            "cost_basis_lamports": cost_basis,
            "trust_level": trust_level,
            "mode": mode,
            "network": network,
            "load_database": load_database,
            "dry_run": launch_choice == "dry_run",
        }

    def _build_onchain_create_form(self, base_config) -> dict[str, Any] | None:
        default_mode, default_network = self._resolved_defaults()
        mode_options = ("paper", "simulate", "live", "read_only")
        form = {
            "name": "",
            "symbol": "",
            "metadata_mode": "uri",
            "uri": "",
            "image": "",
            "description": "",
            "buy_sol": "0.01",
            "mode": (
                base_config.runtime.mode
                if base_config is not None and base_config.runtime.mode in mode_options
                else ("paper" if default_mode == "read_only" else default_mode)
            ),
            "network": base_config.runtime.network if base_config is not None else default_network,
            "tx_action": "simulate" if (base_config.runtime.network if base_config is not None else default_network) == "mainnet" else "send_and_follow",
            "no_follow": False,
            "load_database": False,
        }
        selected_value = "launch"

        def render_value(value: str, *, empty: str = "(unset)") -> str:
            value = str(value or "").strip()
            return value if value else empty

        def image_status(value: str) -> str:
            candidate = str(value or "").strip()
            if not candidate:
                return "(unset)"
            resolved = Path(candidate).expanduser()
            return f"{candidate} [{'exists' if resolved.is_file() else 'missing'}]"

        while True:
            metadata_mode = str(form["metadata_mode"])
            tx_action = str(form["tx_action"])
            should_offer_follow = tx_action == "send_and_follow"
            should_offer_database = should_offer_follow and not bool(form["no_follow"])
            current_config, current_config_error = self._safe_config(
                mode=str(form["mode"]),
                network=str(form["network"]),
            )

            intro = [
                "Review the whole create plan on one screen, edit only the rows you want, then launch.",
                "URI and image upload stay mutually exclusive here, and image rows show live file status.",
            ]
            if not should_offer_database:
                intro.append("Database bootstrap is hidden unless Dexter will actually follow the created position after a live send.")

            options = [
                TuiOption(
                    f"Basics | Network: {form['network']}",
                    "network",
                    "Choose the target cluster for the create flow.",
                ),
                TuiOption(
                    f"Basics | Transaction Action: {tx_action}",
                    "tx_action",
                    "Build only, signed simulation, or live send with optional Dexter handoff.",
                ),
                TuiOption(
                    f"Basics | Runtime Mode After Create: {form['mode']}",
                    "mode",
                    "Dexter runtime mode that follows a successful handoff.",
                ),
                TuiOption(
                    f"Token | Name: {render_value(form['name'])}",
                    "name",
                    "Token name Dexter should use for the Pump.fun launch.",
                ),
                TuiOption(
                    f"Token | Symbol: {render_value(form['symbol'])}",
                    "symbol",
                    "Token symbol Dexter should use for the Pump.fun launch.",
                ),
                TuiOption(
                    f"Metadata | Source: {'Direct URI' if metadata_mode == 'uri' else 'Image Upload'}",
                    "metadata_mode",
                    "Pick either a direct metadata URI or a local image upload that Pump.fun will turn into a URI.",
                ),
            ]

            if metadata_mode == "uri":
                options.append(
                    TuiOption(
                        f"Metadata | URI: {render_value(form['uri'])}",
                        "uri",
                        "Direct metadata URI. This is required when image upload is not selected.",
                    )
                )
            else:
                options.extend(
                    [
                        TuiOption(
                            f"Metadata | Image Path: {image_status(form['image'])}",
                            "image",
                            "Local image file Dexter should upload. Missing files are blocked before launch.",
                        ),
                        TuiOption(
                            f"Metadata | Description: {render_value(form['description'], empty='(optional)')}",
                            "description",
                            "Optional description used only for image-upload metadata.",
                        ),
                    ]
                )

            options.append(
                TuiOption(
                    f"Execution | Bundled Buy SOL: {render_value(form['buy_sol'])}",
                    "buy_sol",
                    "Auto-buy amount in SOL for the create flow.",
                )
            )

            if should_offer_follow:
                options.append(
                    TuiOption(
                        f"Handoff | Follow Created Position: {'no' if form['no_follow'] else 'yes'}",
                        "no_follow",
                        "Live send can either hand the created position into Dexter or stop after the transaction.",
                    )
                )
            if should_offer_database:
                options.append(
                    TuiOption(
                        f"Handoff | Database Setup: {'standard bootstrap' if form['load_database'] else 'DB-free'}",
                        "load_database",
                        "DB-free is the lighter handoff. Standard bootstrap is only needed for the full PostgreSQL-backed path.",
                    )
                )

            options.extend(
                [
                    TuiOption("Launch Create Flow", "launch", "Run the create command with the values shown above."),
                    TuiOption("Back", "back", "Return to the previous menu."),
                ]
            )

            choice = self._choose(
                "Create And Buy A New Token",
                options,
                intro=intro,
                config=current_config,
                config_error=current_config_error,
                default_value=selected_value,
            )
            if choice in {None, "back"}:
                return None
            selected_value = choice

            if choice == "network":
                network = self._choose(
                    "Network",
                    [TuiOption(item, item) for item in ("devnet", "mainnet")],
                    intro=["Choose the network for the create flow."],
                    config=current_config,
                    config_error=current_config_error,
                    default_value=str(form["network"]),
                )
                if network is not None:
                    form["network"] = network
                    if network == "mainnet" and form["tx_action"] == "send_and_follow":
                        form["tx_action"] = "simulate"
                        form["no_follow"] = False
                        form["load_database"] = False
                continue

            if choice == "tx_action":
                tx_action = self._choose(
                    "Create Transaction Action",
                    [
                        TuiOption("build_only", "build_only", "Build the signed create transaction only."),
                        TuiOption("simulate", "simulate", "Build and simulate the signed create transaction."),
                        TuiOption("send_and_follow", "send_and_follow", "Send the create flow and optionally hand the position into Dexter."),
                    ],
                    intro=["Choose whether Dexter should build, simulate, or send this create flow."],
                    config=current_config,
                    config_error=current_config_error,
                    default_value=str(form["tx_action"]),
                )
                if tx_action is not None:
                    form["tx_action"] = tx_action
                    if tx_action != "send_and_follow":
                        form["no_follow"] = False
                        form["load_database"] = False
                continue

            if choice == "mode":
                chosen_mode = self._choose(
                    "Runtime Mode After Create",
                    [TuiOption(item, item) for item in mode_options],
                    intro=["Choose Dexter's runtime mode after the create flow."],
                    config=current_config,
                    config_error=current_config_error,
                    default_value=str(form["mode"]),
                )
                if chosen_mode is not None:
                    form["mode"] = chosen_mode
                continue

            if choice == "metadata_mode":
                metadata_mode = self._choose(
                    "Metadata Source",
                    [
                        TuiOption("Direct URI", "uri", "Use a ready-made metadata URI."),
                        TuiOption("Image Upload", "image", "Upload a local image and let Pump.fun create the metadata URI."),
                    ],
                    intro=["Mamba-style rule: direct URI and image upload stay mutually exclusive."],
                    config=current_config,
                    config_error=current_config_error,
                    default_value=str(form["metadata_mode"]),
                )
                if metadata_mode is not None:
                    form["metadata_mode"] = metadata_mode
                    if metadata_mode == "uri":
                        form["image"] = ""
                        form["description"] = ""
                    else:
                        form["uri"] = ""
                continue

            if choice == "name":
                updated = self._input_text(
                    "Token Name",
                    default=str(form["name"]),
                    detail="Name Dexter should use for the Pump.fun launch.",
                    required=True,
                )
                if updated is not None:
                    form["name"] = updated
                continue

            if choice == "symbol":
                updated = self._input_text(
                    "Token Symbol",
                    default=str(form["symbol"]),
                    detail="Symbol Dexter should use for the Pump.fun launch.",
                    required=True,
                )
                if updated is not None:
                    form["symbol"] = updated
                continue

            if choice == "uri":
                updated = self._input_text(
                    "Metadata URI",
                    default=str(form["uri"]),
                    detail="Direct metadata URI used instead of an image upload.",
                    required=True,
                )
                if updated is not None:
                    form["uri"] = updated
                continue

            if choice == "image":
                updated = self._input_text(
                    "Image Path",
                    default=str(form["image"]),
                    detail="Local image file Dexter should upload to Pump.fun when using image metadata.",
                    required=True,
                )
                if updated is not None:
                    form["image"] = updated
                continue

            if choice == "description":
                updated = self._input_text(
                    "Description",
                    default=str(form["description"]),
                    detail="Optional description used only with an image upload.",
                )
                if updated is not None:
                    form["description"] = updated
                continue

            if choice == "buy_sol":
                updated = self._input_text(
                    "Bundled Buy SOL",
                    default=str(form["buy_sol"]),
                    detail="Auto-buy amount in SOL for the create flow.",
                    required=True,
                    validator=lambda raw: self._validate_field_value(SettingsFieldSpec("buy_sol", "", "", kind="decimal"), raw),
                )
                if updated is not None:
                    form["buy_sol"] = updated
                continue

            if choice == "no_follow":
                follow_choice = self._choose(
                    "Follow Created Position?",
                    [
                        TuiOption("Follow in Dexter", "follow", "Hand the created position directly into Dexter."),
                        TuiOption("Do not follow", "no_follow", "Send the transaction but stop before the trader handoff."),
                    ],
                    intro=["Only live send-and-follow mode can hand the position straight into Dexter."],
                    config=current_config,
                    config_error=current_config_error,
                    default_value="no_follow" if bool(form["no_follow"]) else "follow",
                )
                if follow_choice is not None:
                    form["no_follow"] = follow_choice == "no_follow"
                    if form["no_follow"]:
                        form["load_database"] = False
                continue

            if choice == "load_database":
                load_database_choice = self._choose(
                    "Database Setup During Handoff",
                    [
                        TuiOption("Stay DB-free", "skip", "Recommended when Dexter can hand the new position straight into the runtime."),
                        TuiOption("Run database bootstrap", "load", "Only needed if you want Dexter's standard PostgreSQL-backed startup path."),
                    ],
                    intro=["This only matters for a live send that Dexter will follow automatically."],
                    config=current_config,
                    config_error=current_config_error,
                    default_value="load" if bool(form["load_database"]) else "skip",
                )
                if load_database_choice is not None:
                    form["load_database"] = load_database_choice == "load"
                continue

            if choice == "launch":
                errors: list[str] = []
                if not str(form["name"]).strip():
                    errors.append("Token name is required.")
                if not str(form["symbol"]).strip():
                    errors.append("Token symbol is required.")
                if str(form["metadata_mode"]) == "uri":
                    if not str(form["uri"]).strip():
                        errors.append("Metadata URI is required when direct URI mode is selected.")
                else:
                    image_path = Path(str(form["image"]).strip()).expanduser()
                    if not str(form["image"]).strip():
                        errors.append("Image path is required when image upload mode is selected.")
                    elif not image_path.is_file():
                        errors.append(f"Image path was not found: {image_path}")
                try:
                    if Decimal(str(form["buy_sol"])) <= 0:
                        errors.append("Bundled buy SOL must be greater than 0.")
                except Exception:
                    errors.append("Bundled buy SOL must be a valid decimal number.")
                if str(form["network"]) == "mainnet" and str(form["tx_action"]) == "send_and_follow":
                    errors.append("Mainnet create sends are disabled here. Choose build_only or simulate instead.")
                if errors:
                    self._message("Create Form Needs Attention", errors)
                    continue
                return {
                    "name": str(form["name"]).strip(),
                    "symbol": str(form["symbol"]).strip(),
                    "uri": str(form["uri"]).strip() if str(form["metadata_mode"]) == "uri" else "",
                    "image": str(form["image"]).strip() if str(form["metadata_mode"]) == "image" else "",
                    "description": str(form["description"]).strip() if str(form["metadata_mode"]) == "image" else "",
                    "buy_sol": str(form["buy_sol"]).strip(),
                    "mode": str(form["mode"]),
                    "network": str(form["network"]),
                    "tx_action": str(form["tx_action"]),
                    "no_follow": bool(form["no_follow"]),
                    "load_database": bool(form["load_database"]) if should_offer_database else False,
                }

    def _create_menu(self) -> None:
        base_config, config_error = self._safe_config()
        flow = self._choose(
            "Create",
            [
                TuiOption("Create and buy a new token", "onchain", "Launch a new Pump.fun token or simulate the full create path."),
                TuiOption("Take over an existing token or position", "seeded", "Seed a session into Dexter for an existing mint."),
                TuiOption("Back", "back", "Return to the main menu."),
            ],
            intro=["Choose the create flow Dexter should guide you through."],
            config=base_config,
            config_error=config_error,
        )
        if flow in {None, "back"}:
            return
        if flow == "seeded":
            form = self._build_seeded_create_form(base_config)
            if form is None:
                return
            if not bool(form["dry_run"]):
                intent = "create:seeded:database" if bool(form["load_database"]) else "create:seeded"
                if not self._ensure_intent_ready(
                    intent,
                    mode=str(form["mode"]),
                    network=str(form["network"]),
                    skip_database=not bool(form["load_database"]),
                ):
                    return
            argv = build_seeded_create_argv(form)
        else:
            form = self._build_onchain_create_form(base_config)
            if form is None:
                return
            skip_database = (
                str(form.get("tx_action") or "") != "send_and_follow"
                or bool(form.get("no_follow"))
                or not bool(form.get("load_database"))
            )
            if not self._ensure_intent_ready(
                "create:onchain",
                mode=str(form["mode"]),
                network=str(form["network"]),
                skip_database=skip_database,
            ):
                return
            argv = build_onchain_create_argv(form)
        self._suspend_for_command(argv, prompt="Create flow finished with exit code {exit_code}. Press Enter to return to Dexter...")

    def _manage_position_label(self, item: dict[str, Any]) -> tuple[str, str]:
        pnl_lamports = _position_unrealized_profit_lamports(item)
        pnl_display = "n/a"
        if pnl_lamports is not None:
            pnl_display = f"{Decimal(int(pnl_lamports)) / Decimal('1000000000'):+.6f} SOL"
        label = str(item.get("mint_id") or "(unknown mint)")
        detail = (
            f"status={item.get('status')}  market={item.get('market')}  "
            f"balance={int(item.get('token_balance', 0) or 0)}  "
            f"buy={item.get('buy_price') or '-'}  "
            f"last={item.get('current_price') or item.get('last_price') or '-'}  pnl={pnl_display}"
        )
        return label, detail

    def _show_closed_history(self, config, closed_positions: list[dict[str, Any]]) -> None:
        lines = ["Recently closed positions"] + [
            f"{self._manage_position_label(item)[0]}  {self._manage_position_label(item)[1]}"
            for item in closed_positions[:10]
        ]
        if len(lines) == 1:
            lines.append("No closed positions found for this network.")
        self._message("Closed History", lines)

    def _manage_menu(self) -> None:
        base_config, config_error = self._safe_config()
        mode_options = ("paper", "simulate", "live", "read_only")
        mode = self._choose(
            "Manage Runtime Mode",
            [TuiOption(item, item) for item in mode_options],
            intro=["Choose the mode Dexter should use when managing tracked positions."],
            config=base_config,
            config_error=config_error,
            default_value="paper" if base_config is None or base_config.runtime.mode == "read_only" else base_config.runtime.mode,
        )
        if mode is None:
            return
        network = self._choose(
            "Manage Network",
            [TuiOption(item, item) for item in ("devnet", "mainnet")],
            intro=["Choose which network's tracked positions you want to inspect or liquidate."],
            config=base_config,
            config_error=config_error,
            default_value=base_config.runtime.network if base_config is not None else "devnet",
        )
        if network is None:
            return
        while True:
            config, manage_error = self._safe_config(mode=mode, network=network)
            if config is None:
                self._message("Manage Configuration Error", [manage_error or "Unable to load config for manage mode."])
                return
            open_positions = [
                item
                for item in list_managed_positions(config, status="open")
                if item.get("network") in (None, config.runtime.network)
            ]
            closed_positions = [
                item
                for item in list_managed_positions(config, status="closed")
                if item.get("network") in (None, config.runtime.network)
            ]
            options: list[TuiOption] = []
            for item in open_positions[:12]:
                label, detail = self._manage_position_label(item)
                options.append(TuiOption(f"Sell {label}", f"sell:{label}", detail))
            options.extend(
                [
                    TuiOption("Sell all open positions", "sell_all", "Queue a manual exit for every open tracked position on this network."),
                    TuiOption("Show closed history", "closed", "Review the most recent closed positions."),
                    TuiOption("Refresh", "refresh", "Reload the recovery store and redraw this screen."),
                    TuiOption("Back", "back", "Return to the main menu."),
                ]
            )
            intro = [
                f"Open positions: {len(open_positions)}",
                "Select an open position with > or click it to sell. The numbered text prompt is gone.",
            ]
            choice = self._choose(
                "Manage Positions",
                options,
                intro=intro,
                config=config,
                config_error=manage_error,
                default_value=options[0].value if options else "back",
            )
            if choice in {None, "back"}:
                return
            if choice == "refresh":
                continue
            if choice == "closed":
                self._show_closed_history(config, closed_positions)
                continue
            if choice == "sell_all":
                if not open_positions:
                    self._message("Nothing To Sell", ["There are no open positions to liquidate on this network."])
                    continue
                confirmed = self._confirm(
                    "Sell All Open Positions?",
                    "Dexter will queue a manual sell for every open tracked position listed here.",
                    default_yes=False,
                )
                if confirmed is not True:
                    continue
                if not self._ensure_intent_ready("manage:sell_all", mode=mode, network=network):
                    continue
                argv = build_manage_argv(mode=mode, network=network, sell_all=True)
                self._suspend_for_command(argv, prompt="Manage command finished with exit code {exit_code}. Press Enter to return to Dexter...")
                continue
            if choice.startswith("sell:"):
                mint_id = choice.split(":", 1)[1]
                confirmed = self._confirm(
                    "Sell Selected Position?",
                    f"Dexter will queue a manual sell for {mint_id}.",
                    default_yes=False,
                )
                if confirmed is not True:
                    continue
                if not self._ensure_intent_ready("manage:sell", mode=mode, network=network):
                    continue
                argv = build_manage_argv(mode=mode, network=network, sell_mint=mint_id)
                self._suspend_for_command(argv, prompt="Manage command finished with exit code {exit_code}. Press Enter to return to Dexter...")

    def _startup_prompt(self) -> bool:
        mode, network = self._resolved_defaults()
        fields = onboarding_fields_for_intent("startup", mode=mode, network=network, env_values=read_env_values())
        if not fields:
            return True
        choice = self._choose(
            "Welcome To Dexter",
            [
                TuiOption("Start onboarding", "onboarding", "Dexter will ask for the missing or placeholder setup values now."),
                TuiOption("Open the main menu", "menu", "Skip onboarding for now and go straight into the TUI."),
                TuiOption("Exit", "exit", "Close Dexter."),
            ],
            intro=["Dexter noticed that some core setup values still need attention, like wallet or database settings."],
            config=self._safe_config()[0],
            config_error=self._safe_config()[1],
            allow_escape=False,
        )
        if choice == "exit":
            return False
        if choice == "onboarding":
            return bool(self._run_onboarding("startup", field_keys=fields, mode=mode, network=network))
        return True

    def _main_loop(self) -> int:
        if not self._startup_prompt():
            return 0
        while True:
            config, config_error = self._safe_config()
            startup_fields = onboarding_fields_for_intent(
                "startup",
                mode=self._resolved_defaults()[0],
                network=self._resolved_defaults()[1],
                env_values=read_env_values(),
            )
            intro = ["Arrow keys and mouse clicks now drive Dexter's menu"]
            if startup_fields:
                intro.append("Setup is still incomplete. Open onboarding or configuration any time from here.")
            options = []
            if startup_fields:
                options.append(TuiOption("Onboarding", "onboarding", "Ask Dexter for the missing setup it still needs."))
            options.extend(
                [
                    TuiOption("Run", "run", "Launch trader, collector, or analyzer through Dexter's guided runtime flow."),
                    TuiOption("Create", "create", "Launch a new token or seed an existing position into Dexter."),
                    TuiOption("Manage", "manage", "Inspect and liquidate tracked positions from the recovery store."),
                    TuiOption("Configure", "configure", "Edit .env-backed settings from inside the TUI."),
                    TuiOption("Help", "help", "Review modes, controls, and key runtime knobs."),
                    TuiOption("Exit", "exit", "Close Dexter."),
                ]
            )
            choice = self._choose(
                "Main Menu",
                options,
                intro=intro,
                config=config,
                config_error=config_error,
            )
            if choice in {None, "exit"}:
                return 0
            if choice == "onboarding":
                self._run_onboarding("startup")
                continue
            if choice == "run":
                self._run_menu()
                continue
            if choice == "create":
                self._create_menu()
                continue
            if choice == "manage":
                self._manage_menu()
                continue
            if choice == "configure":
                self._configure_menu()
                continue
            self._show_help()

    def run(self) -> int:
        tui_ready, tui_message = _tui_available()
        if not tui_ready:
            if tui_message:
                print(tui_message)
                print("The command-line interface is still available with `dexter help`.")
            return 1

        def _wrapped(stdscr) -> int:
            self._init_screen(stdscr)
            self._show_splash_tui()
            return self._main_loop()

        try:
            return int(curses.wrapper(_wrapped))
        except KeyboardInterrupt:
            return 0
        finally:
            self._shutdown_background_services()


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if args and args[0] == "cli":
        import dexter_cli as dexter_commands

        cli_args = args[1:]
        if not cli_args:
            parser = dexter_commands.build_parser()
            parser.print_help()
            return 0
        return int(dexter_commands.legacy_main(cli_args))

    if not args or (args and args[0] in {"menu", "interactive"}):
        if not _interactive_terminal():
            import dexter_cli as dexter_commands

            parser = dexter_commands.build_parser()
            parser.print_help()
            return 1
        tui_ready, tui_message = _tui_available()
        if not tui_ready:
            if tui_message:
                print(tui_message)
            import dexter_cli as dexter_commands

            parser = dexter_commands.build_parser()
            parser.print_help()
            return 1
        app = InteractiveDexterCLI()
        return int(app.run())

    import dexter_cli as dexter_commands

    return int(dexter_commands.legacy_main(args))


if __name__ == '__main__':
    raise SystemExit(main())
