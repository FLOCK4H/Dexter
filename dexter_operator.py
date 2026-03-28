from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time
from typing import Any
from uuid import uuid4


logger = logging.getLogger(__name__)

_WINDOWS_FILE_ACCESS_RETRY_DELAYS = (0.05, 0.1, 0.2, 0.5)
_WINDOWS_FILE_ACCESS_WINERRORS = {5, 32}
_RUNTIME_SNAPSHOT_WARNING_INTERVAL_SECONDS = 30.0
_path_write_locks: dict[str, threading.Lock] = {}
_path_write_locks_guard = threading.Lock()
_runtime_snapshot_warning_times: dict[str, float] = {}
_runtime_snapshot_warning_guard = threading.Lock()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _read_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _path_lock_key(path: Path) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


def _get_path_write_lock(path: Path) -> threading.Lock:
    key = _path_lock_key(path)
    with _path_write_locks_guard:
        lock = _path_write_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _path_write_locks[key] = lock
        return lock


def _is_retryable_windows_file_access_error(exc: BaseException) -> bool:
    return (
        os.name == "nt"
        and isinstance(exc, OSError)
        and getattr(exc, "winerror", None) in _WINDOWS_FILE_ACCESS_WINERRORS
    )


def _replace_with_retry(temp_path: Path, path: Path) -> None:
    for delay in _WINDOWS_FILE_ACCESS_RETRY_DELAYS:
        try:
            temp_path.replace(path)
            return
        except OSError as exc:
            if not _is_retryable_windows_file_access_error(exc):
                raise
            time.sleep(delay)
    temp_path.replace(path)


def _should_log_runtime_snapshot_warning(path: Path) -> bool:
    key = _path_lock_key(path)
    now = time.monotonic()
    with _runtime_snapshot_warning_guard:
        last_logged_at = _runtime_snapshot_warning_times.get(key)
        if last_logged_at is not None and (now - last_logged_at) < _RUNTIME_SNAPSHOT_WARNING_INTERVAL_SECONDS:
            return False
        _runtime_snapshot_warning_times[key] = now
        return True


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.{uuid4().hex}.tmp")
    rendered = json.dumps(_json_safe(payload), indent=2, sort_keys=True)
    with _get_path_write_lock(path):
        try:
            temp_path.write_text(rendered, encoding="utf-8")
            _replace_with_retry(temp_path, path)
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass


class RuntimeStateWriter:
    def __init__(self, path: Path, component: str):
        self.path = path
        self.component = component

    def write(self, payload: dict[str, Any]) -> None:
        state = {
            "component": self.component,
            **payload,
        }
        _atomic_write_json(self.path, state)


class OperatorCommandBus:
    def __init__(self, path: Path):
        self.path = path
        self._offset = 0

    def append(self, command: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_json_safe(command), sort_keys=True) + "\n")

    def read_pending(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        commands: list[dict[str, Any]] = []
        with self.path.open("r", encoding="utf-8") as handle:
            handle.seek(self._offset)
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    commands.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
            self._offset = handle.tell()
        return commands


def maybe_desktop_notify(title: str, message: str) -> bool:
    if not shutil.which("notify-send"):
        return False
    try:
        subprocess.run(["notify-send", title, message], check=False)
        return True
    except Exception:
        return False


def _default_control_state() -> dict[str, Any]:
    return {
        "pause_new_entries": False,
        "blacklist_creators": [],
        "whitelist_creators": [],
        "watchlist_mints": [],
        "force_sell_mints": [],
    }


def load_control_state(path: Path) -> dict[str, Any]:
    payload = _read_json_file(path, _default_control_state())
    state = _default_control_state()
    if isinstance(payload, dict):
        state.update(payload)
    return state


def save_control_state(path: Path, state: dict[str, Any]) -> dict[str, Any]:
    merged = _default_control_state()
    merged.update(state or {})
    _atomic_write_json(path, merged)
    return merged


def publish_runtime_snapshot(path: Path, snapshot: dict[str, Any]) -> None:
    try:
        _atomic_write_json(path, snapshot)
    except OSError as exc:
        if _is_retryable_windows_file_access_error(exc):
            if _should_log_runtime_snapshot_warning(path):
                logger.warning(
                    "Skipping runtime snapshot update for %s because the file is temporarily locked: %s",
                    path,
                    exc,
                )
            return
        raise


def _line_set(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def update_line_file(path: Path, value: str, *, present: bool) -> list[str]:
    lines = _line_set(path)
    normalized = value.strip()
    if normalized:
        if present:
            lines.add(normalized)
        else:
            lines.discard(normalized)
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = sorted(lines)
    path.write_text(("".join(f"{line}\n" for line in rendered)) if rendered else "", encoding="utf-8")
    return rendered


class AlertDispatcher:
    def __init__(self, config):
        self.config = config

    async def send(self, title: str, lines: list[str]) -> None:
        message = f"{title}\n" + "\n".join(lines)
        tasks = []
        if self.config.alerts.discord_webhook_url:
            tasks.append(self._send_discord(message))
        if self.config.alerts.telegram_bot_token and self.config.alerts.telegram_chat_id:
            tasks.append(self._send_telegram(message))
        if self.config.alerts.desktop_notifications:
            tasks.append(self._send_desktop(title, "\n".join(lines)))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_discord(self, message: str) -> None:
        import aiohttp

        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await session.post(self.config.alerts.discord_webhook_url, json={"content": message})

    async def _send_telegram(self, message: str) -> None:
        import aiohttp

        url = f"https://api.telegram.org/bot{self.config.alerts.telegram_bot_token}/sendMessage"
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await session.post(
                url,
                json={"chat_id": self.config.alerts.telegram_chat_id, "text": message},
            )

    async def _send_desktop(self, title: str, message: str) -> None:
        if not shutil.which("notify-send"):
            return
        process = await asyncio.create_subprocess_exec("notify-send", title, message)
        await process.communicate()


async def _db_health(config) -> dict[str, Any]:
    try:
        import asyncpg
    except ModuleNotFoundError:
        return {"status": "warn", "detail": "asyncpg is not installed."}
    if not config.database.dsn:
        return {"status": "fail", "detail": "DATABASE_URL is not configured."}
    conn = None
    try:
        conn = await asyncpg.connect(config.database.dsn, timeout=5)
        await conn.execute("SELECT 1;")
        return {"status": "pass", "detail": "reachable"}
    except Exception as exc:
        return {"status": "fail", "detail": str(exc)}
    finally:
        if conn is not None:
            await conn.close()


async def build_dashboard_snapshot(config, *, limit: int = 5) -> dict[str, Any]:
    from dexter_phase2 import Phase2Store

    trader_state = _read_json_file(config.paths.trader_snapshot_file, {})
    collector_state = _read_json_file(config.paths.collector_snapshot_file, {})
    db_health = await _db_health(config)
    phase2_snapshot: dict[str, Any] = {
        "leaderboard_version": None,
        "top_creators": [],
        "active_sessions": [],
        "recent_positions": [],
        "recent_risk_events": [],
        "recent_fills": [],
    }
    if config.database.dsn:
        try:
            store = Phase2Store(config.database.dsn, config=config)
            await store.ensure_schema()
            phase2_snapshot = await store.fetch_operator_snapshot(limit=limit)
        except Exception as exc:
            phase2_snapshot = {"status": "fail", "detail": str(exc)}

    watchlist_file = config.project_root / "watchlist.txt"
    blacklist_file = config.project_root / "blacklist.txt"
    return {
        "runtime": {
            "network": config.runtime.network,
            "mode": config.runtime.mode,
            "strategy_profile": config.strategy.default_profile,
        },
        "collector": collector_state,
        "trader": trader_state,
        "database": db_health,
        "phase2": phase2_snapshot,
        "watchlist": sorted(_line_set(watchlist_file)),
        "blacklist": sorted(_line_set(blacklist_file)),
        "paths": {
            "trader_snapshot": str(config.paths.trader_snapshot_file),
            "collector_snapshot": str(config.paths.collector_snapshot_file),
            "operator_control": str(config.paths.operator_control_file),
        },
    }


def render_dashboard(snapshot: dict[str, Any]) -> str:
    runtime = snapshot.get("runtime", {})
    collector = snapshot.get("collector", {})
    trader = snapshot.get("trader", {})
    database = snapshot.get("database", {})
    phase2 = snapshot.get("phase2", {})

    def _section(title: str, lines: list[str]) -> str:
        border = "=" * max(24, len(title) + 6)
        return "\n".join([border, title, *lines])

    top_creators = phase2.get("top_creators", []) or []
    recent_positions = phase2.get("recent_positions", []) or []
    recent_risk_events = phase2.get("recent_risk_events", []) or []
    active_holdings = trader.get("holdings_preview", []) or []
    recent_fills = phase2.get("recent_fills", []) or []

    def _preview(items: list[Any], formatter) -> list[str]:
        if not items:
            return ["none"]
        return [formatter(item) for item in items[:5]]

    sections = [
        _section(
            "Dexter Dashboard",
            [
                f"network={runtime.get('network')} mode={runtime.get('mode')} strategy={runtime.get('strategy_profile')}",
                f"db_status={database.get('status')} db_detail={database.get('detail')}",
            ],
        ),
        _section(
            "Collector Health",
            [
                f"status={collector.get('status', 'unknown')} subscribed={collector.get('subscribed', False)}",
                f"processed_logs={collector.get('processed_logs', 0)} last_event_at={collector.get('last_event_at')}",
            ],
        ),
        _section(
            "Trader Health",
            [
                f"status={trader.get('status', 'unknown')} wallet={trader.get('wallet')}",
                f"active_sessions={trader.get('active_session_count', 0)} holdings={len(active_holdings)} pending={trader.get('pending_positions', 0)}",
                f"daily_pnl_lamports={trader.get('daily_realized_pnl_lamports', 0)} reserved={trader.get('reserved_lamports', 0)}",
            ],
            ),
        _section(
            "Holdings",
            _preview(
                active_holdings,
                lambda item: f"{item.get('mint_id')} owner={item.get('owner')} balance={item.get('token_balance')} market={item.get('market')}",
            ),
        ),
        _section(
            "Recent Positions",
            _preview(
                recent_positions,
                lambda item: f"{item.get('mint_id')} status={item.get('status')} pnl={item.get('realized_profit_lamports')} exit={item.get('exit_reason')}",
            ),
        ),
        _section(
            "Top Creators",
            _preview(
                top_creators,
                lambda item: f"#{item.get('rank')} {item.get('creator')} perf={item.get('performance_score')} trust={item.get('trust_factor')}",
            ),
        ),
        _section(
            "Recent Fills",
            _preview(
                recent_fills,
                lambda item: f"{item.get('side')} {item.get('mint_id')} tx={item.get('tx_id')} at={item.get('filled_at')}",
            ),
        ),
        _section(
            "Risk Events",
            _preview(
                recent_risk_events,
                lambda item: f"{item.get('severity')} {item.get('event_type')} mint={item.get('mint_id')} detail={item.get('detail')}",
            ),
        ),
        _section(
            "Lists",
            [
                f"watchlist={', '.join(snapshot.get('watchlist', [])[:8]) or 'none'}",
                f"blacklist={', '.join(snapshot.get('blacklist', [])[:8]) or 'none'}",
            ],
        ),
    ]
    return "\n\n".join(sections)


async def run_dashboard(config, *, watch: bool = False, interval: float = 2.0, as_json: bool = False, limit: int = 5) -> int:
    while True:
        snapshot = await build_dashboard_snapshot(config, limit=limit)
        output = json.dumps(_json_safe(snapshot), indent=2, sort_keys=True) if as_json else render_dashboard(snapshot)
        if watch:
            if os.name == "nt":
                os.system("cls")
            else:
                print("\033[2J\033[H", end="")
        print(output)
        if not watch:
            return 0
        await asyncio.sleep(max(0.5, interval))


def pause_entries(config) -> None:
    state = load_control_state(config.paths.operator_control_file)
    state["pause_new_entries"] = True
    save_control_state(config.paths.operator_control_file, state)
    config.runtime.emergency_stop_file.parent.mkdir(parents=True, exist_ok=True)
    config.runtime.emergency_stop_file.touch(exist_ok=True)


def resume_entries(config) -> None:
    state = load_control_state(config.paths.operator_control_file)
    state["pause_new_entries"] = False
    save_control_state(config.paths.operator_control_file, state)
    if config.runtime.emergency_stop_file.exists():
        config.runtime.emergency_stop_file.unlink()


def blacklist_owner(config, owner: str) -> list[str]:
    entries = update_line_file(config.project_root / "blacklist.txt", owner, present=True)
    state = load_control_state(config.paths.operator_control_file)
    state["blacklist_creators"] = sorted(set(state.get("blacklist_creators", []) or []) | {owner})
    save_control_state(config.paths.operator_control_file, state)
    return entries


def whitelist_owner(config, owner: str) -> list[str]:
    entries = update_line_file(config.project_root / "blacklist.txt", owner, present=False)
    state = load_control_state(config.paths.operator_control_file)
    blacklist = set(state.get("blacklist_creators", []) or [])
    blacklist.discard(owner)
    state["blacklist_creators"] = sorted(blacklist)
    whitelist = set(state.get("whitelist_creators", []) or [])
    whitelist.add(owner)
    state["whitelist_creators"] = sorted(whitelist)
    save_control_state(config.paths.operator_control_file, state)
    return entries


def add_watchlist_mint(config, mint: str) -> list[str]:
    entries = update_line_file(config.project_root / "watchlist.txt", mint, present=True)
    state = load_control_state(config.paths.operator_control_file)
    state["watchlist_mints"] = sorted(set(state.get("watchlist_mints", []) or []) | {mint})
    save_control_state(config.paths.operator_control_file, state)
    return entries


def remove_watchlist_mint(config, mint: str) -> list[str]:
    entries = update_line_file(config.project_root / "watchlist.txt", mint, present=False)
    state = load_control_state(config.paths.operator_control_file)
    watchlist = set(state.get("watchlist_mints", []) or [])
    watchlist.discard(mint)
    state["watchlist_mints"] = sorted(watchlist)
    save_control_state(config.paths.operator_control_file, state)
    return entries


def queue_force_sell(config, mint: str, *, reason: str) -> None:
    state = load_control_state(config.paths.operator_control_file)
    force_sell = set(state.get("force_sell_mints", []) or [])
    force_sell.add(mint)
    state["force_sell_mints"] = sorted(force_sell)
    state["last_force_sell_reason"] = reason
    save_control_state(config.paths.operator_control_file, state)
