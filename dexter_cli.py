from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
import json
import logging
import os
from pathlib import Path
import shutil
import sys
import tempfile

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

PROCESS_ENV_DEXTER_NETWORK = os.environ.get("DEXTER_NETWORK")

from dexter_config import (
    AppConfig,
    current_utc_timestamp,
    ensure_directories,
    load_config,
    redact_url,
    resolve_trade_execution_mode,
    validate_config,
)
from dexter_data_store import Phase2Store, run_export, run_replay
from dexter_local_postgres import ensure_local_postgres_running
from dexter_operator import (
    add_watchlist_mint,
    blacklist_owner,
    pause_entries,
    queue_force_sell,
    remove_watchlist_mint,
    resume_entries,
    run_dashboard,
    whitelist_owner,
)
from dexter_strategy import backtest_records, get_strategy_profile, load_records_from_json, render_backtest_report
from settings import PRICE_STEP_UNITS

try:
    from DexLab.colors import cc
except Exception:
    class cc:  # type: ignore[no-redef]
        RESET = ""
        CYAN = ""
        LIGHT_CYAN = ""
        LIGHT_GREEN = ""
        LIGHT_RED = ""
        YELLOW = ""
        LIGHT_WHITE = ""


@dataclass(frozen=True)
class CheckResult:
    title: str
    status: str
    detail: str


def _format_seed_number(value: float | None) -> str | None:
    if value is None:
        return None
    rendered = f"{value:.12f}".rstrip("0").rstrip(".")
    return rendered or "0"


def _print_block(title: str, lines: list[str]) -> None:
    border = "-" * max(36, len(title) + 4)
    print(f"{cc.LIGHT_CYAN}{border}{cc.RESET}")
    print(f"{cc.CYAN}{title}{cc.RESET}")
    for line in lines:
        print(line)
    print(f"{cc.LIGHT_CYAN}{border}{cc.RESET}")


def _render_runtime_summary(config: AppConfig, *, heading: str, intent: str) -> None:
    _print_block(
        heading,
        [
            f"intent         {intent}",
            f"runtime_mode   {config.runtime.mode}",
            f"network        {config.runtime.network}",
            f"http_rpc       {redact_url(config.rpc.http_url)}",
            f"ws_rpc         {redact_url(config.rpc.ws_url)}",
            f"log_dir        {config.paths.log_dir}",
        ],
    )


def _resolve_cli_network(args: argparse.Namespace) -> tuple[str, str | None]:
    requested = getattr(args, "network", None)
    if requested:
        return requested, None

    env_network = (PROCESS_ENV_DEXTER_NETWORK or "").strip().lower()
    if env_network in {"mainnet", "devnet"}:
        return env_network, None

    return (
        "devnet",
        "DEXTER_NETWORK is unset; defaulting this command to devnet. Pass --network mainnet to inspect mainnet under the existing safety gates.",
    )


def _load_cli_config(args: argparse.Namespace) -> tuple[AppConfig, str | None]:
    network, warning = _resolve_cli_network(args)
    return load_config(args.mode, network_override=network), warning


def _get_subcommand_parser(parser: argparse.ArgumentParser, name: str) -> argparse.ArgumentParser | None:
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        return action.choices.get(name)
    return None


def _merge_issues(*issue_sets: tuple[list[str], list[str]]) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    for err_list, warn_list in issue_sets:
        errors.extend(err_list)
        warnings.extend(warn_list)
    return sorted(set(errors)), sorted(set(warnings))


def _status_symbol(status: str) -> str:
    return {
        "pass": "PASS",
        "warn": "WARN",
        "fail": "FAIL",
        "skip": "SKIP",
    }.get(status, status.upper())


def _check_env(config: AppConfig, component: str) -> CheckResult:
    if component == "all":
        errors, warnings = _merge_issues(
            validate_config(config, "collector"),
            validate_config(config, "trader"),
        )
    else:
        errors, warnings = validate_config(config, component if component != "all" else "doctor")

    if errors:
        return CheckResult("Environment", "fail", "; ".join(errors))
    if warnings:
        return CheckResult("Environment", "warn", "; ".join(warnings))
    return CheckResult("Environment", "pass", "Required variables and safety gates look valid.")


async def _check_database(config: AppConfig) -> CheckResult:
    if not config.database.dsn:
        return CheckResult("Database", "fail", "DATABASE_URL or DB_* variables are not configured.")

    try:
        import asyncpg
    except ModuleNotFoundError as exc:
        return CheckResult("Database", "fail", f"Missing dependency: {exc.name}")

    conn = None
    try:
        ensure_local_postgres_running(config)
        conn = await asyncpg.connect(config.database.dsn, timeout=5)
        await conn.execute("SELECT 1;")
        return CheckResult(
            "Database",
            "pass",
            f"Connected to {config.database.host}:{config.database.port}/{config.database.name}.",
        )
    except Exception as exc:
        return CheckResult("Database", "fail", f"Connection failed: {exc}")
    finally:
        if conn is not None:
            await conn.close()


async def _check_http_rpc(config: AppConfig) -> CheckResult:
    if not config.rpc.http_url:
        return CheckResult("HTTP RPC", "fail", "Resolved HTTP RPC URL is missing.")

    try:
        import aiohttp
    except ModuleNotFoundError as exc:
        return CheckResult("HTTP RPC", "fail", f"Missing dependency: {exc.name}")

    payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth", "params": []}
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(config.rpc.http_url, json=payload) as response:
                if response.status != 200:
                    return CheckResult(
                        "HTTP RPC",
                        "fail",
                        f"{redact_url(config.rpc.http_url)} returned HTTP {response.status}.",
                    )
                data = await response.json()
                if data.get("error"):
                    return CheckResult("HTTP RPC", "warn", f"RPC responded with error payload: {data['error']}")
                return CheckResult("HTTP RPC", "pass", f"Reachable at {redact_url(config.rpc.http_url)}.")
    except Exception as exc:
        return CheckResult("HTTP RPC", "fail", f"Reachability check failed: {exc}")


async def _check_ws_rpc(config: AppConfig) -> CheckResult:
    if not config.rpc.ws_url:
        return CheckResult("WebSocket RPC", "fail", "Resolved WebSocket RPC URL is missing.")

    try:
        import websockets
    except ModuleNotFoundError as exc:
        return CheckResult("WebSocket RPC", "fail", f"Missing dependency: {exc.name}")

    try:
        async with websockets.connect(config.rpc.ws_url, ping_interval=None, open_timeout=8, close_timeout=3):
            return CheckResult("WebSocket RPC", "pass", f"Reachable at {redact_url(config.rpc.ws_url)}.")
    except Exception as exc:
        return CheckResult("WebSocket RPC", "fail", f"WebSocket reachability failed: {exc}")


def _check_wallet(config: AppConfig, component: str) -> CheckResult:
    if not config.rpc.private_key:
        if component in {"trader", "all"} and config.runtime.mode in {"simulate", "live"}:
            return CheckResult("Wallet", "fail", "PRIVATE_KEY is required for simulate/live trader mode.")
        return CheckResult("Wallet", "warn", "PRIVATE_KEY is not configured.")

    try:
        import base58
        from solders.keypair import Keypair  # type: ignore

        keypair = Keypair.from_bytes(base58.b58decode(config.rpc.private_key))
        return CheckResult("Wallet", "pass", f"Decoded wallet {keypair.pubkey()}.")
    except ModuleNotFoundError as exc:
        return CheckResult("Wallet", "fail", f"Missing dependency: {exc.name}")
    except Exception as exc:
        return CheckResult("Wallet", "fail", f"PRIVATE_KEY could not be decoded: {exc}")


def _touch_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path, delete=True):
        pass


def _check_directories(config: AppConfig) -> CheckResult:
    try:
        ensure_directories(config)
        _touch_directory(config.paths.log_dir)
        if config.backup.enabled:
            _touch_directory(config.backup.directory)
        return CheckResult(
            "Directories",
            "pass",
            f"Writable log dir={config.paths.log_dir} backup dir={config.backup.directory}.",
        )
    except Exception as exc:
        return CheckResult("Directories", "fail", f"Directory write check failed: {exc}")


def _check_pg_dump(config: AppConfig) -> CheckResult:
    if not config.backup.enabled:
        return CheckResult("Backup Tooling", "skip", "Backups are disabled.")

    pg_dump_path = config.backup.pg_dump_path
    resolved = shutil.which(pg_dump_path) if pg_dump_path == "pg_dump" else None
    if pg_dump_path != "pg_dump" and Path(pg_dump_path).exists():
        return CheckResult("Backup Tooling", "pass", f"pg_dump found at {pg_dump_path}.")
    if resolved:
        return CheckResult("Backup Tooling", "pass", f"pg_dump found at {resolved}.")
    return CheckResult(
        "Backup Tooling",
        "warn",
        f"pg_dump was not found for DEXTER_PG_DUMP_PATH={pg_dump_path}. Backups will fail until this is fixed.",
    )


async def run_doctor(args: argparse.Namespace) -> int:
    try:
        config = load_config(args.mode, network_override=getattr(args, "network", None))
    except Exception as exc:
        print(f"[{current_utc_timestamp()}] FAIL  Config: {exc}")
        return 1

    checks = [
        _check_env(config, args.component),
        await _check_database(config),
        await _check_http_rpc(config),
        await _check_ws_rpc(config),
        _check_wallet(config, args.component),
        _check_directories(config),
        _check_pg_dump(config),
    ]

    exit_code = 0
    for check in checks:
        if check.status == "fail":
            exit_code = 1
        print(f"[{current_utc_timestamp()}] {_status_symbol(check.status):<5} {check.title}: {check.detail}")

    return exit_code


def run_collector(args: argparse.Namespace) -> int:
    from DexLab.wsLogs import run as run_collector_runtime

    return run_collector_runtime(
        mode_override=args.mode,
        network_override=getattr(args, "network", None),
    )


def run_trader(args: argparse.Namespace) -> int:
    from Dexter import run as run_trader_runtime

    return run_trader_runtime(
        mode_override=args.mode,
        network_override=getattr(args, "network", None),
    )


def run_start(args: argparse.Namespace) -> int:
    config, network_warning = _load_cli_config(args)
    args.network = config.runtime.network
    _render_runtime_summary(
        config,
        heading="Dexter Start",
        intent=f"start:{args.target}",
    )
    if network_warning:
        print(f"[{current_utc_timestamp()}] WARN  Start: {network_warning}")
    next_lines = [f"launch        {args.target} runtime in {config.runtime.mode} mode on {config.runtime.network}"]
    if args.target == "trade":
        collector_line = (
            "collector     auto-start wsLogs supervision is enabled"
            if getattr(config.runtime, "enable_wslogs", True)
            else "collector     auto-start wsLogs supervision is disabled"
        )
        next_lines.append(collector_line)
    _print_block("Next", next_lines)
    if args.doctor_first:
        doctor_code = asyncio.run(
            run_doctor(
                argparse.Namespace(
                    mode=args.mode,
                    network=config.runtime.network,
                    component="all" if args.target == "trade" else args.target,
                )
            )
        )
        if doctor_code != 0:
            return doctor_code

    if args.target == "trade":
        return run_trader(args)
    if args.target == "collector":
        return run_collector(args)
    return run_analyze(args)


def run_analyze(args: argparse.Namespace) -> int:
    from DexAI.trust_factor import Analyzer
    from dexter_config import log_startup_summary

    config = load_config(args.mode, network_override=getattr(args, "network", None))
    errors, warnings = validate_config(config, "analyze")
    if errors:
        for error in errors:
            print(f"[{current_utc_timestamp()}] FAIL  Analyze: {error}")
        return 1
    for warning in warnings:
        print(f"[{current_utc_timestamp()}] WARN  Analyze: {warning}")

    ensure_local_postgres_running(config)
    ensure_directories(config)
    log_startup_summary(__import__("logging").getLogger("dexter.analyze"), config, "analyze")
    analyzer = Analyzer(config.database.dsn)
    asyncio.run(analyzer.analyze_market())
    leaderboard = analyzer.process_results()
    if config.phase2.enabled:
        asyncio.run(
            Phase2Store(config.database.dsn, config=config).record_leaderboard_generation(
                leaderboard,
                source="dexter.analyze",
                metadata={"runtime_mode": config.runtime.mode},
            )
        )
    return 0


def run_database_init(args: argparse.Namespace) -> int:
    from database import run as run_database_runtime

    try:
        return run_database_runtime(network_override=getattr(args, "network", None))
    except Exception as exc:
        print(f"[{current_utc_timestamp()}] FAIL  Database init: {exc}")
        return 1


def run_database_setup(args: argparse.Namespace) -> int:
    from database_setup import run_windows_setup

    try:
        return int(run_windows_setup(args))
    except Exception as exc:
        print(f"[{current_utc_timestamp()}] FAIL  Database setup: {exc}")
        return 1


def _sol_to_lamports(value: float | None) -> int:
    if value is None:
        return 0
    decimal_value = Decimal(str(value))
    if decimal_value <= 0:
        return 0
    return int((decimal_value * Decimal("1000000000")).to_integral_value(rounding=ROUND_CEILING))


def _is_seeded_create(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "mint", None) or getattr(args, "owner", None))


def _create_invocation_is_ambiguous(args: argparse.Namespace) -> bool:
    fields = (
        "mint",
        "owner",
        "bonding_curve",
        "name",
        "symbol",
        "uri",
        "image",
        "description",
        "twitter",
        "telegram",
        "website",
        "buy_sol",
        "buy_price",
        "token_balance",
        "cost_basis_lamports",
    )
    return not any(getattr(args, field, None) not in (None, "", False, 0) for field in fields)


def _validate_create_args(args: argparse.Namespace) -> tuple[str, list[str]]:
    if _is_seeded_create(args):
        errors: list[str] = []
        if not args.mint or not args.owner:
            errors.append("Seeded create mode requires both --mint and --owner.")

        conflicting = []
        for flag, value in [
            ("--uri", getattr(args, "uri", None)),
            ("--image", getattr(args, "image", None)),
            ("--description", getattr(args, "description", None)),
            ("--twitter", getattr(args, "twitter", None)),
            ("--telegram", getattr(args, "telegram", None)),
            ("--website", getattr(args, "website", None)),
            ("--hide-name", getattr(args, "hide_name", False)),
            ("--ipfs-upload-url", getattr(args, "ipfs_upload_url", None)),
            ("--buy-sol", getattr(args, "buy_sol", None)),
            ("--priority-micro-lamports", getattr(args, "priority_micro_lamports", 0)),
            ("--simulate-tx", getattr(args, "simulate_tx", False)),
            ("--no-follow", getattr(args, "no_follow", False)),
        ]:
            if value not in (None, False, ""):
                conflicting.append(flag)

        if conflicting:
            errors.append(
                "Seeded create mode cannot be combined with on-chain create flags: "
                + ", ".join(conflicting)
            )
        return "seeded", errors

    errors = []
    if not getattr(args, "name", None):
        errors.append("On-chain create mode requires --name.")
    if not getattr(args, "symbol", None):
        errors.append("On-chain create mode requires --symbol.")
    if not getattr(args, "uri", None) and not getattr(args, "image", None):
        errors.append("On-chain create mode requires --uri or --image.")
    if getattr(args, "uri", None) and getattr(args, "image", None):
        errors.append("--uri cannot be combined with --image.")
    if getattr(args, "image", None):
        image_path = Path(str(args.image)).expanduser()
        if not image_path.is_file():
            errors.append(f"--image path was not found: {image_path}")
    if getattr(args, "description", None) or getattr(args, "twitter", None) or getattr(args, "telegram", None) or getattr(args, "website", None) or getattr(args, "ipfs_upload_url", None) or getattr(args, "hide_name", False):
        if not getattr(args, "image", None):
            errors.append("--description/--twitter/--telegram/--website/--hide-name/--ipfs-upload-url require --image.")
    if getattr(args, "dry_run", False) and getattr(args, "simulate_tx", False):
        errors.append("--dry-run cannot be combined with --simulate-tx.")
    if getattr(args, "buy_sol", None) is None or float(args.buy_sol) <= 0:
        errors.append("On-chain create mode requires --buy-sol > 0.")
    if not 0 <= float(getattr(args, "slippage_pct", 15.0)) <= 99:
        errors.append("--slippage-pct must be in [0, 99].")
    if getattr(args, "market", "pump_fun") != "pump_fun":
        errors.append("On-chain create mode currently supports Pump.fun launch only.")
    return "onchain", errors


def _seed_session_from_args(args: argparse.Namespace) -> dict:
    seed = {
        "mint_id": args.mint,
        "owner": args.owner,
        "name": args.name or args.symbol or args.mint[:8],
        "bonding_curve": args.bonding_curve,
        "trust_level": args.trust_level,
        "buy_price": _format_seed_number(args.buy_price),
        "token_balance": args.token_balance,
        "cost_basis_lamports": args.cost_basis_lamports,
        "profit_target_pct": args.profit_target_pct,
        "market": args.market,
        "skip_database": not args.load_database,
    }
    return {key: value for key, value in seed.items() if value is not None}


def _seed_session_from_create_result(args: argparse.Namespace, result: dict) -> dict:
    buy_price = result.get("buy_price") or result.get("expected_buy_price")
    normalized_buy_price = None
    if buy_price not in (None, ""):
        normalized_buy_price = format(Decimal(str(buy_price)).normalize(), "f")
    token_balance = int(result.get("token_balance_raw", 0) or 0)
    seed = {
        "mint_id": result["mint"],
        "owner": result["owner"],
        "name": args.name or args.symbol or result["mint"][:8],
        "bonding_curve": result["bonding_curve"],
        "trust_level": args.trust_level,
        "buy_price": normalized_buy_price,
        "token_balance": token_balance,
        "cost_basis_lamports": int(result.get("buy_sol_lamports", 0) or 0),
        "profit_target_pct": args.profit_target_pct,
        "market": result.get("market", "pump_fun"),
        "skip_database": not args.load_database,
    }
    return {key: value for key, value in seed.items() if value is not None}


async def _run_onchain_create(
    args: argparse.Namespace,
    config: AppConfig,
    *,
    execution: str | None = None,
) -> dict:
    import aiohttp
    from solana.rpc.async_api import AsyncClient

    from DexLab.pump_fun.pump_swap import DEFAULT_PUMP_FUN_IPFS_UPLOAD_URL, PumpFun

    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async_client = AsyncClient(config.rpc.http_url)
        try:
            builder = PumpFun(session, config.rpc.private_key, async_client)
            if execution is None:
                if getattr(args, "dry_run", False):
                    execution = "build"
                elif getattr(args, "simulate_tx", False):
                    execution = "simulate"
                else:
                    resolved = resolve_trade_execution_mode(config)
                    if resolved not in {"simulate", "live"}:
                        raise ValueError(
                            "On-chain create requires --mode simulate/live, --simulate-tx, or --dry-run."
                        )
                    execution = resolved
            return await builder.pump_create_token(
                name=args.name,
                symbol=args.symbol,
                uri=args.uri,
                image_path=args.image,
                description=args.description or "",
                twitter=args.twitter,
                telegram=args.telegram,
                website=args.website,
                show_name=not bool(args.hide_name),
                upload_url=args.ipfs_upload_url or DEFAULT_PUMP_FUN_IPFS_UPLOAD_URL,
                buy_sol_lamports=_sol_to_lamports(args.buy_sol),
                slippage_pct=float(args.slippage_pct),
                priority_micro_lamports=int(args.priority_micro_lamports),
                sim=execution == "simulate",
                send=execution == "live",
            )
        finally:
            await async_client.close()


def run_create_command(args: argparse.Namespace) -> int:
    if _create_invocation_is_ambiguous(args):
        network, warning = _resolve_cli_network(args)
        print(
            f"[{current_utc_timestamp()}] INFO  Create: choose a seeded handoff or an on-chain launch. "
            f"resolved_network={network}"
        )
        if warning:
            print(f"[{current_utc_timestamp()}] WARN  Create: {warning}")
        _print_block(
            "Create Modes",
            [
                "seeded        attach Dexter to an existing mint you already created or bought",
                "onchain       build, simulate, or send a Pump.fun create + optional bundled buy",
            ],
        )
        _print_block(
            "Examples",
            [
                "seeded        dexter create --network devnet --mode paper --mint <mint> --owner <owner> --buy-price 0.000000041 --token-balance 123456",
                "simulate      dexter create --network devnet --mode simulate --name DexterTest --symbol DXT --uri https://example.invalid/token.json --buy-sol 0.01",
                "mainnet       dexter create --network mainnet --mode live --dry-run --name DexterTest --symbol DXT --uri https://example.invalid/token.json --buy-sol 0.01",
            ],
        )
        return 1

    create_mode, create_errors = _validate_create_args(args)
    if create_errors:
        for error in create_errors:
            print(f"[{current_utc_timestamp()}] FAIL  Create: {error}")
        return 1

    config, network_warning = _load_cli_config(args)
    _render_runtime_summary(
        config,
        heading="Dexter Create",
        intent="create_seeded" if create_mode == "seeded" else "create_launch",
    )
    if network_warning:
        print(f"[{current_utc_timestamp()}] WARN  Create: {network_warning}")

    if create_mode == "seeded":
        seed = _seed_session_from_args(args)
        _print_block(
            "Seeded Session",
            [
                f"mint           {seed['mint_id']}",
                f"owner          {seed['owner']}",
                f"market         {seed.get('market', 'pump_fun')}",
                f"bonding_curve  {seed.get('bonding_curve', '(unset)')}",
                f"buy_price      {seed.get('buy_price', '(live discovery)')}",
                f"token_balance  {seed.get('token_balance', 0)}",
                f"skip_database  {seed.get('skip_database', False)}",
            ],
        )

        if args.dry_run:
            _print_block(
                "Next",
                ["preview       seeded handoff only; Dexter will not launch"],
            )
            _print_block(
                "Payload JSON",
                ["json          machine-readable seed payload follows"],
            )
            print(json.dumps(seed, indent=2, sort_keys=True))
            return 0

        _print_block(
            "Next",
            [f"launch        seeded trader session in {config.runtime.mode} mode on {config.runtime.network}"],
        )
        from Dexter import run as run_trader_runtime

        return run_trader_runtime(
            mode_override=args.mode,
            network_override=config.runtime.network,
            session_seed=seed,
        )

    if not config.rpc.private_key:
        print(f"[{current_utc_timestamp()}] FAIL  Create: PRIVATE_KEY is required for on-chain create mode.")
        return 1

    execution = "build" if args.dry_run else "simulate" if args.simulate_tx else "live"
    errors, warnings = validate_config(config, "trader")
    skip_database = execution in {"build", "simulate"} or args.no_follow or not args.load_database
    if skip_database:
        errors = [error for error in errors if "DATABASE_URL" not in error and "DB_*" not in error]
    if errors:
        for error in errors:
            print(f"[{current_utc_timestamp()}] FAIL  Create: {error}")
        return 1
    for warning in warnings:
        print(f"[{current_utc_timestamp()}] WARN  Create: {warning}")

    if execution == "live" and config.runtime.network == "mainnet":
        print(
            f"[{current_utc_timestamp()}] FAIL  Create: mainnet create sends are disabled here; use --dry-run or --simulate-tx."
        )
        return 1

    _print_block(
        "Create Plan",
        [
            f"name           {args.name}",
            f"symbol         {args.symbol}",
            f"uri            {args.uri or '(auto upload from image)'}",
            f"image          {args.image or '(unset)'}",
            f"buy_sol        {args.buy_sol}",
            f"slippage_pct   {args.slippage_pct}",
            f"tx_action      {execution}",
            f"runtime_mode   {config.runtime.mode}",
            f"follow         {False if args.no_follow else execution == 'live'}",
            f"skip_database  {not args.load_database}",
        ],
    )
    _print_block(
        "Next",
        [
            {
                "build": "launch        build create transaction only; no simulation or send",
                "simulate": "launch        build and simulate the signed create transaction",
                "live": f"launch        send create transaction on {config.runtime.network} and hand off to Dexter",
            }[execution],
        ],
    )

    try:
        result = asyncio.run(_run_onchain_create(args, config, execution=execution))
    except Exception as exc:
        print(f"[{current_utc_timestamp()}] FAIL  Create: {exc}")
        return 1

    _print_block(
        "Create Result",
        [
            f"mode           {result.get('mode', execution)}",
            f"mint           {result['mint']}",
            f"owner          {result['owner']}",
            f"bonding_curve  {result['bonding_curve']}",
            f"metadata_uri   {result.get('metadata_uri')}",
            f"token_program  {result.get('token_program')}",
            f"quote_tokens   {result.get('quote', {}).get('expected_tokens_out', 0)}",
            f"buy_price      {result.get('buy_price') or result.get('expected_buy_price') or '(unset)'}",
            f"signature      {result.get('signature', '(not sent)')}",
        ],
    )

    if execution in {"build", "simulate"} or args.no_follow:
        if execution in {"build", "simulate"}:
            _print_block(
                "Payload JSON",
                ["json          machine-readable create result follows"],
            )
            print(json.dumps(result, indent=2, sort_keys=True))
        if args.no_follow and execution == "live":
            _print_block("Next", ["create        transaction sent; follow disabled by --no-follow."])
        return 0

    if not result.get("confirmed"):
        _print_block("Next", ["create        transaction sent but not confirmed in time; inspect the signature before handoff."])
        return 0

    if int(result.get("token_balance_raw", 0) or 0) <= 0:
        _print_block("Next", ["create        transaction confirmed, but no bought balance is available for Dexter handoff."])
        return 0

    seed = _seed_session_from_create_result(args, result)
    _print_block(
        "Handoff",
        [
            f"mint           {seed['mint_id']}",
            f"token_balance  {seed.get('token_balance', 0)}",
            f"buy_price      {seed.get('buy_price', '(unset)')}",
        ],
    )
    _print_block(
        "Next",
        [f"launch        seeded trader session in {config.runtime.mode} mode on {config.runtime.network}"],
    )
    from Dexter import run as run_trader_runtime

    return run_trader_runtime(
        mode_override=args.mode,
        network_override=config.runtime.network,
        session_seed=seed,
    )


def run_manage_command(args: argparse.Namespace) -> int:
    dexter_module = sys.modules.get("Dexter") or sys.modules.get("__main__")
    if dexter_module is None or not all(
        hasattr(dexter_module, name)
        for name in ("_position_unrealized_profit_lamports", "liquidate_managed_position", "list_managed_positions")
    ):
        import Dexter as dexter_module  # type: ignore

    _position_unrealized_profit_lamports = dexter_module._position_unrealized_profit_lamports
    liquidate_managed_position = dexter_module.liquidate_managed_position
    list_managed_positions = dexter_module.list_managed_positions

    config = load_config(args.mode, network_override=getattr(args, "network", None))
    ensure_directories(config)

    positions = [
        item
        for item in list_managed_positions(config)
        if item.get("network") in (None, config.runtime.network)
    ]
    open_positions = [item for item in positions if item.get("status") == "open"]

    if args.sell_all:
        if not open_positions:
            print(f"[{current_utc_timestamp()}] WARN  Manage: no open positions found for {config.runtime.network}.")
            return 0
        exit_code = 0
        for position in open_positions:
            result = liquidate_managed_position(config, position, reason=args.reason or "manual_manage_sell")
            print(
                f"[{current_utc_timestamp()}] {'PASS' if result['result'] in {'sold', 'shadow_sold'} else 'WARN':<5} "
                f"Manage: mint={position['mint_id']} result={result['result']} mode={result['mode']} network={result['network']}"
            )
            if result["result"] not in {"sold", "shadow_sold"}:
                exit_code = 1
        return exit_code

    if args.sell_mint:
        position = next((item for item in open_positions if item.get("mint_id") == args.sell_mint), None)
        if position is None:
            print(
                f"[{current_utc_timestamp()}] FAIL  Manage: {args.sell_mint} is not present in the open-position recovery store for {config.runtime.network}."
            )
            return 1
        result = liquidate_managed_position(config, position, reason=args.reason or "manual_manage_sell")
        status = "PASS" if result["result"] in {"sold", "shadow_sold"} else "WARN"
        print(
            f"[{current_utc_timestamp()}] {status:<5} Manage: mint={args.sell_mint} result={result['result']} "
            f"mode={result['mode']} network={result['network']}"
        )
        updated_position = result.get("position") or {}
        if updated_position.get("realized_profit_lamports") is not None:
            print(
                f"[{current_utc_timestamp()}] INFO  Manage: realized_profit_lamports={updated_position.get('realized_profit_lamports')} "
                f"sell_proceeds_lamports={updated_position.get('sell_proceeds_lamports')}"
            )
        return 0 if result["result"] in {"sold", "shadow_sold"} else 1

    if args.json:
        print(json.dumps({"network": config.runtime.network, "positions": positions}, indent=2, sort_keys=True))
        return 0

    _render_runtime_summary(
        config,
        heading="Dexter Manage",
        intent="inspect_recovery_positions",
    )
    if not positions:
        print(f"[{current_utc_timestamp()}] WARN  Manage: no tracked positions found.")
        return 0

    for position in positions:
        pnl_lamports = _position_unrealized_profit_lamports(position)
        print(
            f"[{current_utc_timestamp()}] INFO  Manage: mint={position.get('mint_id')} status={position.get('status')} "
            f"market={position.get('market')} token_balance={int(position.get('token_balance', 0) or 0)} "
            f"buy_price={position.get('buy_price')} current_price={position.get('current_price') or position.get('last_price')} "
            f"pnl_lamports={pnl_lamports if pnl_lamports is not None else 'n/a'}"
        )
    return 0


def run_replay_command(args: argparse.Namespace) -> int:
    if not args.session_id and not args.mint_id:
        print(f"[{current_utc_timestamp()}] FAIL  Replay: provide --session-id or --mint-id.")
        return 1

    config = load_config(args.mode, network_override=getattr(args, "network", None))
    errors, warnings = validate_config(config, "analyze")
    if errors:
        for error in errors:
            print(f"[{current_utc_timestamp()}] FAIL  Replay: {error}")
        return 1
    for warning in warnings:
        print(f"[{current_utc_timestamp()}] WARN  Replay: {warning}")

    ensure_local_postgres_running(config)
    output = asyncio.run(
        run_replay(
            config.database.dsn,
            session_id=args.session_id,
            mint_id=args.mint_id,
            as_json=args.json,
        )
    )
    print(output)
    return 0


def run_export_command(args: argparse.Namespace) -> int:
    config = load_config(args.mode, network_override=getattr(args, "network", None))
    errors, warnings = validate_config(config, "analyze")
    if errors:
        for error in errors:
            print(f"[{current_utc_timestamp()}] FAIL  Export: {error}")
        return 1
    for warning in warnings:
        print(f"[{current_utc_timestamp()}] WARN  Export: {warning}")

    ensure_local_postgres_running(config)
    output_path, count = asyncio.run(
        run_export(
            config.database.dsn,
            config=config,
            kind=args.kind,
            output_path=args.output,
            session_id=args.session_id,
            mint_id=args.mint_id,
            leaderboard_version=args.leaderboard_version,
            limit=args.limit,
        )
    )
    print(f"[{current_utc_timestamp()}] PASS  Export: wrote {count} row(s) to {output_path}")
    return 0


def run_dashboard_command(args: argparse.Namespace) -> int:
    config = load_config(args.mode, network_override=getattr(args, "network", None))
    ensure_local_postgres_running(config)
    ensure_directories(config)
    return int(
        asyncio.run(
            run_dashboard(
                config,
                watch=bool(args.watch),
                interval=float(args.interval),
                as_json=bool(args.json),
                limit=int(args.limit),
            )
        )
    )


def run_control_command(args: argparse.Namespace) -> int:
    config = load_config(getattr(args, "mode", None), network_override=getattr(args, "network", None))
    ensure_directories(config)

    if args.action == "pause":
        pause_entries(config)
        print(f"[{current_utc_timestamp()}] PASS  Control: new entries paused via {config.runtime.emergency_stop_file}")
        return 0
    if args.action == "resume":
        resume_entries(config)
        print(f"[{current_utc_timestamp()}] PASS  Control: new entries resumed.")
        return 0
    if args.action == "force-sell":
        if not args.mint:
            print(f"[{current_utc_timestamp()}] FAIL  Control: --mint is required for force-sell.")
            return 1
        queue_force_sell(config, args.mint, reason=args.reason or "operator_force_sell")
        print(f"[{current_utc_timestamp()}] PASS  Control: queued force-sell for {args.mint}.")
        return 0
    if args.action == "blacklist":
        if not args.owner:
            print(f"[{current_utc_timestamp()}] FAIL  Control: --owner is required for blacklist.")
            return 1
        entries = blacklist_owner(config, args.owner)
        print(f"[{current_utc_timestamp()}] PASS  Control: blacklisted {args.owner}. entries={len(entries)}")
        return 0
    if args.action == "whitelist":
        if not args.owner:
            print(f"[{current_utc_timestamp()}] FAIL  Control: --owner is required for whitelist.")
            return 1
        entries = whitelist_owner(config, args.owner)
        print(f"[{current_utc_timestamp()}] PASS  Control: removed {args.owner} from blacklist. entries={len(entries)}")
        return 0
    if args.action == "watchlist-add":
        if not args.mint:
            print(f"[{current_utc_timestamp()}] FAIL  Control: --mint is required for watchlist-add.")
            return 1
        entries = add_watchlist_mint(config, args.mint)
        print(f"[{current_utc_timestamp()}] PASS  Control: added {args.mint} to watchlist. entries={len(entries)}")
        return 0
    if args.action == "watchlist-remove":
        if not args.mint:
            print(f"[{current_utc_timestamp()}] FAIL  Control: --mint is required for watchlist-remove.")
            return 1
        entries = remove_watchlist_mint(config, args.mint)
        print(f"[{current_utc_timestamp()}] PASS  Control: removed {args.mint} from watchlist. entries={len(entries)}")
        return 0
    print(f"[{current_utc_timestamp()}] FAIL  Control: unsupported action {args.action}.")
    return 1


def run_backtest_command(args: argparse.Namespace) -> int:
    config = load_config(args.mode, network_override=getattr(args, "network", None))
    ensure_local_postgres_running(config)
    ensure_directories(config)
    profile = get_strategy_profile(args.strategy or config.strategy.default_profile)

    if args.input:
        records = load_records_from_json(args.input)
    else:
        from DexAI.trust_factor import Analyzer

        analyzer = Analyzer(config.database.dsn)
        records = []
        offset = 0
        remaining = max(1, int(args.limit or 250))
        while len(records) < remaining:
            chunk = asyncio.run(analyzer.load_data(chunk_size=min(250, remaining - len(records)), offset=offset))
            if not chunk:
                break
            records.extend(chunk)
            offset += len(chunk)

    report = backtest_records(records, profile=profile)
    if config.phase2.enabled and config.database.dsn:
        store = Phase2Store(config.database.dsn, config=config)
        asyncio.run(store.ensure_schema())
        asyncio.run(
            store.ensure_strategy_profile(
                profile_key=profile.name,
                profile_name=profile.name,
                version=profile.version,
                definition=profile.as_dict(),
                metadata={"source": "dexter.backtest"},
            )
        )
        asyncio.run(
            store.record_backtest_run(
                profile_key=profile.name,
                input_source=args.input or "database:stagnant_mints",
                report=report,
            )
        )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(render_backtest_report(report))
    return 0


def run_verify_migration_command(args: argparse.Namespace) -> int:
    from dexter_migration_harness import run_migration_harness

    config = load_config(getattr(args, "mode", None), network_override=getattr(args, "network", None))
    if config.runtime.network != "devnet":
        print(f"[{current_utc_timestamp()}] FAIL  Verify Migration: only devnet is supported for this harness.")
        return 1
    ensure_directories(config)
    if args.json:
        import Dexter  # noqa: F401
    root_logger = logging.getLogger()
    previous_level = root_logger.level
    if args.json:
        root_logger.setLevel(logging.WARNING)
    try:
        report = asyncio.run(run_migration_harness(config))
    finally:
        if args.json:
            root_logger.setLevel(previous_level)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(
            f"[{current_utc_timestamp()}] PASS  Verify Migration: market={report['price_feed']['market']} "
            f"pool={report['price_feed']['pool']} exit={report['exit']['result']} venue={report['exit']['venue']} "
            f"report={report['report_file']}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dexter",
        description="Dexter runtime tooling. Use `dexter help` or `dexter help <command>` for guided command help.",
        epilog=(
            "Examples:\n"
            "  dexter database-setup\n"
            "  dexter run --network devnet --mode paper\n"
            "  dexter create --network devnet --mode paper --mint <mint> --owner <owner> --buy-price 0.000000041 --token-balance 123456\n"
            "  dexter create --network devnet --mode simulate --name DexterTest --symbol DXT --uri https://example.invalid/token.json --buy-sol 0.01\n"
            "  dexter manage --network devnet\n"
            "  dexter create --network devnet --mode live --name DexterTest --symbol DXT --uri https://example.invalid/token.json --buy-sol 0.01"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser(
        "run",
        aliases=["start"],
        help="Guided runtime entrypoint for trader, collector, or analyzer. Trader mode can auto-start wsLogs.",
        epilog="Examples:\n  dexter run --network devnet --mode paper\n  dexter run --target collector --network devnet --doctor-first",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    start.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    start.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command. Defaults to devnet when DEXTER_NETWORK is unset.")
    start.add_argument(
        "--target",
        choices=["trade", "collector", "analyze"],
        default="trade",
        help="Runtime target to launch.",
    )
    start.add_argument("--doctor-first", action="store_true", help="Run `dexter doctor` before launching the target runtime.")
    start.set_defaults(handler=run_start)

    create = subparsers.add_parser(
        "create",
        help="Launch a Pump.fun token or hand off an existing token into a focused Dexter session.",
        epilog=(
            "Examples:\n"
            "  dexter create --network devnet --mode paper --mint <mint> --owner <owner> --buy-price 0.000000041 --token-balance 123456\n"
            "  dexter create --network devnet --mode simulate --name DexterTest --symbol DXT --uri https://example.invalid/token.json --buy-sol 0.01\n"
            "  dexter create --network devnet --mode live --name DexterTest --symbol DXT --uri https://example.invalid/token.json --buy-sol 0.01\n"
            "  dexter create --network devnet --mode live --name DexterTest --symbol DXT --image ./token.png --description 'devnet smoke' --buy-sol 0.01 --no-follow"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    create.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    create.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command. Defaults to devnet when DEXTER_NETWORK is unset.")
    create.add_argument("--mint", help="Seeded mode: mint address for the created/bought token that Dexter should take over.")
    create.add_argument("--owner", help="Seeded mode: creator/owner address associated with the session.")
    create.add_argument("--bonding-curve", help="Seeded mode: Pump.fun bonding-curve address when the token is still on Pump.fun.")
    create.add_argument("--name", help="On-chain mode: token name. Seeded mode: optional display name override.")
    create.add_argument("--symbol", help="On-chain mode: token symbol. Seeded mode: optional symbol override.")
    create.add_argument("--uri", help="On-chain mode: metadata URI to store on-chain.")
    create.add_argument("--image", help="On-chain mode: image path to upload to Pump.fun IPFS; implies metadata upload.")
    create.add_argument("--description", help="On-chain mode: metadata description used with --image.")
    create.add_argument("--twitter", help="On-chain mode: metadata twitter URL used with --image.")
    create.add_argument("--telegram", help="On-chain mode: metadata telegram URL used with --image.")
    create.add_argument("--website", help="On-chain mode: metadata website URL used with --image.")
    create.add_argument("--hide-name", action="store_true", help="On-chain mode: request showName=false in the Pump.fun IPFS upload.")
    create.add_argument("--ipfs-upload-url", help="On-chain mode: override the Pump.fun IPFS upload endpoint used with --image.")
    create.add_argument("--buy-sol", type=float, help="On-chain mode: bundled auto-buy amount in SOL.")
    create.add_argument("--slippage-pct", type=float, default=15.0, help="On-chain mode: bundled auto-buy slippage percent. Matches Mamba's 15%% default.")
    create.add_argument("--priority-micro-lamports", type=int, default=0, help="On-chain mode: optional compute-unit price for the create transaction.")
    create.add_argument("--simulate-tx", action="store_true", help="On-chain mode: force simulation even when --mode live would otherwise send.")
    create.add_argument("--no-follow", action="store_true", help="On-chain live mode: create/send without launching Dexter afterwards.")
    create.add_argument("--market", choices=["pump_fun", "pump_swap"], default="pump_fun", help="Seeded mode: initial market source for the session.")
    create.add_argument("--trust-level", type=int, default=1, help="Initial trust level for the seeded session.")
    create.add_argument("--buy-price", type=float, help="Seed the current/buy price so the session can start immediately.")
    create.add_argument("--token-balance", type=int, default=0, help="Seed the token balance already held for the session.")
    create.add_argument("--cost-basis-lamports", type=int, default=0, help="Optional seeded cost basis for PnL reporting.")
    create.add_argument("--profit-target-pct", type=float, default=float(PRICE_STEP_UNITS), help="Seeded target percentage for the single-mint session.")
    create.add_argument("--load-database", action="store_true", help="Opt into the standard database bootstrap instead of the default DB-free create handoff.")
    create.add_argument("--dry-run", action="store_true", help="Build the create payload without simulating or sending it. On-chain mode still resolves metadata and signs the transaction locally.")
    create.set_defaults(handler=run_create_command)

    manage = subparsers.add_parser(
        "manage",
        help="Inspect and liquidate tracked positions from Dexter's local recovery store.",
    )
    manage.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    manage.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command.")
    manage.add_argument("--sell-mint", help="Sell a specific tracked mint from the recovery store.")
    manage.add_argument("--sell-all", action="store_true", help="Sell every open tracked position for the selected network.")
    manage.add_argument("--reason", default="manual_manage_sell", help="Reason recorded for manual exits.")
    manage.add_argument("--json", action="store_true", help="Emit the recovery store view as JSON.")
    manage.set_defaults(handler=run_manage_command)

    doctor = subparsers.add_parser("doctor", help="Validate environment, connectivity, and writable paths.")
    doctor.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    doctor.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command.")
    doctor.add_argument(
        "--component",
        choices=["all", "collector", "trader"],
        default="all",
        help="Validation profile to run.",
    )
    doctor.set_defaults(handler=lambda args: asyncio.run(run_doctor(args)))

    collector = subparsers.add_parser("collector", help="Start the log collector standalone with Dexter's validation checks.")
    collector.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    collector.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command.")
    collector.set_defaults(handler=run_collector)

    trader = subparsers.add_parser("trade", help="Start the trader with Dexter's validation checks and auto-start wsLogs when enabled.")
    trader.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    trader.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command.")
    trader.set_defaults(handler=run_trader)

    analyze = subparsers.add_parser("analyze", help="Run the creator analyzer.")
    analyze.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    analyze.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command.")
    analyze.set_defaults(handler=run_analyze)

    replay = subparsers.add_parser("replay", help="Replay a stored normalized session from Dexter's research data.")
    replay.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    replay.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command.")
    replay.add_argument("--session-id", help="Replay a specific normalized session id.")
    replay.add_argument("--mint-id", help="Replay the latest normalized session for a mint.")
    replay.add_argument("--json", action="store_true", help="Emit the replay report as JSON.")
    replay.set_defaults(handler=run_replay_command)

    backtest = subparsers.add_parser("backtest", help="Evaluate a strategy profile offline against historical data.")
    backtest.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    backtest.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command.")
    backtest.add_argument("--strategy", choices=["aggressive", "balanced", "conservative"], help="Strategy profile to evaluate.")
    backtest.add_argument("--input", help="Optional JSON/JSONL historical dataset. Defaults to loading stagnant_mints via Analyzer.")
    backtest.add_argument("--limit", type=int, default=250, help="Max records to process when --input is omitted.")
    backtest.add_argument("--json", action="store_true", help="Emit the backtest report as JSON.")
    backtest.set_defaults(handler=run_backtest_command)

    verify_migration = subparsers.add_parser(
        "verify-migration",
        help="Run a deterministic devnet-safe migration harness for the Pump.fun -> PumpSwap exit path.",
    )
    verify_migration.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for config loading.")
    verify_migration.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command. Devnet only.")
    verify_migration.add_argument("--json", action="store_true", help="Emit the harness report as JSON.")
    verify_migration.set_defaults(handler=run_verify_migration_command)

    export = subparsers.add_parser("export", help="Export Dexter's normalized research data.")
    export.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    export.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command.")
    export.add_argument("--kind", choices=["sessions", "raw_events", "leaderboard", "positions", "risk_events", "strategy_profiles"], required=True, help="Dataset to export.")
    export.add_argument("--output", help="Destination JSONL path. Defaults to the research export dir.")
    export.add_argument("--session-id", help="Filter session exports to one session id.")
    export.add_argument("--mint-id", help="Filter session/raw-event exports to one mint.")
    export.add_argument("--leaderboard-version", help="Export entries for a specific leaderboard version.")
    export.add_argument("--limit", type=int, help="Optional max rows to export.")
    export.set_defaults(handler=run_export_command)

    dashboard = subparsers.add_parser("dashboard", help="Render the local operator dashboard.")
    dashboard.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    dashboard.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command.")
    dashboard.add_argument("--watch", action="store_true", help="Refresh the dashboard until interrupted.")
    dashboard.add_argument("--interval", type=float, default=2.0, help="Refresh interval in seconds for --watch.")
    dashboard.add_argument("--limit", type=int, default=5, help="Rows per dashboard section.")
    dashboard.add_argument("--json", action="store_true", help="Emit the dashboard snapshot as JSON.")
    dashboard.set_defaults(handler=run_dashboard_command)

    control = subparsers.add_parser("control", help="Manual operator controls for pause/resume, force-sell, and lists.")
    control.add_argument("--mode", choices=["read_only", "paper", "simulate", "live"], help="Override DEXTER_RUNTIME_MODE for this command.")
    control.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command.")
    control.add_argument("action", choices=["pause", "resume", "force-sell", "blacklist", "whitelist", "watchlist-add", "watchlist-remove"])
    control.add_argument("--owner", help="Creator address for blacklist/whitelist.")
    control.add_argument("--mint", help="Mint address for force-sell/watchlist actions.")
    control.add_argument("--reason", default="operator_force_sell", help="Optional reason for force-sell.")
    control.set_defaults(handler=run_control_command)

    database_setup = subparsers.add_parser("database-setup", help="Install or repair Dexter's managed local PostgreSQL instance on Windows.")
    database_setup.add_argument("--network", choices=["devnet", "mainnet"], help="Optional Dexter network override for config loading.")
    database_setup.add_argument("--major-version", default="17", help="WinGet PostgreSQL major version to install when PostgreSQL binaries are missing.")
    database_setup.add_argument("--admin-password", help="Password for Dexter's local postgres superuser. Defaults to the managed value or a generated password.")
    database_setup.add_argument("--db-user", help="Dexter application database user. Defaults to the managed value, DB_USER, or dexter_user.")
    database_setup.add_argument("--db-password", help="Dexter application database password. Defaults to the managed value, DB_PASSWORD, or a generated value.")
    database_setup.add_argument("--db-name", help="Dexter application database name. Defaults to the managed value, DB_NAME, or dexter_db.")
    database_setup.add_argument("--db-port", type=int, help="Local PostgreSQL port. Defaults to the managed value or 55432.")
    database_setup.add_argument("--cluster-dir", help="Path for Dexter's local PostgreSQL data directory. Defaults to .dexter/postgres/<version>/data.")
    database_setup.add_argument("--log-file", help="Path for Dexter's local PostgreSQL log file.")
    database_setup.add_argument("--skip-install", action="store_true", help="Do not run WinGet; assume PostgreSQL binaries are already installed locally.")
    database_setup.add_argument("--dry-run", action="store_true", help="Print the intended install/setup actions without changing the machine.")
    database_setup.set_defaults(handler=run_database_setup)

    database_init = subparsers.add_parser("database-init", help="Create or repair Dexter's database roles, tables, and schema after PostgreSQL exists.")
    database_init.add_argument("--network", choices=["devnet", "mainnet"], help="Override DEXTER_NETWORK for this command.")
    database_init.set_defaults(handler=run_database_init)

    return parser


def legacy_main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = list(argv if argv is not None else sys.argv[1:])
    if not args:
        parser.print_help()
        return 1

    if args[0] in {"help", "--help", "-h"}:
        if len(args) == 1:
            parser.print_help()
            return 0
        subcommand = args[1]
        subparser = _get_subcommand_parser(parser, subcommand)
        if subparser is None:
            print(f"[{current_utc_timestamp()}] FAIL  Help: unknown command '{subcommand}'.")
            parser.print_help()
            return 1
        subparser.print_help()
        return 0

    parsed = parser.parse_args(args)
    return int(parsed.handler(parsed))


def main(argv: list[str] | None = None) -> int:
    from Dexter import main as dexter_main

    return int(dexter_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
