from __future__ import annotations

import json
import types
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from unittest import mock


class _HarnessAnalyzer:
    def __init__(self, db_dsn):
        self.db_dsn = db_dsn
        self.total_supply = Decimal("1000000000")
        self.sol_price_usd = Decimal("210")


def _seed_harness_position(trader) -> None:
    trader.holdings["mint-harness"] = {
        "owner": "owner-harness",
        "trust_level": 1,
        "token_balance": 123456,
        "buy_price": "0.0000000280",
        "cost_basis_lamports": 100000,
    }
    trader.swap_folder["mint-harness"] = {
        "name": "Mint Harness",
        "owner": "owner-harness",
        "bonding_curve": "curve-harness",
        "market": "pump_fun",
        "migration_pending": True,
        "state": {
            "price": Decimal("0.0000000280"),
            "open_price": Decimal("0.0000000280"),
            "high_price": Decimal("0.0000000280"),
            "mc": Decimal("0"),
            "price_usd": 0.0,
            "last_tx_time": "0.000",
            "holders": {},
            "price_history": {},
            "tx_counts": {"swaps": 0, "buys": 0, "sells": 0},
            "created": 0,
        },
    }


def _write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


async def run_migration_harness(config) -> dict:
    if config.runtime.network != "devnet":
        raise ValueError("verify-migration only supports devnet.")

    import Dexter as dexter_module

    harness_config = replace(
        config,
        runtime=replace(
            config.runtime,
            mode="live",
            network="devnet",
            close_positions_on_shutdown=False,
            allow_mainnet_live=False,
            mainnet_dry_run=True,
        ),
        phase2=replace(config.phase2, enabled=False),
    )

    saved_results = []

    async def fake_save_result(result):
        saved_results.append(result)

    with mock.patch.object(dexter_module, "Analyzer", _HarnessAnalyzer):
        trader = dexter_module.Dexter(harness_config)

    trader.phase2 = None
    trader.save_result = fake_save_result  # type: ignore[assignment]
    trader._validate_result = mock.AsyncMock()
    trader.wallet_balance = 100000000
    trader.dexLogs = types.SimpleNamespace(
        process_log=mock.AsyncMock(return_value={"signature": "pump-swap-sig", "slot": 321}),
    )
    trader.pump_swap = types.SimpleNamespace(
        pump_sell=mock.AsyncMock(return_value="migrated"),
        close=mock.AsyncMock(),
    )
    trader.pump_swap_executor = types.SimpleNamespace(
        pump_sell=mock.AsyncMock(return_value="sell-tx-pump-swap"),
    )
    trader.pump_swap_market = types.SimpleNamespace(
        find_pool_for_mint=mock.AsyncMock(
            return_value=types.SimpleNamespace(
                pool="pool-migrated",
                creator="owner-harness",
                coin_creator="owner-harness",
                is_mayhem_mode=False,
            )
        ),
        fetch_price_snapshot=mock.AsyncMock(
            return_value={
                "pool": "pool-migrated",
                "price": Decimal("0.0000000315"),
                "liquidity_sol": Decimal("3.2"),
            }
        ),
    )
    trader.swaps = types.SimpleNamespace(
        get_swap_tx=mock.AsyncMock(
            return_value={
                "wallet_balance": 100078000,
                "wallet_delta_lamports": 78000,
                "price": 0,
                "fee_lamports": 7000,
            }
        )
    )
    _seed_harness_position(trader)

    await trader.handle_single_log({"params": {}}, dexter_module.PUMP_SWAP)
    post_promotion = trader.swap_folder["mint-harness"]

    sell_result = await trader.sell(
        "mint-harness",
        123456,
        "harness-drop-time",
        "owner-harness",
        1,
        Decimal("0.0000000280"),
    )

    report = {
        "harness": "pump_fun_to_pump_swap_migration",
        "network": harness_config.runtime.network,
        "funded_send": False,
        "mainnet_send": False,
        "price_feed": {
            "market": post_promotion.get("market"),
            "pool": post_promotion.get("pump_swap_pool"),
            "price": str(post_promotion.get("state", {}).get("price")),
            "pump_swap_swaps": int(post_promotion.get("state", {}).get("tx_counts", {}).get("pump_swap_swaps", 0) or 0),
        },
        "exit": {
            "result": sell_result,
            "venue": saved_results[0]["venue"] if saved_results else None,
            "wallet_delta_lamports": saved_results[0]["sell_proceeds_lamports"] if saved_results else None,
            "signature": "sell-tx-pump-swap",
        },
        "report_file": str(config.paths.state_dir / "migration-harness-report.json"),
    }
    _write_report(config.paths.state_dir / "migration-harness-report.json", report)
    return report
