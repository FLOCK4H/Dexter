from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from unittest import TestCase

from DexLab.market import _normalize_holder_balances
from dexter_phase2 import Phase2Store, _compact_mint_snapshot


class StorageCompactionTests(TestCase):
    def test_normalize_holder_balances_removes_balance_change_history(self) -> None:
        holders = {
            "wallet_a": {
                "balance": 12.5,
                "balance_changes": [
                    {"type": "buy", "price_was": 1.23},
                    {"type": "sell", "price_was": 2.34},
                ],
            },
            "wallet_b": 3,
        }

        normalized = _normalize_holder_balances(holders)

        self.assertEqual(normalized, {"wallet_a": 12.5, "wallet_b": 3.0})

    def test_compact_mint_snapshot_drops_large_json_fields(self) -> None:
        snapshot = {
            "mint_sig": "sig-123",
            "created_at": dt.datetime(2026, 3, 28, tzinfo=dt.timezone.utc),
            "holders": {"wallet_a": 1.0, "wallet_b": 2.0},
            "price_history": {"1.000": 0.1, "2.000": 0.2, "3.000": 0.3},
            "tx_counts": {"swaps": 7, "buys": 5, "sells": 2},
            "volume": {"30sec": {"swaps": 7}},
        }

        compact_snapshot, compact_payload = _compact_mint_snapshot(snapshot)

        self.assertEqual(compact_snapshot["holders"], {})
        self.assertEqual(compact_snapshot["price_history"], {})
        self.assertEqual(compact_payload["holder_count"], 2)
        self.assertEqual(compact_payload["price_point_count"], 3)
        self.assertEqual(compact_payload["swap_count"], 7)

    def test_active_snapshot_throttle_uses_last_persisted_time(self) -> None:
        config = SimpleNamespace(
            phase2=SimpleNamespace(
                mint_snapshot_interval_seconds=15,
                mint_snapshot_retention_per_mint=4,
                maintenance_interval_seconds=60,
                max_database_size_bytes=0,
                raw_event_retention_days=30,
            )
        )
        store = Phase2Store("postgres://unused", config=config)
        t0 = dt.datetime(2026, 3, 28, tzinfo=dt.timezone.utc)

        self.assertTrue(
            store._should_record_mint_snapshot(
                mint_id="mint-1",
                lifecycle_state="active",
                recorded_at=t0,
            )
        )
        self.assertFalse(
            store._should_record_mint_snapshot(
                mint_id="mint-1",
                lifecycle_state="active",
                recorded_at=t0 + dt.timedelta(seconds=5),
            )
        )
        self.assertTrue(
            store._should_record_mint_snapshot(
                mint_id="mint-1",
                lifecycle_state="active",
                recorded_at=t0 + dt.timedelta(seconds=16),
            )
        )
