from decimal import Decimal
from pathlib import Path
import tempfile
from unittest import TestCase

from dexter_smoke import SmokeBudget


class BudgetTests(TestCase):
    def budget(self, directory, price='100', cap='1'):
        return SmokeBudget(Path(directory)/'ledger.json','wallet',Decimal(price),Decimal(cap))

    def test_counts_rent_fees_and_reserves_before_send(self):
        with tempfile.TemporaryDirectory() as directory:
            budget=self.budget(directory)
            budget.check(6_000_000,3_000_000)
            with self.assertRaises(RuntimeError):
                budget.check(7_000_000,3_000_000)

    def test_unknown_signature_blocks_another_test_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            budget=self.budget(directory)
            budget.submitted(signature='known-signature',mint='mint',side='buy',market='pump_swap')
            reloaded=self.budget(directory)
            with self.assertRaisesRegex(RuntimeError,'unresolved'):
                reloaded.check(1)

    def test_recovered_rent_reduces_net_outlay_and_price_never_reduces(self):
        with tempfile.TemporaryDirectory() as directory:
            budget=self.budget(directory)
            budget.data['transactions']=[{'status':'confirmed','wallet_delta_lamports':-5_000_000},
                                         {'status':'confirmed','wallet_delta_lamports':4_500_000}]
            budget.save()
            reloaded=self.budget(directory,price='90')
            self.assertEqual(reloaded.outlay,500_000)
            self.assertEqual(reloaded.price,Decimal('100'))

    def test_rejects_invalid_or_above_authorized_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            for cap in ('NaN','Infinity','0','-1','1.01'):
                with self.assertRaises(ValueError):
                    self.budget(directory,cap=cap)
