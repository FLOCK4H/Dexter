from __future__ import annotations

import asyncio
import base64
from decimal import Decimal
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase, TestCase, mock

from solders.keypair import Keypair
from solders.pubkey import Pubkey

import dexter_config
from dexter_execution import NativeExecutor, NativeMarket, decode_instructions, slippage_percent


class BoundaryTests(TestCase):
    def test_confirmed_fill_counts_acquired_tokens_without_preexisting_holdings(self):
        from DexLab.swaps import SolanaSwaps
        swaps=SolanaSwaps.__new__(SolanaSwaps)
        swaps.wallet_address='our-wallet'
        def entry(owner,amount):
            return {'mint':'mint','owner':owner,'uiTokenAmount':{'amount':str(amount)}}
        result={'meta':{'preTokenBalances':[entry('our-wallet',1000)],
                        'postTokenBalances':[entry('our-wallet',1600),entry('other-wallet',999999)]}}
        self.assertEqual(swaps._wallet_token_delta(result,'mint'),(600,1600))

    def test_legacy_slippage_ratio_converts_to_percent(self):
        self.assertEqual(slippage_percent(1.3), 30)
        self.assertEqual(slippage_percent(5), 5)
        for invalid in (0, -1, 1, 100, 'NaN', 'Infinity'):
            with self.assertRaises(ValueError):
                slippage_percent(invalid)

    def test_instruction_boundary_preserves_bytes_and_account_flags(self):
        address = str(Pubkey.new_unique())
        result = decode_instructions([{'program_id':address, 'data':base64.b64encode(b'\x00\xff').decode(),
            'accounts':[{'pubkey':address,'is_signer':False,'is_writable':True}]}])[0]
        self.assertEqual(result.data, b'\x00\xff')
        self.assertTrue(result.accounts[0].is_writable)
        self.assertFalse(result.accounts[0].is_signer)

    def test_dexter_never_reads_external_mamba_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / '.env'
            env.write_text('MAMBA_PRIVATE_KEY=shared-key\nMAMBA_API_HTTP_URLS=https://rpc.example\n'
                           'MAMBA_API_ENABLE_LIVE_SENDS=true\nDEXTER_ALLOW_MAINNET_LIVE=true\n')
            with mock.patch.dict('os.environ', {'MAMBA_ENV_FILE':str(env)}, clear=True):
                with mock.patch.object(dexter_config, 'load_dotenv') as load:
                    dexter_config.load_env_file()
                load.assert_called_once_with(dotenv_path=dexter_config.ENV_PATH, override=False)
                import os
                self.assertNotIn('MAMBA_PRIVATE_KEY', os.environ)
                self.assertNotIn('MAMBA_API_ENABLE_LIVE_SENDS', os.environ)
                self.assertNotIn('DEXTER_ALLOW_MAINNET_LIVE', os.environ)

    def test_own_signer_and_devnet_rpc_isolation(self):
        with mock.patch.dict('os.environ', {'MAMBA_PRIVATE_KEY':'mamba', 'PRIVATE_KEY':'legacy',
                    'HTTP_URLS':'https://mainnet.example', 'MAMBA_ENV_FILE':'missing'}, clear=True):
            with mock.patch.object(dexter_config, 'load_dotenv'):
                dexter_config.load_config.cache_clear()
                config = dexter_config.load_config('simulate', 'devnet')
                self.assertEqual(config.rpc.private_key, 'legacy')
                self.assertEqual(config.rpc.http_url, 'https://api.devnet.solana.com')
                self.assertNotIn('mamba', repr(config.rpc))
        dexter_config.load_config.cache_clear()


class BridgeAsyncTests(IsolatedAsyncioTestCase):
    async def test_cleared_signatures_do_not_reappear_from_recovery_store(self):
        import Dexter
        trader=Dexter.Dexter.__new__(Dexter.Dexter)
        trader.holdings={'mint':{'token_balance':1234,'buy_price':'1','owner':'creator'}}
        trader.swap_folder={'mint':{'market':'pump_swap'}}
        trader.config=SimpleNamespace(runtime=SimpleNamespace(network='mainnet'))
        trader.runtime_mode='simulate'
        trader.execution_label='simulate'
        trader.shadow_mode=True
        record=trader._build_managed_position_record('mint',existing={
            'pending_buy_tx':'old-buy','pending_sell_tx':'old-sell'})
        self.assertIsNone(record['pending_buy_tx'])
        self.assertIsNone(record['pending_sell_tx'])

    async def test_market_switch_preserves_held_amount_and_persists_new_venue(self):
        import Dexter
        trader=Dexter.Dexter.__new__(Dexter.Dexter)
        trader.holdings={'mint':{'token_balance':1234,'market':'pump_fun'}}
        trader.swap_folder={'mint':{'market':'pump_fun','migration_pending':True}}
        trader._sync_managed_position=mock.Mock()
        await trader._apply_native_route('mint',{'market':'pump_swap','pool':'new-pool','creator':'creator'})
        self.assertEqual(trader.holdings['mint'],{'token_balance':1234,'market':'pump_swap'})
        self.assertFalse(trader.swap_folder['mint']['migration_pending'])
        trader._sync_managed_position.assert_called_once_with('mint',status='open')

    async def test_shutdown_keeps_an_unresolved_wallet_position(self):
        import Dexter
        trader=Dexter.Dexter.__new__(Dexter.Dexter)
        holding={'owner':'creator','pending_buy_tx':'known-signature'}
        trader.holdings={'mint':holding}
        trader.swap_folder={'mint':{'migration_pending':True}}
        trader._log_exposure_summary=mock.Mock()
        trader._resolve_position_for_shutdown=mock.AsyncMock(return_value=(holding,0,Decimal(0)))
        trader._sync_managed_position=mock.Mock()
        trader._release_buy_spend=mock.Mock()
        await trader._liquidate_open_positions('shutdown')
        self.assertIn('mint',trader.holdings)
        self.assertIn('mint',trader.swap_folder)
        trader._release_buy_spend.assert_not_called()

    def executor(self, sdk, **kwargs):
        key = Keypair()
        return NativeExecutor(None, str(key), mock.AsyncMock(), sdk=sdk, **kwargs)

    async def test_sell_after_migration_uses_new_build_and_exact_position_amount(self):
        sdk = SimpleNamespace(request=mock.AsyncMock(side_effect=[
            {'market':'pump_fun','pool':'curve','creator':'creator','instructions':[]},
            {'market':'pump_swap','pool':'migrated-pool','creator':'creator','instructions':[]},
        ]))
        on_route = mock.AsyncMock()
        executor = self.executor(sdk, on_route=on_route)
        await executor.build('mint', 'buy', lamports=1000)
        result = await executor.build('mint', 'sell', token_amount_raw=7654321)
        self.assertEqual(result['market'], 'pump_swap')
        self.assertEqual(sdk.request.await_args.kwargs['token_amount_raw'], 7654321)
        self.assertNotIn('pool', sdk.request.await_args.kwargs)
        self.assertEqual(on_route.await_count, 2)

    async def test_live_gate_runs_before_building_or_signing(self):
        sdk = SimpleNamespace(request=mock.AsyncMock())
        executor = self.executor(sdk)
        with self.assertRaisesRegex(RuntimeError, 'live sends are disabled'):
            await executor.pump_buy('mint', 'curve', 1000)
        sdk.request.assert_not_awaited()
        executor.async_client.send_transaction.assert_not_awaited()

    async def test_migration_window_returns_pending_without_submitting(self):
        sdk = SimpleNamespace(request=mock.AsyncMock(side_effect=ValueError('route_unavailable_or_migration_pending')))
        executor = self.executor(sdk, allow_live_send=True)
        self.assertEqual(await executor.pump_sell('mint', 'curve', 1234), 'migration_pending')
        executor.async_client.send_transaction.assert_not_awaited()

    async def test_transport_failure_is_not_silenced_as_missing_pool(self):
        sdk = SimpleNamespace(request=mock.AsyncMock(side_effect=ValueError('RPC timed out')))
        market = NativeMarket(None, 'https://rpc.example', sdk=sdk)
        with self.assertRaisesRegex(ValueError, 'RPC timed out'):
            await market.find_pool_for_mint('mint')

    async def test_snapshot_keeps_decimal_prices(self):
        sdk = SimpleNamespace(request=mock.AsyncMock(return_value={'price':0.0000000123,'liquidity_sol':5.0}))
        snapshot = await NativeMarket(None, 'https://rpc.example', sdk=sdk).fetch_price_snapshot(pool='pool')
        self.assertEqual(snapshot['price'], Decimal('0.0000000123'))

    async def test_unexpected_signer_is_rejected_before_rpc(self):
        executor = self.executor(SimpleNamespace())
        other = str(Pubkey.new_unique())
        plan = {'priority_micro_lamports':0,'instructions':[{'program_id':other,'data':'',
            'accounts':[{'pubkey':other,'is_signer':True,'is_writable':True}]}]}
        with self.assertRaisesRegex(ValueError, 'unexpected signer'):
            await executor.transaction(plan)
        executor.async_client.get_latest_blockhash.assert_not_awaited()
