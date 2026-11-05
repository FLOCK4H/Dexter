from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal
import json
import threading
from unittest import IsolatedAsyncioTestCase, mock

import dexter_network as network
from DexLab.wsLogs import DexBetterLogs
from DexLab.market import Market


class WebsocketHealthTests(IsolatedAsyncioTestCase):
    def setUp(self):
        network._ws_cursors.clear()

    async def test_cancelling_a_healthy_stream_does_not_rotate_the_provider(self):
        ws=mock.AsyncMock()
        ws.close_code=None
        with mock.patch.dict('os.environ',{'WS_URLS':'ws://one.example,ws://two.example'},clear=True):
            with mock.patch('websockets.connect',new=mock.AsyncMock(return_value=ws)) as connect:
                with self.assertRaises(asyncio.CancelledError):
                    async with network.rpc_websocket('ws://one.example'):
                        raise asyncio.CancelledError()
                async with network.rpc_websocket('ws://one.example'):
                    pass
        self.assertEqual([call.args[0] for call in connect.await_args_list],['ws://one.example']*2)

    async def test_application_error_does_not_rotate_a_healthy_provider(self):
        ws=mock.AsyncMock()
        ws.close_code=None
        with mock.patch.dict('os.environ',{'WS_URLS':'ws://one.example,ws://two.example'},clear=True):
            with mock.patch('websockets.connect',new=mock.AsyncMock(return_value=ws)) as connect:
                with self.assertRaises(ValueError):
                    async with network.rpc_websocket('ws://one.example'):
                        raise ValueError('application parser failure')
                async with network.rpc_websocket('ws://one.example'):
                    pass
        self.assertEqual(connect.await_args.args[0],'ws://one.example')

    async def test_rejected_subscription_rotates_and_error_log_omits_rpc_credentials(self):
        ws=mock.AsyncMock()
        ws.close_code=None
        endpoint='ws://one.example/private-path?api-key=secret-value'
        with mock.patch.dict('os.environ',{'WS_URLS':endpoint+',ws://two.example'},clear=True):
            with mock.patch('websockets.connect',new=mock.AsyncMock(return_value=ws)) as connect:
                with self.assertLogs('dexter.websocket',level='WARNING') as log:
                    with self.assertRaises(network.SubscriptionRejected):
                        async with network.rpc_websocket(endpoint):
                            raise network.SubscriptionRejected('logsSubscribe rejected (code=-32005)')
                async with network.rpc_websocket(endpoint):
                    pass
        self.assertEqual(connect.await_args.args[0],'ws://two.example')
        output=' '.join(log.output)
        self.assertIn('-32005',output)
        self.assertNotIn('secret-value',output)
        self.assertNotIn('private-path',output)

    async def test_subscription_acknowledgement_timeout_is_bounded(self):
        ws=mock.AsyncMock()
        blocker=asyncio.Event()
        ws.recv.side_effect=blocker.wait
        with mock.patch.dict('os.environ',{'DEXTER_WS_SUBSCRIBE_TIMEOUT_SECONDS':'0.01'},clear=True):
            with self.assertRaisesRegex(network.SubscriptionRejected,'acknowledgement timed out'):
                await network.subscribe_program_logs(ws,'program')

    async def test_subscription_error_cannot_be_reported_as_connected(self):
        ws=mock.AsyncMock()
        ws.recv.return_value=json.dumps({'id':1,'error':{'code':-32005,'message':'sensitive provider details'}})
        with self.assertRaisesRegex(network.SubscriptionRejected,'code=-32005') as error:
            await network.subscribe_program_logs(ws,'program')
        self.assertNotIn('sensitive provider details',str(error.exception))

    async def test_malformed_acknowledgements_are_rejected(self):
        for response in ([],{'id':1,'result':True},{'id':1,'result':-1},{'id':1}):
            ws=mock.AsyncMock()
            ws.recv.return_value=json.dumps(response)
            with self.assertRaises(network.SubscriptionRejected):
                await network.subscribe_program_logs(ws,'program')
        ws.recv.return_value='not JSON'
        with self.assertRaises(network.SubscriptionRejected):
            await network.subscribe_program_logs(ws,'program')

    async def test_clean_server_close_also_rotates_the_provider(self):
        ws=mock.AsyncMock()
        ws.close_code=1000
        with mock.patch.dict('os.environ',{'WS_URLS':'ws://one.example,ws://two.example'},clear=True):
            with mock.patch('websockets.connect',new=mock.AsyncMock(return_value=ws)) as connect:
                async with network.rpc_websocket('ws://one.example'):
                    pass
                ws.close_code=None
                async with network.rpc_websocket('ws://one.example'):
                    pass
        self.assertEqual(connect.await_args.args[0],'ws://two.example')

    async def test_collector_drains_bursts_without_waiting_for_snapshot_disk_writes(self):
        collector=DexBetterLogs.__new__(DexBetterLogs)
        collector.stop_event=asyncio.Event()
        collector.logs=asyncio.Queue()
        collector.subscribed=False
        collector.last_error=None
        collector.current_status='idle'
        collector.publish_snapshot=mock.AsyncMock()
        collector.rpc_endpoint='ws://test.example'
        class Socket:
            async def send(self,payload):
                pass
            async def recv(self):
                return json.dumps({'id':1,'result':42})
            async def __aiter__(self):
                for index in range(1000):
                    yield json.dumps({'event':index})
                collector.stop_event.set()
        @asynccontextmanager
        async def connection(*args,**kwargs):
            yield Socket()
        with mock.patch('DexLab.wsLogs.rpc_websocket',new=connection):
            await collector.subscribe('program')
        self.assertEqual(collector.logs.qsize(),1000)
        collector.publish_snapshot.assert_not_awaited()
        self.assertEqual(collector.current_status,'streaming')

    async def test_slow_price_provider_does_not_block_the_event_loop_or_duplicate_requests(self):
        market=Market.__new__(Market)
        market.sol_price_usd=Decimal('100')
        market._sol_price_refresh_interval_seconds=60
        market._last_sol_price_refresh_monotonic=0
        market._sol_price_refresh_lock=asyncio.Lock()
        release=threading.Event()
        entered=asyncio.Event()
        loop=asyncio.get_running_loop()
        def fetch():
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(2):
                raise AssertionError('event loop could not release the price fetch')
            return Decimal('101')
        market.get_solana_price_usd=mock.Mock(side_effect=fetch)
        refresh=asyncio.create_task(market._refresh_solana_price_if_due())
        try:
            await asyncio.wait_for(entered.wait(),timeout=1)
            # Another mint uses the last price instead of starting another HTTP fetch.
            await market._refresh_solana_price_if_due()
            self.assertFalse(refresh.done())
            self.assertEqual(market.sol_price_usd,Decimal('100'))
        finally:
            release.set()
            await refresh
        self.assertEqual(market.sol_price_usd,Decimal('101'))
        market.get_solana_price_usd.assert_called_once_with()

    async def test_ping_timeout_stays_enabled_and_is_shared_by_all_streams(self):
        ws=mock.AsyncMock()
        ws.close_code=None
        with mock.patch.dict('os.environ',{},clear=True):
            with mock.patch('websockets.connect',new=mock.AsyncMock(return_value=ws)) as connect:
                async with network.rpc_websocket('ws://one.example'):
                    pass
        self.assertEqual(connect.await_args.kwargs['ping_interval'],20)
        self.assertEqual(connect.await_args.kwargs['ping_timeout'],30)
