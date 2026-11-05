from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase, mock

import httpx
from solana.rpc.core import RPCException
from dexter_network import RpcPoolClient, SubmissionUncertain, rpc_websocket


class RpcPoolTests(IsolatedAsyncioTestCase):
    def pool(self, clients):
        pool=RpcPoolClient.__new__(RpcPoolClient)
        pool._clients=clients
        pool._cursor=0
        return pool

    async def test_submission_failover_reuses_identical_signed_transaction(self):
        tx=SimpleNamespace(signatures=['original-signature'])
        first=SimpleNamespace(send_transaction=mock.AsyncMock(side_effect=httpx.ConnectError('connection lost')))
        second=SimpleNamespace(send_transaction=mock.AsyncMock(return_value='accepted'))
        result=await self.pool([first,second]).send_transaction(tx)
        self.assertEqual(result,'accepted')
        self.assertIs(first.send_transaction.await_args.args[0],second.send_transaction.await_args.args[0])

    async def test_exhausted_submission_keeps_original_signature(self):
        tx=SimpleNamespace(signatures=['original-signature'])
        client=SimpleNamespace(send_transaction=mock.AsyncMock(side_effect=httpx.ConnectError('connection lost')))
        with self.assertRaises(SubmissionUncertain) as context:
            await self.pool([client]).send_transaction(tx)
        self.assertEqual(context.exception.signature,'original-signature')

    async def test_program_rejection_does_not_trigger_submission_retry(self):
        client=SimpleNamespace(send_transaction=mock.AsyncMock(side_effect=RPCException('program rejected')))
        backup=SimpleNamespace(send_transaction=mock.AsyncMock())
        with self.assertRaises(RPCException):
            await self.pool([client,backup]).send_transaction(SimpleNamespace(signatures=['signature']))
        backup.send_transaction.assert_not_awaited()

    async def test_devnet_client_does_not_inherit_mainnet_pool(self):
        with mock.patch.dict('os.environ',{'HTTP_URLS':'https://mainnet.example'},clear=True):
            with mock.patch('dexter_network.AsyncClient') as constructor:
                RpcPoolClient('https://api.devnet.solana.com')
        constructor.assert_called_once_with('https://api.devnet.solana.com')

    async def test_websocket_rotates_after_connection_rejection(self):
        ws=mock.AsyncMock()
        with mock.patch.dict('os.environ',{'WS_URLS':'wss://one.example,wss://two.example'},clear=True):
            with mock.patch('websockets.connect',new=mock.AsyncMock(side_effect=[OSError('rate limit'),ws])) as connect:
                async with rpc_websocket('wss://one.example') as connection:
                    self.assertIs(connection,ws)
        self.assertEqual(connect.await_count,2)
        ws.close.assert_awaited_once()
