"""TLS-verified HTTP sessions with both system and Mozilla trust roots."""
import ssl
import asyncio
from functools import lru_cache
from urllib.parse import urlsplit

import aiohttp
import certifi
import os
import httpx
import logging
from contextlib import asynccontextmanager
from solana.exceptions import SolanaRpcException
from solana.rpc.async_api import AsyncClient
from solana.rpc.core import RPCException

logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)


class SubmissionUncertain(RuntimeError):
    def __init__(self, signature):
        self.signature = str(signature)
        super().__init__(f'RPC submission is unresolved; reconcile signature {self.signature} before another send')


@lru_cache(maxsize=1)
def verified_tls_context():
    context = ssl.create_default_context()
    context.load_verify_locations(cafile=certifi.where())
    return context


def create_http_session(**kwargs):
    if 'connector' not in kwargs:
        kwargs['connector'] = aiohttp.TCPConnector(ssl=verified_tls_context())
    return aiohttp.ClientSession(**kwargs)


class RpcPoolClient:
    """Retry transport failures through same-cluster configured RPCs.

    Submission retries reuse the exact signed transaction and signature.
    On-chain/preflight rejections propagate without a retry.
    """
    def __init__(self, endpoint: str):
        configured = [url.strip() for url in os.getenv('HTTP_URLS','').split(',') if url.strip()]
        urls = [endpoint,*configured] if endpoint in configured else [endpoint]
        self._clients = [AsyncClient(url) for url in dict.fromkeys(urls)]
        self._cursor = 0

    def __getattr__(self, name):
        attribute = getattr(self._clients[self._cursor],name)
        if name.startswith('_') or not callable(attribute):
            return attribute
        async def request(*args,**kwargs):
            start = self._cursor
            uncertain = False
            for offset in range(len(self._clients)):
                index = (start+offset)%len(self._clients)
                try:
                    result = await getattr(self._clients[index],name)(*args,**kwargs)
                    self._cursor = index
                    return result
                except (SolanaRpcException,httpx.HTTPError,TimeoutError):
                    uncertain = name == 'send_transaction'
                    if offset+1 == len(self._clients):
                        if uncertain:
                            raise SubmissionUncertain(args[0].signatures[0]) from None
                        raise
                except RPCException:
                    if uncertain:
                        raise SubmissionUncertain(args[0].signatures[0]) from None
                    raise
        return request

    async def close(self):
        import asyncio
        await asyncio.gather(*(client.close() for client in self._clients))

    async def __aenter__(self):
        return self

    async def __aexit__(self,*args):
        await self.close()


_ws_cursors = {}
_ws_logger = logging.getLogger('dexter.websocket')


class SubscriptionRejected(ConnectionError):
    pass


def _ws_seconds(name, default):
    value = float(os.getenv(name, str(default)))
    if not 0 < value < float('inf'):
        raise ValueError(name+' must be finite and positive')
    return value


async def subscribe_program_logs(ws, program):
    """Require an acknowledged subscription before marking the feed connected."""
    import json
    await ws.send(json.dumps({'jsonrpc':'2.0','method':'logsSubscribe','id':1,
        'params':[{'mentions':[program]},{'commitment':'processed'}]}))
    try:
        response = json.loads(await asyncio.wait_for(ws.recv(),
            timeout=_ws_seconds('DEXTER_WS_SUBSCRIBE_TIMEOUT_SECONDS',15)))
    except asyncio.TimeoutError:
        raise SubscriptionRejected('logsSubscribe acknowledgement timed out') from None
    except json.JSONDecodeError:
        raise SubscriptionRejected('logsSubscribe returned an invalid acknowledgement') from None
    if not isinstance(response,dict):
        raise SubscriptionRejected('logsSubscribe returned an invalid acknowledgement')
    if response.get('error'):
        error = response['error']
        code = error.get('code') if isinstance(error,dict) else None
        code = code if isinstance(code,int) else 'unknown'
        raise SubscriptionRejected(f'logsSubscribe rejected (code={code})')
    if (response.get('id') != 1 or not isinstance(response.get('result'),int)
            or isinstance(response['result'],bool) or response['result'] < 0):
        raise SubscriptionRejected('logsSubscribe returned an invalid acknowledgement')
    return response['result']


def _websocket_reason(exc):
    import websockets
    if isinstance(exc,websockets.ConnectionClosed):
        sent,received = exc.sent,exc.rcvd
        if sent and sent.reason == 'keepalive ping timeout':
            return 'keepalive_ping_timeout'
        return f'connection_closed sent={sent.code if sent else "none"} received={received.code if received else "none"}'
    if isinstance(exc,SubscriptionRejected):
        return str(exc)  # Fixed diagnostic text; never contains RPC credentials.
    return type(exc).__name__


async def _observe_websocket_loop(stats):
    loop = asyncio.get_running_loop()
    while True:
        before = loop.time()
        await asyncio.sleep(0.5)
        stats['max_loop_lag'] = max(stats['max_loop_lag'],max(0,loop.time()-before-0.5))


@asynccontextmanager
async def rpc_websocket(endpoint: str, *, label='rpc', **kwargs):
    import websockets
    configured = [url.strip() for url in os.getenv('WS_URLS','').split(',') if url.strip()]
    urls = tuple(dict.fromkeys([endpoint,*configured] if endpoint in configured else [endpoint]))
    start = _ws_cursors.get(urls,0)
    for offset in range(len(urls)):
        index = (start+offset)%len(urls)
        url = urls[index]
        options = dict(kwargs)
        options.setdefault('open_timeout',8)
        options.setdefault('ping_interval',_ws_seconds('DEXTER_WS_PING_INTERVAL_SECONDS',20))
        options.setdefault('ping_timeout',_ws_seconds('DEXTER_WS_PING_TIMEOUT_SECONDS',30))
        options.setdefault('close_timeout',5)
        if url.startswith('wss://'):
            options.setdefault('ssl',verified_tls_context())
        try:
            ws = await websockets.connect(url,**options)
        except (OSError,TimeoutError,websockets.WebSocketException) as exc:
            _ws_logger.warning('WebSocket connect failed stream=%s endpoint=%s reason=%s',
                label,urlsplit(url).hostname,_websocket_reason(exc))
            continue
        _ws_cursors[urls] = index
        started = asyncio.get_running_loop().time()
        stats = {'max_loop_lag':0.0}
        observer = asyncio.create_task(_observe_websocket_loop(stats))
        _ws_logger.info('WebSocket connected stream=%s endpoint=%s provider=%d/%d',
            label,urlsplit(url).hostname,index+1,len(urls))
        try:
            yield ws
            # A clean server close ends async iteration without raising.
            if isinstance(getattr(ws,'close_code',None),int):
                _ws_cursors[urls] = (index+1)%len(urls)
                _ws_logger.warning('WebSocket ended stream=%s endpoint=%s received=%s uptime=%.1fs; reconnecting',
                    label,urlsplit(url).hostname,ws.close_code,asyncio.get_running_loop().time()-started)
        except (OSError,TimeoutError,websockets.WebSocketException) as exc:
            _ws_cursors[urls] = (index+1)%len(urls)
            latency = getattr(ws,'latency',0.0)
            latency = latency if isinstance(latency,(float,int)) else 0.0
            _ws_logger.warning('WebSocket interrupted stream=%s endpoint=%s reason=%s uptime=%.1fs '
                'ping_rtt=%.3fs max_loop_lag=%.3fs; reconnecting',label,urlsplit(url).hostname,
                _websocket_reason(exc),asyncio.get_running_loop().time()-started,latency,stats['max_loop_lag'])
            raise
        finally:
            observer.cancel()
            await asyncio.gather(observer,return_exceptions=True)
            await ws.close()
        return
    raise ConnectionError('All configured WebSocket RPC endpoints failed')
