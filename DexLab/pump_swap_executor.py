"""Compatibility adapter; all PumpSwap instructions are built by the native binding."""
from dexter_execution import NativeExecutor


class PumpSwapExecutor(NativeExecutor):
    def __init__(self, market, priv_key, async_client, mev_config=None, *,
                 allow_live_send=False, on_route=None):
        super().__init__(market.session,priv_key,async_client,mev_config,market=market,
                         allow_live_send=allow_live_send,on_route=on_route)
