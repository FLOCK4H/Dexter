import asyncio
import aiohttp
import base58
import base64
import json
from typing import Optional
from solders.keypair import Keypair # lint: ignore
from solders.transaction import VersionedTransaction # lint: ignore
from solders import message
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts
import time, logging, os, sys
from decimal import Decimal

try:
    from .common_ import *
    from .colors import *

except ImportError:
    from common_ import *
    from colors import *

LOG_DIR = 'dev/logs'

logging.basicConfig(
    format=f'{cc.LIGHT_CYAN}[Dexter] %(levelname)s - %(message)s{cc.RESET}',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'swaps.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class SolanaSwaps:
    def __init__(self, parent, private_key: Keypair, wallet_address: str, rpc_endpoint: str, api_key: str):
        self.rpc_endpoint = rpc_endpoint
        self.swap_url = SWAP_URL
        self.wallet_address = wallet_address
        self.private_key = private_key
        self.api_key = api_key
        self.q_retry = 0
        self.session = aiohttp.ClientSession()  # Persistent session
        self.async_client = AsyncClient(endpoint=self.rpc_endpoint)
        self.dexter = parent

    async def fetch_wallet_balance_sol(self):
        headers = {"Content-Type": "application/json"}
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getBalance",
            "params": [
                f"{WALLET}",
            ]
        }
        async with self.session.post(RPC_URL, json=payload, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                result = data.get('result')
                value = result.get('value')
                logging.info(f"{cc.BRIGHT}{cc.LIGHT_GREEN}| Wallet balance: {Decimal(value) / Decimal('1e9')} SOL")
                return value
            else:
                raise Exception(f"HTTP {resp.status}: {await resp.text()}")

    async def close_session(self):
        await self.session.close()

    async def fetch_json(self, url: str) -> dict:
        """Fetch JSON data asynchronously from a given URL."""
        try:
            async with self.session.get(url, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                logging.debug(f"Fetched data from {url}: {data}")
                return data
        except aiohttp.ClientError as e:
            logging.error(f"HTTP error while fetching {url}: {e}")
            raise
        except asyncio.TimeoutError:
            logging.error(f"Request to {url} timed out.")
            raise

    async def post_json(self, url: str, payload: dict) -> dict:
        """Post JSON data asynchronously to a given URL."""
        try:
            async with self.session.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                logging.debug(f"Posted data to {url}: {data}")
                return data
        except aiohttp.ClientError as e:
            logging.error(f"HTTP error while posting to {url}: {e}")
            raise
        except asyncio.TimeoutError:
            logging.error(f"Post request to {url} timed out.")
            raise
