import asyncio
import aiohttp
import base58
import base64
import json
from typing import Optional
from solders.keypair import Keypair # type: ignore
from solders.transaction import VersionedTransaction # type: ignore
from solders import message
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts
import time, logging, os, sys
from decimal import Decimal

try:
    from DexLab.common_ import *
    from colors import *
    from serializers import *
except ImportError:
    from .common_ import *
    from .colors import *
    from .serializers import *

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
        self.wallet_address = wallet_address
        self.private_key = private_key
        self.api_key = api_key
        self.q_retry = 0
        self.session = aiohttp.ClientSession()  # Persistent session
        self.async_client = AsyncClient(endpoint=self.rpc_endpoint)
        self.dexter = parent
        self.serializer = Interpreters()

    async def fetch_wallet_balance_sol(self):
        headers = {"Content-Type": "application/json"}
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getBalance",
            "params": [
                f"{self.private_key.pubkey()}",
            ]
        }
        async with self.session.post(RPC_URL, json=payload, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                result = data.get('result')
                value = result.get('value')
                logging.info(f"{cc.BRIGHT}{cc.LIGHT_GREEN}Wallet balance: {Decimal(value) / Decimal('1e9')} SOL")
                return value
            else:
                raise Exception(f"HTTP {resp.status}: {await resp.text()}")

    async def close(self):
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

    async def get_swap_tx(self, tx_id: str, mint_token: str, tx_type: str = "buy", max_retries: int = 4, retry_interval: float = 0.2) -> Optional[str]:
        attempt = 0
        backoff = retry_interval
        while attempt < max_retries:
            try:
                await asyncio.sleep(1)  # Initial delay before first attempt
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [
                        tx_id,
                        {
                            "commitment": "confirmed",
                            "encoding": "json",
                            "maxSupportedTransactionVersion": 0
                        }
                    ]
                }
                headers = {
                    "Content-Type": "application/json"
                }

                async with self.session.post(RPC_URL, json=payload, headers=headers, timeout=10) as response:
                    if response.status != 200:
                        logging.error(f"HTTP Error {response.status}: {await response.text()}")
                        raise Exception(f"HTTP Error {response.status}")
    
                    data = await response.json()
                    logging.debug(f"Attempt {attempt + 1}: Received data: {data}")

                    if data and data.get('result') is not None:
                        result = data['result']
                        meta = result.get("meta", {})
                        err = meta.get("err", {})
                        if err is not None and err.get("InstructionError"):
                            logging.info(f"{cc.RED}Instruction error occurred: {err}")
                            await asyncio.sleep(5)
                            return "InstructionError"
                        
                        post_token_balances = meta.get("postTokenBalances", [])
                        post_balances = meta.get("postBalances", [])

                        if tx_type == "buy":
                            for post_token_balance in post_token_balances:
                                if post_token_balance.get("mint") == mint_token:
                                    if post_token_balance.get('owner') == self.wallet_address:
                                        logging.info("Transaction verified.")
                                        token_balance = post_token_balance.get("uiTokenAmount", {}).get("amount")
                                        price = self.process_log(data)
                                        return {"balance": token_balance, "price": price}
                        elif tx_type == "sell":
                            if post_balances:
                                sol_balance = post_balances[0]
                                price = self.process_log(data)
                                return {"balance": sol_balance, "price": price}
                            else:
                                logging.error("No post balances found for sell transaction.")
                                return None
                    else:
                        logging.warning(f"Attempt {attempt + 1}: Transaction result is None.")
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1}: Exception occurred: {e}")

            attempt += 1
            if attempt < max_retries:
                logging.info(f"Retrying in {backoff} seconds...")
                await asyncio.sleep(backoff)
                backoff *= 2
            else:
                logging.error(f"Max retries reached for transaction ID: {tx_id}. Transaction details not found.")
                return {"balance": 0, "price": 0}
        return None

    def process_log(self, data):
        logs = data.get("result", {}).get("meta", {}).get("logMessages", [])
        for log in logs:
            pdidx = log.find("Program data: ")
            if "Program data: " in log:
                log_data = self.serializer.parse_pumpfun_transaction(log[pdidx + len("Program data: "):])
                price = self.get_price(log_data)
                return price
        return None

    def get_price(self, data):
        vsr = Decimal(data.get("virtual_sol_reserves")) / Decimal('1e9')
        vtr = Decimal(data.get("virtual_token_reserves")) / Decimal('1e6')
        return vsr / vtr