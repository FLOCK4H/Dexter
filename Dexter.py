import asyncio
import logging
import os
import sys
try: from DexLab.colors import cc
except: from .DexLab.colors import cc
try: from DexAI.trust_factor import Analyzer
except: from .DexAI.trust_factor import Analyzer
try: from DexLab.common_ import *
except: from .DexLab.common_ import *
import asyncpg, collections
import datetime, time
from decimal import Decimal
import json, traceback, base58
from solders.keypair import Keypair  # type: ignore
import websockets
try: from DexLab.wsLogs import DexBetterLogs
except: from .DexLab.wsLogs import DexBetterLogs
try: from DexLab.pump_fun import PumpFun
except: from .DexLab.pump_fun import PumpFun
try: from DexLab.swaps import SolanaSwaps
except: from .DexLab.swaps import SolanaSwaps
try: from DexLab.utils import lamports_to_tokens, usd_to_lamports, usd_to_microlamports
except: from .DexLab.utils import lamports_to_tokens, usd_to_lamports, usd_to_microlamports
from aiohttp import ClientSession
from settings import *
from solana.rpc.async_api import AsyncClient
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)

DB_DSN = "postgres://dexter_user:admin123@127.0.0.1/dexter_db"

LOG_DIR = 'dev/logs'

os.makedirs(LOG_DIR, exist_ok=True)

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

sys.stdout.reconfigure(encoding='utf-8')

logging.basicConfig(
    format=f'%(asctime)s - {cc.LIGHT_CYAN}Dexter ⚡ | %(message)s{cc.RESET}',
    datefmt='%H:%M:%S',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'dexter.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

ROLLING_WINDOW_SIZE = 5
SINGLE_LOCK = True

BLACKLIST = []
DEX_DIR = os.path.abspath(os.path.dirname(__file__))

def dex_welcome():
    sys.stdout.write(f"""
{cc.CYAN}▓█████▄ ▓█████ ▒██   ██▒▄▄▄█████▓▓█████  ██▀███  
{cc.CYAN}▒██▀ ██▌▓█   ▀ ▒▒ █ █ ▒░▓  ██▒ ▓▒▓█   ▀ ▓██ ▒ ██▒
{cc.CYAN}░██   █▌▒███   ░░  █   ░▒ ▓██░ ▒░▒███   ▓██ ░▄█ ▒
{cc.CYAN}░▓█▄   ▌▒▓█  ▄  ░ █ █ ▒ ░ ▓██▓ ░ ▒▓█  ▄ ▒██▀▀█▄  
{cc.CYAN}░▒████▓ ░▒████▒▒██▒ ▒██▒  ▒██▒ ░ ░▒████▒░██▓ ▒██▒
{cc.CYAN} ▒▒▓  ▒ ░░ ▒░ ░▒▒ ░ ░▓ ░  ▒ ░░   ░░ ▒░ ░░ ▒▓ ░▒▓░
{cc.CYAN} ░ ▒  ▒  ░ ░  ░░░   ░▒ ░    ░     ░ ░  ░  ░▒ ░ ▒░
{cc.CYAN} ░ ░  ░    ░    ░    ░    ░         ░     ░░   ░ 
{cc.CYAN}   ░       ░  ░ ░    ░              ░  ░   ░     
{cc.CYAN} ░  
{cc.CYAN}          𝗕𝘆 𝗙𝗟𝗢𝗖𝗞𝟰𝗛               𝘃2.0{cc.RESET}

{cc.RESET}""")
    sys.stdout.flush()

class Dexter:
    def __init__(self, db_dsn):
        self.db_dsn = db_dsn
        self.pool = None
        self.analyzer = Analyzer(db_dsn)
        self.last_processed_timestamp = None
        self.leaderboard = None
        self.active_sessions = {}
        self.holdings = {}
        self.privkey = Keypair.from_bytes(base58.b58decode(PRIV_KEY))
        self.wallet_balance = 0
        self.wallet = str(self.privkey.pubkey())
        self.dex_dir = DEX_DIR
        self.logs = asyncio.Queue()
        self.stop_event = asyncio.Event()
        self.active_tasks = set()
        self.counter = 0
        self.time_start = 0
        self.updating = False

        self.swap_folder = collections.defaultdict(
            lambda: {
                "state": {},                  # data about the mint: holders, price, etc.
            }
        )
        self.sub_second_counters = {}
        self.mint_locks = collections.defaultdict(asyncio.Lock)  # One lock per mint_id
    
    """
        Market part
    """
    async def handle_market(self):
        while not self.stop_event.is_set():
            log, program = await self.logs.get()
            if log:
                task = asyncio.create_task(self.handle_single_log(log, program))
                self.active_tasks.add(task)

                def _cleanup(_):
                    self.active_tasks.discard(task)
                task.add_done_callback(_cleanup)
    
    async def handle_single_log(self, log, program):
        try:
            clean_log = await self.dexLogs.collect(log, debug=False)
            if clean_log:
                is_mint = clean_log["is_mint"]
                sig = clean_log["sig"]
                program_data = clean_log["program_data"]

                for idx, data in program_data.items():
                    if is_mint and "bonding_curve" in data:
                        await self.process_data("mints", sig, data)
                    elif "bonding_curve" in data or "sol_amount" in data:
                        await self.process_data("swaps", sig, data)
        except Exception as e:
            logging.error(f"{cc.RED}Error in handle_single_log: {e}{cc.RESET}")
            traceback.print_exc()

    async def process_data(self, type, sig, data):
        """
            This method does concurrency control & the sub-second timestamp logic for each swap.
        """
        mint = data.get('mint', None)
        if not mint:
            return

        if self.leaderboard is None:
            return

        lock = self.mint_locks[mint]
        async with lock:
            # MINTS
            if type == "mints":
                owner = data.get("user")
                name = data.get("name")
                bonding_curve = data.get("bonding_curve")

                OIL = (owner in self.leaderboard)

                if self.counter == 0:
                    self.counter = 1
                    logging.info(f"{cc.LIGHT_GRAY}Sample: {mint}, owner: {owner}, created: {time.strftime('%H:%M:%S')}{cc.RESET}")

                if OIL:
                    logging.info(
                        f"Processed Mint: {mint}, owner: {owner}, created: {time.strftime('%H:%M:%S')}, "
                        f"{cc.GREEN}Match{cc.RESET}"
                    )

                # If it's an owner in the leaderboard, set up a session
                if OIL:
                    self.time_start = time.time()
                    logging.info(f"{cc.YELLOW}Creating session for {mint}...{cc.RESET}")

                    if mint not in self.active_sessions:
                        self.swap_folder[mint] = {"name": name, "bonding_curve": bonding_curve}
                        session_task = asyncio.create_task(
                            self.monitor_mint_session(mint, owner)
                        )
                        self.active_sessions[mint] = session_task
                    else:
                        logging.info(f"Session for {mint} already active.")

            # SWAPS
            elif type == "swaps":
                if mint not in self.swap_folder:
                    return

                user = data.get('user')
                sol_amount = data.get('sol_amount')
                token_amount = data.get('token_amount')
                is_buy = data.get('is_buy')
                timestamp = data.get('timestamp')
                vsr = data.get('virtual_sol_reserves')
                vtr = data.get('virtual_token_reserves')

                # Sub-second timestamp logic
                if mint not in self.sub_second_counters:
                    self.sub_second_counters[mint] = (timestamp, 0)
                    counter = 0
                else:
                    last_ts, counter = self.sub_second_counters[mint]
                    if timestamp == last_ts:
                        counter += 1
                    else:
                        counter = 0
                self.sub_second_counters[mint] = (timestamp, counter)

                unique_timestamp = f"{timestamp}.{counter:03d}"

                price = await self._compute_price(vsr, vtr)
                mc = await self._get_market_cap(price)
                price_usd = float(price * self.analyzer.sol_price_usd)

                if "state" not in self.swap_folder[mint]:
                    self.swap_folder[mint]["state"] = {
                        "price": price,
                        "open_price": price,
                        "high_price": price,
                        "mc": mc,
                        "price_usd": price_usd,
                        "last_tx_time": unique_timestamp,
                        "holders": {
                            user: {
                                "balance": int(token_amount),
                                "balance_changes": [{"type": "buy", "price_was": price}]
                            }
                        },
                        "price_history": {unique_timestamp: price},
                        "tx_counts": {"swaps": 1, "buys": 1 if is_buy else 0, "sells": 0 if is_buy else 1},
                        "created": int(time.time()) - int(timestamp)
                    }
                    logging.info(f"Socket Latency: {round(time.time() - data['timestamp'], 2)}s")
                else:
                    st = self.swap_folder[mint]["state"]
                    # Update state

                    st["price"] = price
                    if price > st["high_price"]:
                        st["high_price"] = price
                    st["mc"] = mc
                    st["price_usd"] = price_usd
                    st["last_tx_time"] = unique_timestamp
                    st["tx_counts"]["swaps"] += 1
                    if is_buy:
                        st["tx_counts"]["buys"] += 1
                    else:
                        st["tx_counts"]["sells"] += 1

                    if user not in st["holders"]:
                        st["holders"][user] = {
                            "balance": int(token_amount),
                            "balance_changes": [{"type": "buy" if is_buy else "sell", "price_was": price}]
                        }
                    else:
                        if is_buy:
                            st["holders"][user]["balance"] += int(token_amount)
                        else:
                            st["holders"][user]["balance"] -= int(token_amount)
                        st["holders"][user]["balance_changes"].append(
                            {"type": "buy" if is_buy else "sell", "price_was": price}
                        )

                    st["price_history"][unique_timestamp] = price

    async def load_blacklist(self):
        try:
            os.makedirs(self.dex_dir, exist_ok=True)
            blacklist_path = os.path.join(self.dex_dir, "blacklist.txt")
            if os.path.exists(blacklist_path):
                with open(blacklist_path, "r", encoding="utf-8") as f:
                    BLACKLIST.extend(f.read().splitlines())
            else:
                logging.warning(f"Blacklist file not found at {blacklist_path}.")
        except Exception as e:
            logging.error(f"Error loading blacklist: {e}")

    async def add_to_blacklist(self, owner):
        try:
            BLACKLIST.append(owner)
            os.makedirs(self.dex_dir, exist_ok=True)
            blacklist_path = os.path.join(self.dex_dir, "blacklist.txt")
            with open(blacklist_path, "a", encoding="utf-8") as f:
                f.write(f"{owner}\n")
            logging.info(f"{cc.LIGHT_BLUE}Added {owner} to blacklist.{cc.RESET}")
        except Exception as e:
            logging.error(f"Error adding to blacklist: {e}")

    async def save_result(self, result):
        try:
            os.makedirs("dev", exist_ok=True)
            with open("dev/results.txt", "a", encoding="utf-8") as f:
                f.write(json.dumps(result, indent=2) + "\n")
        except Exception as e:
            logging.error(f"Error saving result: {e}")

    async def init_db_pool(self):
        self.pool = await asyncpg.create_pool(dsn=self.db_dsn, min_size=5, max_size=20)
        logging.info("Connection pool initialized.")

    async def close_db_pool(self):
        if self.pool:
            await self.pool.close()
            logging.info("Connection pool closed.")

    async def subscribe(self, program=PUMP_FUN):
        while not self.stop_event.is_set():
            if self.leaderboard is not None:
                ws = None
                try:
                    async with websockets.connect(WS_URL, ping_interval=2, ping_timeout=15) as ws:
                        await ws.send(json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "method": "logsSubscribe",
                                "id": 1,
                                "params": [
                                    {"mentions": [program]},
                                    {"commitment": "processed"}
                                ]
                            }))
                        response = json.loads(await ws.recv())

                        if 'result' in response:
                            logging.info("Dexter successfully connected to Solana's Network ✔")

                        async for message in ws:
                            if self.stop_event.is_set():
                                break
                            hMessage = json.loads(message)
                            await self.logs.put([hMessage, program])

                except websockets.exceptions.ConnectionClosedError:
                    logging.error(f"{cc.RED}Connection closed when subscribing to {program}.{cc.RESET}")
                    await asyncio.sleep(5)
                except TimeoutError:
                    logging.error(f"{cc.RED}TimeoutError when subscribing to {program}.{cc.RESET}")
                    await asyncio.sleep(5)
                except Exception as e:
                    logging.error(f"{cc.RED}Error when subscribing to {program}, {e}{cc.RESET}")
                    traceback.print_exc()
                finally:
                    if ws:
                        await ws.close()
                    await asyncio.sleep(1)
            else:
                await asyncio.sleep(0.5)

    async def _process_peak_change(self, open_price, peak_price):
        peak_pct_change = 0.0
        if peak_price > 0 and open_price > 0:
            peak_pct_change = float(((peak_price - open_price) / open_price) * 100)
        return peak_pct_change

    def _update_rolling_window(self, rolling_buffer, current_time, price, swaps):
        while rolling_buffer and (current_time - rolling_buffer[0][0]).total_seconds() >= ROLLING_WINDOW_SIZE:
            rolling_buffer.popleft()
        rolling_buffer.append((current_time, price, swaps))

    async def _compute_price(self, vsr, vtr):
        vsr = Decimal(vsr) / Decimal(1e9)
        vtr = Decimal(vtr) / Decimal(1e6)
        if vtr == 0:
            return Decimal('0')
        return vsr / vtr

    async def _get_market_cap(self, current_price):
        price_in_usd = Decimal(current_price) * self.analyzer.sol_price_usd
        return self.analyzer.total_supply * price_in_usd

    def _compute_composite_score(self, rolling_buffer):
        """
            Compute the composite score for a token based on
            price trend and transaction momentum.
        """
        if len(rolling_buffer) < 2:
            return 0
        t0, price0, swaps0 = rolling_buffer[0]
        t1, price1, swaps1 = rolling_buffer[-1]
        dt = (t1 - t0).total_seconds()
        if dt == 0:
            return 0

        price_change_pct = 0
        if price0 > 0:
            price_change_pct = float(((price1 - price0) / price0) * 100)

        swaps_diff = float(swaps1 - swaps0)
        tx_momentum = (swaps_diff / dt) * 10

        raw_score = (PRICE_TREND_WEIGHT * Decimal(price_change_pct)) + \
                    (TX_MOMENTUM_WEIGHT * Decimal(tx_momentum))

        score = max(0, min(100, float(raw_score)))
        return score

    async def _validate_result(self, owner, result):
        if result in ["malicious", "sell>buy"]:
            await self.add_to_blacklist(owner)
            logging.info(f"{cc.RED}Blacklisted {owner} for {result}.{cc.RESET}")
        elif result in ["safe", "stagnant", "drop-time"]:
            logging.info(f"{cc.GREEN}Safe {owner} for {result}.{cc.RESET}")

    async def get_latest_price(self, mint_id: str) -> Decimal:
        """
        Fetch the latest price for a given mint ID from swap folder.
        """
        try:
            if mint_id in self.swap_folder:
                state = self.swap_folder[mint_id].get("state", {})
                price = state.get("price", Decimal('0'))
                return price
            logging.info(f"No price data available for {mint_id}. Returning 0.")
            return Decimal('0')
        except Exception as e:
            logging.error(f"Error fetching latest price for {mint_id}: {e}")
            return Decimal('0')

    async def buy(self, mint_id, trust_level, owner):
        try:
            self.holdings[mint_id] = {}
            bonding_curve = self.swap_folder.get(mint_id, {})["bonding_curve"]

            if not bonding_curve:
                logging.info(f"{cc.RED}Bonding curve not found for {mint_id}.{cc.RESET}")
                self.holdings.pop(mint_id, None)
                return
            
            # Here change buy price
            amount = AMOUNT_BUY_TL_1 if trust_level == 1 else AMOUNT_BUY_TL_2
            lamports = await usd_to_lamports(amount, self.analyzer.sol_price_usd)
            if self.wallet_balance <= lamports:
                self.holdings.pop(mint_id, None)
                logging.info(f"{cc.RED}Insufficient balance for {mint_id}, wallet: {self.wallet_balance}, lamports: {lamports}.{cc.RESET}")
                return
            
            price = await self.get_latest_price(mint_id)
            token_amount = await lamports_to_tokens(lamports, price)
            # 0.1USD fee, 50k compute units
            fee = usd_to_microlamports(BUY_FEE, self.analyzer.sol_price_usd, 50_000) if trust_level == 1 else usd_to_microlamports(BUY_FEE, self.analyzer.sol_price_usd, 50_000)
            slippage = SLIPPAGE_AMOUNT # type: ignore

            tx_id = await self.pump_swap.pump_buy(
                mint_id, 
                bonding_curve,
                lamports,
                owner,
                token_amount,
                False, 
                fee,
                slippage
            )
            if tx_id == "migrated":
                logging.info(f"{cc.RED}Bonding curve migrated for {mint_id}. Ending session.{cc.RESET}")
                self.holdings.pop(mint_id, None)
                return "migrated"
            
            if self.time_start != 0:
                logging.info(f"Full time taken to buy: {time.time() - self.time_start}s")

            if tx_id == "PriceTooHigh":
                logging.info(f"{cc.RED}Price too high for {mint_id}. Ending session.{cc.RESET}")
                self.holdings.pop(mint_id, None)
                return "PriceTooHigh"
            
            self.wallet_balance -= (lamports + (fee / 1e4))
            logging.info(f"{cc.WHITE}Buy at {price:.10f} for {mint_id} (trust_level: {trust_level}).{cc.RESET}")
            await self.save_result({"type": "buy", "mint_id": mint_id, "owner": owner, "price": str(price), "trust_level": trust_level})
            return tx_id
        
        except Exception as e:
            logging.error(f"Buy transaction failed: {e}")
            traceback.print_exc()

    async def sell(self, mint_id, amount, reason, owner, trust_level, buy_price):
        try:
            logging.info(f"{cc.LIGHT_MAGENTA}Initiating sell for {mint_id}.")

            bonding_curve = self.swap_folder.get(mint_id, {})["bonding_curve"]
            if not bonding_curve:
                logging.info(f"{cc.RED}Bonding curve not found for {mint_id}.{cc.RESET}")
                self.holdings.pop(mint_id, None)
                return
            
            fee = usd_to_microlamports(SELL_FEE, self.analyzer.sol_price_usd, 50_000) if trust_level == 1 else usd_to_microlamports(SELL_FEE, self.analyzer.sol_price_usd, 50_000)
            
            if mint_id in self.holdings:
                tx_id_sell = await self.pump_swap.pump_sell(
                    mint_id, 
                    bonding_curve,
                    amount,
                    0,
                    owner,
                    False, 
                    fee
                )
                if tx_id_sell == "migrated":
                    logging.info(f"{cc.RED}Bonding curve migrated for {mint_id} to PumpSwapAMM. Ending session.{cc.RESET}")
                    self.holdings.pop(mint_id, None)
                    return "migrated"

                results = await self.swaps.get_swap_tx(tx_id_sell, mint_id, max_retries=6, tx_type="sell")

                if results == "InstructionError":
                    # We have to sell, let's wait and try again
                    await asyncio.sleep(0.02)
                    return await self.sell(mint_id, amount, reason, owner, trust_level, buy_price)
                
                sol_balance = results.get("balance", 0)
                price = results.get("price", Decimal(0))

                peak_change = await self._process_peak_change(buy_price, price)
                logging.info(f"{cc.LIGHT_MAGENTA}Sold {amount} of {mint_id} with profit {peak_change:.6f}")

                old_balance = self.wallet_balance
                if sol_balance != 0:
                    self.wallet_balance = sol_balance
                logging.info(f"Wallet difference: {old_balance} -> {self.wallet_balance} = {self.wallet_balance - old_balance}")
                self.holdings.pop(mint_id, None)
                await self.save_result({"mint_id": mint_id, "owner": owner, "profit": peak_change, "reason": reason})
                await self._validate_result(owner, reason)

                if sol_balance == 0 or price == 0:
                    logging.info(f"{cc.BLINK}{cc.RED}Sell failed for {mint_id}, increase your priority fee or check if you have sufficient balance.{cc.RESET}")
                    return

        except Exception as e:
            logging.error(f"Sell transaction failed: {e}")
            traceback.print_exc()

    async def set_trust_level(self, creator):
        """
            Trust level is determined by the creator's mint count and median peak market cap.
            If creator has only 1 mint, trust level is 1.
            If creator has median peak market cap >= 50k, trust level is 2.
            If creator has median peak market cap >= 0, trust level is 1.
        """
        trust_level = 0
        if creator["mint_count"] == 1:
            trust_level = 1
        elif creator["median_peak_market_cap"] >= 50000:
            trust_level = 2
        elif creator["median_peak_market_cap"] >= 0:
            trust_level = 1
        return trust_level

    async def monitor_mint_session(self, mint_id, owner):
        logging.info(f"{cc.BLUE}Session started for {mint_id} (owner: {owner}). Monitoring...{cc.RESET}")

        if owner in BLACKLIST or (SINGLE_LOCK and len(self.holdings) > 0) or self.updating:
            logging.info(f"{cc.YELLOW}Owner {owner} is blacklisted, a single lock is enabled, or leaderboard is updating rn. Skipping session.{cc.RESET}")
            self.swap_folder.pop(mint_id, None)
            return

        # Determine trust_level
        trust_level = 0
        creator = self.leaderboard.get(owner)
        trust_level = await self.set_trust_level(creator)
        profit_range = Decimal(str(round(creator["median_success_ratio"], 2)))

        # increments in multiples of 10
        max_target = profit_range * PROFIT_MARGIN
        increments = [PRICE_STEP_UNITS]
        step_unit = PRICE_STEP_UNITS
        val = increments[0] + step_unit
        while val <= max_target:
            increments.append(val)
            val += step_unit

        current_target_step = 0
        to_sell = increments[current_target_step] if increments else Decimal('10')

        last_price = None
        last_price_change_time = datetime.datetime.now(datetime.timezone.utc)
        ref_price_drop = None
        last_buys_count = 0
        last_buys_timestamp = datetime.datetime.now(datetime.timezone.utc)
        last_increment_time = None
        token_balance = 0
        selfBuyPrice = Decimal('0')
        buy_retry = 0
        rolling_buffer = collections.deque()
        skip_if_done = False
        buy_tx_id = ""
        increment_threshold = Decimal('25')

        while True:
            try:
                if mint_id in self.swap_folder:
                    row = self.swap_folder[mint_id].get("state")
                    if not row:
                        await asyncio.sleep(0.1)
                        continue

                    name = self.swap_folder[mint_id]["name"]
                    price = row.get('price', Decimal('0'))
                    db_history = row.get('price_history') or {}
                    tx_counts = row.get('tx_counts') or {}
                    holders = row.get('holders') or {}
                    swaps = tx_counts.get("swaps", 0)
                    buys = tx_counts.get("buys", 0)
                    sells = tx_counts.get("sells", 0)
                    open_price = row.get('open_price', Decimal('0'))
                    peak_price = row.get('high_price', Decimal('0'))

                    # Buy if not already
                    if mint_id not in self.holdings:
                        result = await self.buy(mint_id, trust_level, owner)
                        if result == "PriceTooHigh":
                            logging.info(f"{cc.RED}Ending session for {mint_id} due to high buy price.{cc.RESET}")
                            break
                        buy_tx_id = result
                        continue  # Jump straight to next iteration so we fetch our tx

                    if not skip_if_done:
                        we = holders.get(self.wallet, {})
                        balance = we.get("balance", 0)
                        balance_changes = we.get("balance_changes", [])
                        if balance_changes:
                            for bc in balance_changes:
                                if bc.get("type", "") == "buy":
                                    selfBuyPrice = Decimal(bc.get("price_was", 0))
                                    token_balance = balance
                                    break
                        else:
                            selfBuyPrice = Decimal('0')

                        if buy_retry >= 10:
                            logging.info(f"{cc.RED}Buy retry exceeded for {mint_id}.\nTrying a fallback...{cc.RESET}")
                            results = await self.swaps.get_swap_tx(buy_tx_id, mint_id, tx_type="buy")
                            if not results or isinstance(results, str):
                                self.holdings.pop(mint_id, None)
                                logging.info(f"{cc.BLINK}Buy was not successful :( {mint_id}{cc.RESET}")
                                break
                            token_balance = int(results.get("balance", 0))
                            selfBuyPrice = Decimal(results.get("price", 0))
                            if token_balance <= 0 or selfBuyPrice <= 0:
                                self.holdings.pop(mint_id, None)
                                break
                        if token_balance <= 0 or selfBuyPrice <= 0:
                            buy_retry += 1
                            logging.info(f"{cc.YELLOW}No balance or couldn't get buy price for {mint_id}, retry {buy_retry}.{cc.RESET}")
                            await asyncio.sleep(0.5)
                            continue

                    # personal peak change from buy price
                    selfPeakChange = await self._process_peak_change(selfBuyPrice, peak_price)
                    # global peak
                    peak_pct_change = await self._process_peak_change(open_price, peak_price)

                    current_time = datetime.datetime.now(datetime.timezone.utc)
                    time_since_last_change = (current_time - last_price_change_time).total_seconds()

                    if ref_price_drop is None or peak_price > ref_price_drop:
                        ref_price_drop = peak_price

                    malicious = False
                    if ref_price_drop is not None and price < (ref_price_drop * Decimal('0.5')):
                        malicious = True

                    # Drop-Time
                    if buys > last_buys_count:
                        last_buys_count = buys
                        last_buys_timestamp = datetime.datetime.now(datetime.timezone.utc)
                        
                    time_since_last_buy = (current_time - last_buys_timestamp).total_seconds()
                    is_drop_time = (time_since_last_buy >= DROP_TIME)

                    # Condition priority
                    if sells > buys:
                        condition_level = "sells>buys"
                    if malicious:
                        condition_level = "malicious"
                    elif is_drop_time:
                        condition_level = "drop-time"
                    else:
                        condition_level = "safe"

                    # Adjusted increments so they don't blow up if open->buy was huge
                    open_to_buy_diff = Decimal('0')
                    if open_price > 0 and selfBuyPrice > open_price:
                        open_to_buy_diff = Decimal(
                            ((selfBuyPrice - open_price) / open_price) * 100
                        )

                    if not skip_if_done:
                        if len(increments) > 1 and open_to_buy_diff > 0:
                            personal_range = profit_range - open_to_buy_diff
                            if personal_range < 0:
                                personal_range = Decimal('0')
                            personal_factor = personal_range / open_to_buy_diff if open_to_buy_diff > 0 else Decimal('0')
                            new_increments = [inc for inc in increments if inc <= personal_factor * 100]
                            if new_increments and new_increments != increments:
                                increments = new_increments
                                if current_target_step >= len(increments):
                                    current_target_step = len(increments) - 1
                                if current_target_step < 0:
                                    current_target_step = 0
                            elif not new_increments:
                                increments = [PRICE_STEP_UNITS]
                                current_target_step = 0
                        skip_if_done = True

                    # update rolling window and compute composite
                    self._update_rolling_window(rolling_buffer, current_time, price, swaps)
                    composite_score = self._compute_composite_score(rolling_buffer)

                    # Decide increments
                    next_step_possible = (current_target_step < len(increments) - 1)
                    can_increment = False

                    if condition_level in ["malicious", "drop-time"]:
                        current_target_step = 0
                    elif condition_level == "sells>buys":
                        if composite_score < DECREMENT_THRESHOLD:
                            current_target_step = 0
                    else:
                        if next_step_possible:
                            current_incr = increments[current_target_step]
                            next_incr = increments[current_target_step + 1]
                            difference = next_incr - current_incr
                            half_of_jump = difference * Decimal('0.5')
                            partial_threshold = (current_incr + half_of_jump) if current_incr != increments[0] else half_of_jump

                            if composite_score > increment_threshold:
                                if Decimal(str(selfPeakChange)) >= partial_threshold:
                                    if last_increment_time is None:
                                        can_increment = True
                                    else:
                                        time_since_increment = (current_time - last_increment_time).total_seconds()
                                        if time_since_increment > INCREMENT_COOLDOWN: # increment_cooldown
                                            can_increment = True

                        if can_increment:
                            current_target_step += 1
                            last_increment_time = current_time

                    if current_target_step >= len(increments):
                        current_target_step = len(increments) - 1
                    if current_target_step < 0:
                        current_target_step = 0

                    to_sell = increments[current_target_step]

                    # Sell if selfPeakChange >= to_sell or malicious or drop_time
                    if (Decimal(str(selfPeakChange)) >= to_sell) or malicious or is_drop_time:
                        logging.info(f"""{cc.LIGHT_GREEN}Selling {mint_id} at {str(selfPeakChange)}, is malicious: {malicious}, is drop-time: {is_drop_time}.{cc.RESET}""")
                        await self.sell(mint_id, token_balance, condition_level, owner, trust_level, selfBuyPrice)
                        logging.info(f"{cc.LIGHT_GRAY}Token has been sold, stopping the session.{cc.RESET}")
                        break

                    changed_price = (last_price is None or price != last_price)
                    if changed_price:
                        last_price = price
                        last_price_change_time = current_time
                        logging.info(
                            f"""
                            {cc.MAGENTA}[Updated {mint_id}]
                            {cc.LIGHT_WHITE}Name: {name}
                            {cc.DARK_GRAY}Hist len: {len(db_history)}
                            {cc.LIGHT_CYAN}Price: {price}
                            {cc.LIGHT_MAGENTA}MC: {row.get('mc')}
                            {cc.LIGHT_RED}Peak: {peak_price:.10f} | Open:{open_price:.10f} | Change:{peak_pct_change:.2f}%
                            Cond: {condition_level}, to_sell:{to_sell}%
                            composite_score:{composite_score:.2f}
                            last_tx_diff:{time_since_last_change:.2f}s
                            ourPeakChange: {selfPeakChange}%
                            OpenToBuyDiff: {open_to_buy_diff}
                            Increments: {increments}
                            Buys:{buys} Sells:{sells} Swaps:{swaps}{cc.RESET}
                            """
                        )

                    # Stagnant checks
                    """
                        If price hasn't changed in 30 minutes, sell.
                        If price is < 3e-8 and time since last change > 13s, sell.
                    """
                    if time_since_last_change > 1800:
                        logging.info(f"{cc.YELLOW}{mint_id} stagnant(no price change>30m). Stop.{cc.RESET}")
                        await self.sell(mint_id, token_balance, "stagnant", owner, trust_level, selfBuyPrice)
                        break

                    if price < Decimal('0.0000000300') and time_since_last_change > STAGNANT_UNDER_PRICE: # Stagnant and low price
                        logging.info(f"{cc.YELLOW}{mint_id} stagnant(price<3e-8 & tx>13s). Stop.{cc.RESET}")
                        await self.sell(mint_id, token_balance, "malicious", owner, trust_level, selfBuyPrice)
                        break

                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                logging.info(f"{cc.YELLOW}{mint_id} cancelled.{cc.RESET}")
                break
            except Exception as e:
                logging.info(f"{cc.RED}Error session {mint_id}: {e}{cc.RESET}")
                await asyncio.sleep(1)

        self.active_sessions.pop(mint_id, None)
        self.swap_folder.pop(mint_id, None)
        self.holdings.pop(mint_id, None)
        logging.info(f"{cc.BLUE}Session ended {mint_id}.{cc.RESET}")

    async def update_leaderboard(self):
        while True:
            try:
                if len(self.holdings) > 0:
                    await asyncio.sleep(15)
                    continue

                self.updating = True
                
                await self.load_blacklist()
                
                logging.info(f"{cc.WHITE}Starting leaderboard update...{cc.RESET}")
                await self.analyzer.analyze_market()
                self.leaderboard = await asyncio.to_thread(self.analyzer.process_results_sync, False)

                logging.info(f"{cc.WHITE}Leaderboard updated successfully.{cc.RESET}")
                logging.info(f"{cc.LIGHT_RED}Leaderboard Creators: {len(self.leaderboard)}{cc.RESET}")

                # Sort the leaderboard by performance score
                sorted_leaderboard = sorted(
                    self.leaderboard.items(),
                    key=lambda x: x[1]['performance_score'],
                    reverse=True
                )

                logging.info(f"{cc.LIGHT_CYAN}Top 10 Leaderboard Entries:{cc.RESET}")
                for rank, (creator, entry) in enumerate(sorted_leaderboard[:10], start=1):
                    logging.info(
                        f"{cc.MAGENTA}"
                        f"Rank {rank}: Creator: {creator}, "
                        f"Performance Score: {entry['performance_score']:.2f}, "
                        f"Trust Factor: {entry['trust_factor']:.2f}, "
                        f"Total Mints: {entry['mint_count']}, "
                        f"Total Swaps: {entry['total_swaps']}, "
                        f"Median Market Cap: {entry['median_peak_market_cap']:.2f} USD"
                        f"{cc.RESET}"
                    )

                # Save the sorted leaderboard to a file
                current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
                with open("dev/leaderboard.txt", "a", encoding="utf-8") as file:
                    file.write(f"{current_time}-\n")
                    for creator, entry in sorted_leaderboard:
                        file.write(f"{json.dumps({creator: entry}, indent=2)}\n")

            except Exception as e:
                logging.error(f"Error updating leaderboard: {e}")
                traceback.print_exc()
            self.updating = False
            await asyncio.sleep(LEADERBOARD_UPDATE_INTERVAL * 60) # type: ignore

    async def run(self):
        try:
            dex_welcome()
            self.dexLogs = DexBetterLogs(WS_URL)
            await self.init_db_pool()

            self.session = ClientSession()
            self.async_client = AsyncClient(STAKED_API)

            keypair = PRIV_KEY

            self.pump_swap = PumpFun(
                self.session,
                keypair, 
                async_client=self.async_client, # Preferably staked rpc
            )

            self.swaps = SolanaSwaps(
                self,
                private_key=self.privkey, 
                wallet_address=self.wallet,
                rpc_endpoint=STAKED_API, # Preferably staked rpc
                api_key=RPC_URL
            )

            self.wallet_balance = await self.swaps.fetch_wallet_balance_sol()
            logging.info(f"{cc.CYAN}{cc.BRIGHT}Initialized wallet {self.wallet} with: {self.wallet_balance} SOL")

            await asyncio.gather(
                self.subscribe(),
                self.handle_market(),
                self.update_leaderboard()
            )
        except asyncio.CancelledError:
            await self.close()
        except KeyboardInterrupt:
            await self.close()
        finally:
            for mint_id, task in self.active_sessions.items():
                task.cancel()

    async def close(self):
        try:
            logging.info(f"{cc.CYAN}Closing Dexter...{cc.RESET}")
            await self.pump_swap.close()
            await self.close_db_pool()
            await self.swaps.close()
            await self.swaps.async_client.close()
            await self.dexLogs.session.close()
            self.stop_event.set()
            await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error closing Dexter: {e}")
            traceback.print_exc()

def run():
    dexter = Dexter(DB_DSN)
    asyncio.run(dexter.run())

if __name__ == '__main__':
    dexter = Dexter(DB_DSN)
    asyncio.run(dexter.run())

