import asyncpg
from decimal import Decimal
try:
    from common_ import *
    from colors import *
except ImportError:
    from .common_ import *
    from .colors import *
import logging, time
import asyncio, json, requests, subprocess
import os
import platform, shlex, traceback
from collections import defaultdict

# Change this to your pg_dump path
PG_DUMP_PATH = r"C:\Program Files\PostgreSQL\17\bin\pg_dump.exe" 

def get_pg_dump_path():
    """Return the correct pg_dump path based on the platform."""
    if platform.system() == "Windows":
        return PG_DUMP_PATH
    else:
        return "pg_dump"

logging.basicConfig(
    format=f'{cc.LIGHT_BLUE}[DexLab] %(levelname)s | %(message)s{cc.RESET}',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler()
    ]
)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)

class Market:
    def __init__(self, session, serializer, stop_event,
                 db_dsn="postgres://dexter_user:admin123@127.0.0.1/dexter_db",
                 parent=None):
        self.session = session
        self.serializer = serializer
        self.stop_event = stop_event
        self.db_dsn = db_dsn
        self.parent = parent
        self.mint_monitor_tasks = {}
        self.db_pool = None
        self.sub_second_counters = {}

        self.sol_price_usd = Decimal(self.get_solana_price_usd())
        self.total_supply = Decimal('1000000000')
        self.open_price = Decimal('0.0000000280')
        self.mint_locks = defaultdict(asyncio.Lock)  # One lock per mint_id
        self.count_iter = 0
        self.sema = asyncio.Semaphore(1000)

    async def init_db(self):
        self.db_pool = await asyncpg.create_pool(self.db_dsn, min_size=1, max_size=5000, timeout=60)
        logging.info(f"{cc.WHITE}{cc.BRIGHT}Database connection pool initialized. Dexter is starting...{cc.RESET}")
        async with self.db_pool.acquire() as conn:
            tables = await conn.fetch(
                "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public';"
            )
            if not any(row['tablename'] == 'mints' for row in tables) or \
               not any(row['tablename'] == "stagnant_mints" for row in tables):
                raise RuntimeError("Required tables not found: mints, stagnant_mints.")

    async def close_db(self):
        if self.db_pool:
            await self.db_pool.close()
            logging.info("Database connection pool closed.")

    async def check_integrity(self):
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("SELECT 1;")
            return True
        except Exception as e:
            logging.error(f"Database integrity check failed: {e}")
            return False

    async def store_mint(self, mint_id, data):
        if self.stop_event.is_set():
            logging.warning("Shutdown signal received. Skipping store_mint.")
            return
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO mints 
                    (mint_id, name, symbol, owner, market_cap, price_history, price_usd, liquidity,
                    high_price, low_price, open_price, current_price, age, tx_counts, volume, holders, mint_sig, bonding_curve, created)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                    ON CONFLICT (mint_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    market_cap = EXCLUDED.market_cap,
                    price_history = EXCLUDED.price_history,
                    current_price = EXCLUDED.current_price
                    """,
                    mint_id,
                    data["info"].get("name", ""),
                    data["info"].get("symbol", ""),
                    data.get("owner", ""),
                    float(data.get("market_cap", 0)),
                    json.dumps(data.get("price_history", {}), cls=DecimalEncoder),
                    float(data.get("price_usd", 0)),
                    float(data.get("liquidity", 0)),
                    float(data.get("high_price", 0)),
                    float(data.get("low_price", float("inf"))),
                    float(data.get("open_price", Decimal('0'))),
                    float(data.get("current_price", 0)),
                    float(data.get("age", 0)),
                    json.dumps(data.get("tx_counts", {}), cls=DecimalEncoder),
                    json.dumps({'30sec': {}, '1min': {}, '2min': {}, '5min': {}}, cls=DecimalEncoder),
                    json.dumps(data.get("holders", {}), cls=DecimalEncoder),
                    data.get("mint_sig"),
                    data.get("bonding_curve"),
                    data.get("created"),
                )
        except Exception as e:
            logging.error(f"Error while storing mint: {e}")

    async def update_mint_in_db(self, mint_id, updates):
        try:
            fields = ", ".join([f"{key} = ${i+1}" for i, key in enumerate(updates.keys())])
            values = list(updates.values())
            values.append(mint_id)
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    f"UPDATE mints SET {fields} WHERE mint_id = ${len(values)}",
                    *values
                )
        except Exception as e:
            logging.error(f"Error updating mint in database: {e}")

    async def populate_market(self, program, tx_type, sig, data):
        mint = data.get('mint')
        if not mint:
            logging.error("No mint ID found in transaction data. Skipping update.")
            return
    
        if tx_type == 'mints':
            await self.store_mint(mint, {
                "info": data,
                "owner": data.get('user', ""),
                "market_cap": Decimal('0'), 
                "price_history": {},
                "price_usd": Decimal('0'),
                "liquidity": Decimal('0'),
                "high_price": Decimal('0'),
                "low_price": Decimal('Infinity'),
                "current_price": Decimal('0'),
                "open_price": Decimal('0'),
                "age": 0,
                "tx_counts": {'swaps': 0, 'buys': 0, 'sells': 0},
                "holders": {},
                "mint_sig": sig,
                "bonding_curve": data.get('bonding_curve', ""),
                "created": int(time.time()),
            })
            self.start_monitor_for_mint(mint)

        elif tx_type == 'swaps':
            await self.update_mint(program, mint, data)

    def get_solana_price_usd(self):
        try:
            response = requests.get(
                'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd'
            )
            data = response.json()
            price = data['solana']['usd']
            return price
        except Exception:
            logging.info(f"{cc.RED}Failed to get Solana price from Coingecko{cc.RESET}")
            return '210.11'

    def _decimal_to_str(self, value):
        d = 10
        return f"{value:.{d}f}" if value != Decimal('Infinity') else "Infinity"

    async def update_mint(self, program, mint, data):
        try:
            self.count_iter += 1
            if program == PUMP_FUN:
                lock = self.mint_locks[mint]  # The lock dedicated to this mint
                async with lock:
                    async with self.db_pool.acquire() as conn:
                        row = await conn.fetchrow("SELECT * FROM mints WHERE mint_id = $1", mint)
                        if not row:
                            return

                    try:
                        tx_counts = json.loads(row['tx_counts']) if row['tx_counts'] else {'swaps': 0, 'buys': 0, 'sells': 0}
                        holders = json.loads(row['holders']) if row['holders'] else {}
                        price_history = json.loads(row['price_history']) if row['price_history'] else {}
                        volume = json.loads(row['volume']) if row['volume'] else {
                            '30sec': {}, '1min': {}, '2min': {}, '5min': {}
                        }
                    except json.JSONDecodeError as e:
                        logging.error(f"JSONDecodeError for mint {mint}: {e}")
                        return

                    current_timestamp = int(data["timestamp"])

                    # Determine transaction type and update tx_counts
                    tx_type = "buy" if data.get('is_buy', False) else "sell"
                    tx_counts['swaps'] += 1
                    if tx_type == "buy":
                        tx_counts['buys'] += 1
                    else:
                        tx_counts['sells'] += 1

                    # Compute price
                    vsr = Decimal(data["virtual_sol_reserves"]) / Decimal('1e9')
                    vtr = Decimal(data["virtual_token_reserves"]) / Decimal('1e6')
                    price = self._compute_price(vsr, vtr)

                    # Track price history with unique sub-second timestamp
                    if mint not in self.sub_second_counters:
                        self.sub_second_counters[mint] = (current_timestamp, 0)

                    last_ts, counter = self.sub_second_counters[mint]
                    if current_timestamp == last_ts:
                        counter += 1
                    else:
                        counter = 0

                    self.sub_second_counters[mint] = (current_timestamp, counter)
                    unique_timestamp = f"{current_timestamp}.{counter:03d}"
                    price_history[str(unique_timestamp)] = float(price)

                    # Update holders
                    user = data.get("user", "")
                    if user == "9BgYe7pZybM88PFKuLF6UUMv43jqMa99jaxU4EdbEnq7":
                        logging.info(f"Catched our transaction: {row}\n{data}")
                    token_amount = Decimal(data.get("token_amount", 0)) / Decimal('1e6')
                    if user not in holders:
                        holders[user] = {
                            "balance": float(token_amount),
                            "balance_changes": [{
                                "price_was": float(row['current_price']),
                                "amount": float(token_amount),
                                "type": tx_type,
                                "timestamp": current_timestamp,
                            }]
                        }
                    else:
                        if tx_type == "buy":
                            holders[user]["balance"] += float(token_amount)
                        elif tx_type == "sell":
                            holders[user]["balance"] -= float(token_amount)
                        holders[user]["balance_changes"].append(
                            {
                                "price_was": float(row['current_price']),
                                "amount": float(token_amount),
                                "type": tx_type,
                                "timestamp": current_timestamp,
                            }
                        )

                    if self.count_iter % 100 == 0:
                        self.count_iter = 0
                        logging.info(f"{cc.CYAN}Socket Latency: {round(time.time() - data['timestamp'], 2)}s{cc.RESET}")

                    high_price = float(row['high_price'])
                    low_price = float(row['low_price'])

                    if row['open_price'] == Decimal('0'):
                        open_price = float(price)
                    else:
                        open_price = float(row['open_price'])

                    elapsed_time = int(time.time()) - int(row['created'])

                    if elapsed_time <= 30:
                        volume['30sec'] = tx_counts
                    elif elapsed_time <= 60:
                        volume['1min'] = tx_counts
                    elif elapsed_time <= 120:
                        volume['2min'] = tx_counts
                    elif elapsed_time <= 300:
                        volume['5min'] = tx_counts

                    updates = {
                        "tx_counts": json.dumps(tx_counts, cls=DecimalEncoder),
                        "volume": json.dumps(volume, cls=DecimalEncoder),
                        "holders": json.dumps(holders, cls=DecimalEncoder),
                        "price_history": json.dumps(price_history, cls=DecimalEncoder),
                        "current_price": float(price),
                        "high_price": float(max(high_price, price)),
                        "low_price": float(min(low_price, price)),
                        "open_price": float(open_price),
                        "market_cap": float(self.get_market_cap(price)),
                        "price_usd": float(price * self.sol_price_usd),
                        "liquidity": float((vsr + (vtr * price)) * self.sol_price_usd),
                        "age": float(time.time() - data["timestamp"]) if row['age'] == 0 else float(row['age']),
                    }
                    await self.update_mint_in_db(mint, updates)
        except Exception as e:
            logging.error(f"Error while updating mint: {e}")
            traceback.print_exc()

    def get_market_cap(self, current_price):
        price_in_usd = Decimal(current_price) * self.sol_price_usd
        return self.total_supply * price_in_usd

    def _compute_price(self, vsr, vtr):
        if vtr == 0:
            return Decimal('0')
        return vsr / vtr

    async def create_backup(self):
        backup_path = f"postgres_backup-{int(time.time())}.sql"
        db_name = "dexter_db"
        user = "dexter_user"
        pg_dump_path = get_pg_dump_path()

        if platform.system() == "Windows" and not os.path.isfile(pg_dump_path):
            logging.error(f"pg_dump not found at {pg_dump_path}. Ensure PostgreSQL bin path is correct.")
            return

        command = [
            pg_dump_path,
            "-h", "127.0.0.1",
            "-U", user,
            "-d", db_name,
            "-F", "p",
            "-f", backup_path
        ]
        logging.info(f"Running pg_dump command: {shlex.join(command)}")

        env = os.environ.copy()
        env["PGPASSWORD"] = "admin123"

        try:
            process = await asyncio.create_subprocess_exec(
                *command, env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                logging.info(f"Backup successfully created at {backup_path}")
            else:
                logging.error(f"Backup failed with return code {process.returncode}")
                logging.error(f"STDOUT: {stdout.decode('utf-8').strip()}")
                logging.error(f"STDERR: {stderr.decode('utf-8').strip()}")
        except Exception as e:
            logging.error(f"Error during backup creation: {e}")

    async def move_to_stagnant_mints(self, row_data):
        """
        Insert a mint row into 'stagnant_mints' and remove it from 'mints'.
        """
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    INSERT INTO stagnant_mints (
                        mint_id,
                        name,
                        symbol,
                        owner,
                        holders,
                        price_history,
                        tx_counts,
                        volume,
                        peak_price_change,
                        peak_market_cap,
                        final_market_cap,
                        final_ohlc,
                        mint_sig,
                        bonding_curve,
                        slot_delay
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, 
                        $9, $10, $11, $12, $13, $14, $15
                    )
                    ON CONFLICT (mint_id) DO NOTHING
                """, 
                    row_data["mint_id"],
                    row_data["name"],
                    row_data["symbol"],
                    row_data["owner"],
                    row_data["holders"],
                    row_data["price_history"],
                    row_data["tx_counts"],
                    row_data["volume"],
                    row_data["peak_price_change"],
                    row_data["peak_market_cap"],
                    row_data["final_market_cap"],
                    row_data["final_ohlc"],
                    row_data["mint_sig"],
                    row_data["bonding_curve"],
                    row_data["slot_delay"]
                )

                # Remove from mints
                await conn.execute(
                    "DELETE FROM mints WHERE mint_id = $1",
                    row_data["mint_id"]
                )
                
                if row_data["mint_id"] in self.sub_second_counters:
                    try:
                        self.sub_second_counters.pop(row_data["mint_id"], None)
                    except KeyError:
                        logging.warning(f"Mint ID {row_data['mint_id']} not found in sub_second_counters.")

    async def monitor_single_mint(self, mint_id):
        """
        Monitor a single mint for stagnancy. Stop if the mint is removed
        or if conditions meet for moving it to stagnant_mints.
        """
        peak_pct_change = 0.0
        peak_price = 0
        retry_ = 0
        try:
            while not self.stop_event.is_set():
                async with self.db_pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT * FROM mints WHERE mint_id = $1",
                        mint_id
                    )
                if not row:
                    break

                current_price = Decimal(row["current_price"])
                open_price = Decimal(row["open_price"])
                ph = json.loads(row["price_history"] if row["price_history"] else "{}")

                all_prices = [Decimal(str(val)) for val in ph.values() if val]
                if all_prices and open_price > 0:
                    peak_price = max(all_prices)
                    peak_pct_change = float(((peak_price - open_price) / open_price) * 100)
                
                peak_market_cap = float(self.get_market_cap(peak_price)) if peak_price > 0 else 0.0

                if not ph:
                    retry_ += 1
                    await asyncio.sleep(1)
                    if retry_ >= 20:
                        logging.info(f"Price history not found for mint {mint_id}.")
                        break
                    else:
                        continue

                # last_tx_time is the last key in price_history
                last_tx_time = int(list(ph.keys())[-1].split('.')[0]) if ph else 0
                now = time.time()

                # Check for stagnancy
                no_tx_for_5_min = (now - last_tx_time) > (5 * 60)  # 5 minutes
                price_below_threshold = current_price <= Decimal('0.0000000300')
                price_below_30s = (now - last_tx_time) >= 30 if price_below_threshold else False

                if no_tx_for_5_min or price_below_30s:
                    row_data = {
                        "mint_id": mint_id,
                        "name": row["name"],
                        "symbol": row["symbol"],
                        "owner": row["owner"],
                        "holders": row["holders"],
                        "price_history": row["price_history"],
                        "tx_counts": row["tx_counts"],
                        "volume": row["volume"],
                        "peak_price_change": peak_pct_change,
                        "peak_market_cap": peak_market_cap,
                        "final_market_cap": row["market_cap"],
                        "final_ohlc": json.dumps({"open": row["open_price"], "high": peak_price, "low": row["low_price"], "close": current_price}, cls=DecimalEncoder),
                        "mint_sig": row["mint_sig"],
                        "bonding_curve": row["bonding_curve"],
                        "slot_delay": str(row["age"]),
                    }
                    await self.move_to_stagnant_mints(row_data)
                    break
                await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"Error monitoring mint {mint_id}: {e}")
        finally:
            self.mint_monitor_tasks.pop(mint_id, None)

    def start_monitor_for_mint(self, mint_id):
        if mint_id not in self.mint_monitor_tasks:
            self.mint_monitor_tasks[mint_id] = asyncio.create_task(
                self.monitor_single_mint(mint_id)
            )

    async def shutdown(self):
        logging.info("Shutdown initiated. Waiting for ongoing tasks to complete...")
        for task in list(self.mint_monitor_tasks.values()):
            task.cancel()
        await asyncio.sleep(0.5)
        await self.close_db()
        if self.parent and not self.parent.session.closed:
            await self.parent.session.close()
        logging.info("Database connection pool closed. Shutdown complete.")

    def _get_peak_price(self, price_history):
        prices = [Decimal(str(v)) for v in price_history.values() if v]
        if not prices:
            return Decimal(0)
        return max(prices)