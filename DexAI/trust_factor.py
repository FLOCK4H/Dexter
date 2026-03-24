import logging
try:
    from settings import *
    from .colors import *
except ImportError:
    from colors import *
import asyncio
import json
from decimal import Decimal
import datetime
import asyncpg
import sys
import os, gc
from pathlib import Path

from dexter_config import ensure_directories, load_config, log_startup_summary, validate_config
from dexter_logging import configure_root_logging
from dexter_price import get_solana_price_usd as load_solana_price_usd
from dexter_time import normalize_unix_timestamp

configure_root_logging(
    format=f'{cc.LIGHT_CYAN}[DexAI] %(levelname)s | %(message)s{cc.RESET}',
    level=logging.INFO,
    log_file=Path("dev/logs/dexter.log"),
    stream=sys.stdout,
)

def get_solana_price_usd():
    return str(load_solana_price_usd(logger=logging.getLogger(__name__)))

class Analyzer:
    def __init__(self, db_dsn):
        self.db_dsn = db_dsn
        self.total_supply = Decimal('1000000000')
        self.sol_price_usd = Decimal(get_solana_price_usd())
        self.seen_mints = set()
        self.top_creators = {}

    def reset_analysis_state(self):
        self.seen_mints = set()
        self.top_creators = {}

    def _new_creator_stats(self):
        return {
            "mint_count": 0,
            "peak_market_caps": [],
            "peak_prices": [],
            "open_prices": [],
            "final_market_caps": [],
            "creation_times": [],
            "total_swaps": 0,
            "success_count": 0,
            "unsuccess_count": 0,
            "success_ratios": [],
            "launch_outcomes": [],
            "rug_count": 0,
            "migration_count": 0,
            "holder_wallet_counts": {},
        }

    def _median_launch_gap_seconds(self, creation_times):
        ordered = sorted(float(ts) for ts in creation_times if ts)
        if len(ordered) < 2:
            return 0
        gaps = [ordered[index] - ordered[index - 1] for index in range(1, len(ordered)) if ordered[index] > ordered[index - 1]]
        return self._calculate_median(gaps) if gaps else 0

    def _failure_cluster_ratio(self, success_flags):
        if not success_flags:
            return 0.0
        longest_failure_run = 0
        current_failure_run = 0
        for is_success in success_flags:
            if is_success:
                current_failure_run = 0
                continue
            current_failure_run += 1
            if current_failure_run > longest_failure_run:
                longest_failure_run = current_failure_run
        return longest_failure_run / len(success_flags)

    def _wallet_reuse_ratio(self, holder_wallet_counts, mint_count):
        if mint_count < 2:
            return None
        unique_wallets = len(holder_wallet_counts or {})
        if unique_wallets <= 0:
            return None
        reused_wallets = sum(1 for appearances in holder_wallet_counts.values() if int(appearances or 0) > 1)
        return reused_wallets / unique_wallets

    def _format_metric(self, value, *, decimals=2, suffix=""):
        if value is None:
            return "n/a"
        return f"{float(value):.{decimals}f}{suffix}"

    async def load_data(self, chunk_size=1000, offset=0):
        data = []
        
        conn = await asyncpg.connect(self.db_dsn)
        try:
            async with conn.transaction(isolation='repeatable_read'):
                rows = await conn.fetch(
                    """
                    SELECT
                        mint_id,
                        owner,
                        holders,
                        price_history,
                        tx_counts,
                        peak_market_cap,
                        final_market_cap,
                        final_ohlc
                    FROM stagnant_mints
                    ORDER BY mint_id
                    LIMIT $1 OFFSET $2
                    """,
                    chunk_size,
                    offset,
                )

            for row in rows:
                price_history = json.loads(row['price_history']) if row['price_history'] else {}
                creation_time_list = sorted(price_history.keys(), key=lambda x: float(x)) if price_history else []
                creation_time = creation_time_list[0] if creation_time_list else None

                record = {
                    "mint_id": row['mint_id'],
                    "owner": row['owner'],
                    "holders": json.loads(row['holders']) if row['holders'] else {},
                    "price_history": price_history,
                    "tx_counts": json.loads(row['tx_counts']) if row['tx_counts'] else {},
                    "peak_market_cap": row['peak_market_cap'],
                    "final_market_cap": row['final_market_cap'],
                    "final_ohlc": json.loads(row['final_ohlc']) if row['final_ohlc'] else {},
                    "creation_time": creation_time,
                }
                data.append(record)
        finally:
            await conn.close()
        
        return data

    def _get_peak_price(self, price_history):
        prices = [Decimal(str(v)) for v in price_history.values() if v]
        if not prices:
            return Decimal(0)
        return max(prices)

    def get_market_cap(self, current_price):
        price_in_usd = Decimal(current_price) * self.sol_price_usd
        return self.total_supply * price_in_usd

    def _calculate_median(self, values):
        values = sorted([float(v) for v in values if v])
        if not values:
            return 0
        n = len(values)
        return values[n // 2] if n % 2 == 1 else (values[n // 2 - 1] + values[n // 2]) / 2

    def _format_timestamp(self, ts):
        dt = datetime.datetime.fromtimestamp(
            normalize_unix_timestamp(ts),
            datetime.UTC,
        )
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    def _format_small_number(self, num):
        f_num = float(num)
        return f"{f_num:.10f}".rstrip('0').rstrip('.') if f_num > 0 else "0.0"

    def _is_successful_mint(self, token_info):
        """
        Determine if a mint is successful based on criteria:
        0. Sniping price = Price closest to X second after the first transaction.
        1. Highest price at least 150% of sniping price.
        2. The highest price was reached after at least 25 swaps.
        """
        price_history = token_info["price_history"]
        if not price_history:
            return False, 0

        # Get sorted timestamps and corresponding prices
        timestamps = sorted(price_history.keys(), key=lambda x: float(x))
        prices = [Decimal(str(price_history[t])) for t in timestamps]

        if not timestamps or not prices:
            return False, 0

        # 0: Calculate the sniping price
        first_timestamp = float(timestamps[0])
        target_timestamp = first_timestamp + float(SNIPING_PRICE_TIME)  # 1 second after the first transaction

        # Find the closest timestamp to the target
        closest_index = min(range(len(timestamps)), key=lambda i: abs(float(timestamps[i]) - target_timestamp))
        sniping_price = prices[closest_index]

        # 1: Check if highest price is at least 150% of the sniping price
        peak_price = Decimal(str(token_info["high_price"]))
        if peak_price < sniping_price * Decimal(f"{SNIPE_PRICE_TO_PEAK_PRICE_RATIO}"):
            return False, 0

        # 2: Ensure the highest price was reached after at least 100 swaps
        try:
            peak_index = next(i for i, p in enumerate(prices) if p == peak_price)
        except StopIteration:
            return False, 0

        if peak_index < HIGHEST_PRICE_MIN_SWAPS:
            return False, 0

        # Calculate the profit ratio based on sniping price
        ratio = float((peak_price - sniping_price) / sniping_price) * 100

        return True, ratio

    def analyze_top_creators_sync(self, data):
        """
        Incrementally analyzes creators and updates metrics.
        """
        for record in data:
            price_history = record["price_history"]
            ohlc = record["final_ohlc"] if record["final_ohlc"] else {}
            peak_price = ohlc.get("high", 0) if ohlc else 0
            peak_market_cap = record["peak_market_cap"] if record["peak_market_cap"] else 0
            creator = record.get("owner", "unknown")

            # Skip already processed mints
            if record['mint_id'] in self.seen_mints:
                continue

            self.seen_mints.add(record['mint_id'])

            creator_stats = self.top_creators.setdefault(creator, self._new_creator_stats())
            creator_stats["mint_count"] += 1

            if peak_market_cap:
                creator_stats["peak_market_caps"].append(peak_market_cap)

            open_price = ohlc.get("open", 0) if ohlc else 0
            if open_price:
                creator_stats["open_prices"].append(open_price)

            if peak_price:
                creator_stats["peak_prices"].append(peak_price)

            final_market_cap = record["final_market_cap"] if record["final_market_cap"] else 0
            if final_market_cap:
                creator_stats["final_market_caps"].append(final_market_cap)

            creation_time_value = None
            creation_time = record.get("creation_time")
            if creation_time:
                try:
                    creation_time_value = float(str(creation_time).split(".")[0])
                    creator_stats["creation_times"].append(creation_time_value)
                except ValueError:
                    logging.warning("Skipping malformed creation_time for %s: %s", record["mint_id"], creation_time)

            creator_stats["total_swaps"] += int(record["tx_counts"].get("swaps", 0) or 0)

            is_successful, ratio = self._is_successful_mint(
                {
                    "price_history": price_history,
                    "high_price": peak_price,
                }
            )
            if is_successful:
                creator_stats["success_count"] += 1
                creator_stats["success_ratios"].append(ratio)
            else:
                creator_stats["unsuccess_count"] += 1

            if peak_market_cap and final_market_cap and final_market_cap <= (float(peak_market_cap) * 0.2):
                creator_stats["rug_count"] += 1

            holder_wallets = {
                str(wallet)
                for wallet in (record.get("holders") or {}).keys()
                if wallet
            }
            for wallet in holder_wallets:
                creator_stats["holder_wallet_counts"][wallet] = int(
                    creator_stats["holder_wallet_counts"].get(wallet, 0)
                ) + 1

            creator_stats["launch_outcomes"].append(
                {
                    "creation_time": creation_time_value,
                    "successful": bool(is_successful),
                }
            )

    def process_results_sync(self, show_result=True):
        return self.process_results(show_result=show_result)

    async def analyze_market(self):
        chunk_size = 250
        offset = 0
        self.reset_analysis_state()
        logging.info("Loading data from database in chunks...")

        while True:
            data_chunk = await self.load_data(chunk_size=chunk_size, offset=offset)
            if not data_chunk:
                break

            self.analyze_top_creators_sync(data_chunk)

            del data_chunk
            gc.collect()

            offset += chunk_size

        logging.info("All data processed. Market analysis completed.")

    def process_results(self, show_result=True):
        leaderboard = {}
        excluded_creator_count = 0

        logging.info(f"Creators in the database: {len(self.top_creators)}")

        for creator, data in self.top_creators.items():
            
            mint_count = data['mint_count']
            median_peak_mc = self._calculate_median(data.get("peak_market_caps", []))
            median_open_prices = self._calculate_median(data.get("open_prices", []))
            median_high_prices = self._calculate_median(data.get("peak_prices", []))
            median_market_caps = self._calculate_median(data.get("final_market_caps", []))
            total_swaps = int(data.get('total_swaps', 0) or 0)

            # Here is the main condition to set, this makes the algorithm choose best creators, tweak it to your liking
            if (
                (mint_count >= 2 and median_peak_mc >= float(MEDIAN_PEAK_MC_ABOVE_2_MINTS) and total_swaps >= TOTAL_SWAPS_ABOVE_2_MINTS) or # Owner has more than 2 mints, median peak market cap is at least 7500$, and total swaps are at least 5
                (mint_count >= 1 and median_peak_mc >= float(MEDIAN_PEAK_MC_1_MINT) and total_swaps >= TOTAL_SWAPS_1_MINT) # Owner has 1 mint, median peak market cap is at least 7000$, and total swaps are at least 5
            ): 
                success_count = int(data.get("success_count", 0) or 0)
                unsuccess_count = int(data.get("unsuccess_count", 0) or 0)
                success_ratios = data.get("success_ratios", [])

                total_mints = success_count + unsuccess_count
                trust_factor = success_count / total_mints

                #2 If trust_factor < X, consider creator unsuccessful => exclude
                if total_mints == 0 or trust_factor < float(TRUST_FACTOR_RATIO):
                    continue

                avg_success_ratio = (sum(success_ratios) / success_count) if success_count > 0 else 0.0
                median_success_ratio = self._calculate_median(success_ratios)
                median_launch_gap_seconds = (
                    self._median_launch_gap_seconds(data.get("creation_times", []))
                    if mint_count >= 2 else None
                )
                ordered_launch_outcomes = sorted(
                    data.get("launch_outcomes", []),
                    key=lambda item: (
                        float(item.get("creation_time"))
                        if item.get("creation_time") not in (None, "")
                        else float("inf")
                    ),
                )
                ordered_success_flags = [bool(item.get("successful")) for item in ordered_launch_outcomes]
                failure_cluster_ratio = (
                    self._failure_cluster_ratio(ordered_success_flags)
                    if total_mints >= 2 else None
                )
                rug_ratio = (
                    float(data.get("rug_count", 0) or 0) / total_mints
                    if total_mints > 0 else 0.0
                )
                migration_ratio = (
                    float(data.get("migration_count", 0) or 0) / total_mints
                    if total_mints > 0 else 0.0
                )
                wallet_reuse_ratio = self._wallet_reuse_ratio(
                    data.get("holder_wallet_counts", {}),
                    mint_count,
                )

                performance_score = (
                    mint_count 
                    * median_peak_mc
                    * median_success_ratio
                    / (median_open_prices if median_open_prices > 0 else 1)
                )

                if (unsuccess_count > 0):
                    creation_times = sorted(data.get("creation_times", []))
                    too_close = any(
                        (creation_times[i] - creation_times[i - 1]) < 900
                        for i in range(1, len(creation_times))
                    )
                    if too_close:
                        excluded_creator_count += 1
                        continue

                leaderboard[creator] = {
                    "mint_count": mint_count,
                    "median_peak_market_cap": median_peak_mc,
                    "median_market_cap": median_market_caps,
                    "median_open_price": median_open_prices,
                    "median_high_price": median_high_prices,
                    "performance_score": performance_score,
                    "trust_factor": trust_factor,
                    "avg_success_ratio": avg_success_ratio,
                    "median_success_ratio": median_success_ratio,
                    "success_count": success_count,
                    "unsuccess_count": unsuccess_count,
                    "total_swaps": total_swaps,
                    "median_launch_gap_seconds": median_launch_gap_seconds,
                    "launch_cadence_per_hour": (
                        3600 / median_launch_gap_seconds
                        if median_launch_gap_seconds not in (None, 0)
                        else None
                    ),
                    "failure_cluster_ratio": failure_cluster_ratio,
                    "rug_ratio": rug_ratio,
                    "migration_ratio": migration_ratio,
                    "wallet_reuse_ratio": wallet_reuse_ratio,
                    "migration_success_ratio": max(0.0, 1.0 - migration_ratio),
                    "wallet_reuse_score": wallet_reuse_ratio if wallet_reuse_ratio is not None else 0.0,
                }

        if show_result:
            logging.info(f"\n{cc.MAGENTA}Leaderboard (Top Performers):{cc.RESET}")
            for rank, (leaderboard_creator, entry) in enumerate(
                sorted(
                    leaderboard.items(),
                    key=lambda item: item[1]["performance_score"],
                    reverse=True,
                ),
                start=1,
            ):
                logging.info(f"Rank {rank}:")
                logging.info(f"  Creator: {leaderboard_creator}")
                logging.info(f"  Mint Count: {entry['mint_count']}")
                logging.info(f"  Successful Mints: {entry['success_count']}")
                logging.info(f"  Unsuccessful Mints: {entry['unsuccess_count']}")
                logging.info(f"  Total Swaps: {entry['total_swaps']}")
                logging.info(f"  Median Peak Market Cap: {entry['median_peak_market_cap']:.2f}$")
                logging.info(f"  Median Market Cap: {entry['median_market_cap']:.2f}$")
                logging.info(f"  Median Open Price: {entry['median_open_price']:.10f} SOL")
                logging.info(f"  Median High Price: {entry['median_high_price']:.10f} SOL")
                logging.info(f"  Median Success Ratio (Peak/Open): {entry['median_success_ratio']:.2f}")
                logging.info(f"  Average Success Ratio (Peak/Open): {entry['avg_success_ratio']:.2f}")
                logging.info(f"  Trust Factor: {entry['trust_factor']:.2f}")
                logging.info(f"  Performance Score: {entry['performance_score']:.2f}\n")
                logging.info(f"  Median Launch Gap Seconds: {self._format_metric(entry['median_launch_gap_seconds'])}")
                logging.info(f"  Failure Cluster Ratio: {self._format_metric(entry['failure_cluster_ratio'])}")
                logging.info(f"  Rug Ratio: {entry['rug_ratio']:.2f}")
                logging.info(f"  Wallet Reuse Ratio: {self._format_metric(entry['wallet_reuse_ratio'])}")

        if excluded_creator_count > 0:
            logging.info(
                f"{cc.YELLOW}Excluded {excluded_creator_count} creators from the leaderboard.{cc.RESET}"
            )

        return leaderboard  # Return the leaderboard list

if __name__ == "__main__":
    config = load_config()
    errors, warnings = validate_config(config, "analyze")
    if errors:
        raise RuntimeError("; ".join(errors))
    for warning in warnings:
        logging.warning(warning)

    ensure_directories(config)
    log_startup_summary(logging.getLogger("dexter.analyze"), config, "analyze")
    analyzer = Analyzer(config.database.dsn)
    asyncio.run(analyzer.analyze_market())
    analyzer.process_results()
