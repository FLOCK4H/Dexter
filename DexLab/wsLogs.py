import websockets
import asyncio
import json
import logging
import aiohttp
import traceback
import os, sys, time
import signal

try:
    from serializers import *
    from common_ import *
    from colors import *
    from market import Market
except ImportError:
    from .common_ import *
    from .market import Market
    from .colors import *
    from .serializers import *


logging.basicConfig(
    format=f'{cc.LIGHT_BLUE}[DexLab] %(levelname)s | %(message)s{cc.RESET}',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler()
    ]
)

class DexBetterLogs:
    def __init__(self, rpc_endpoint):
        self.logs = asyncio.Queue()
        self.session = aiohttp.ClientSession()
        self.rpc_endpoint = rpc_endpoint
        self.serializer = Interpreters()
        self.mint_data = None
        self.stop_event = asyncio.Event()
        self.market = Market(self.session, self.serializer, stop_event=self.stop_event, parent=self)
        self.active_tasks = set()

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.handle_exit)   # Ctrl+C
        signal.signal(signal.SIGTERM, self.handle_exit)  # Termination signal

    def handle_exit(self, signum, frame):
        """Signal handler for termination."""
        logging.info(f"Signal {signum} received. Shutting down gracefully...")
        self.stop_event.set()
        time.sleep(1)
        sys.exit(0)

    async def subscribe(self, program: str = PUMP_FUN):
        while not self.stop_event.is_set():
            ws = None
            try:
                async with websockets.connect(self.rpc_endpoint, ping_interval=5, ping_timeout=15) as ws:
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
                        logging.info(f"{cc.LIGHT_GRAY}Subscribed to logs successfully!{cc.RESET}")

                    async for message in ws:
                        if self.stop_event.is_set():
                            break
                        hMessage = json.loads(message)
                        await self.logs.put([hMessage, program])

            except Exception as e:
                logging.error(f"{cc.RED}Error when subscribing to {program}, {e}{cc.RESET}")
                traceback.print_exc()
            finally:
                # Safely close if not None
                if ws:
                    await ws.close()
                await asyncio.sleep(1)

    async def market_handler(self):
        """
        Consume logs from self.logs asynchronously, spawning a new Task for each
        log so that heavy processing does not block other logs.
        """
        while not self.stop_event.is_set():
            log, program = await self.logs.get()
            if log:
                # Create a task to process this log concurrently
                task = asyncio.create_task(self.handle_single_log(log, program))
                self.active_tasks.add(task)

                # Once the task finishes, remove it from active tasks
                def _cleanup(_):
                    self.active_tasks.discard(task)
                task.add_done_callback(_cleanup)

    async def handle_single_log(self, log, program):
        """
        Actual processing of a single log in a separate Task.
        """
        try:
            clean_log = await self.collect(log)
            if clean_log:
                is_mint = clean_log["is_mint"]
                sig = clean_log["sig"]
                program_data = clean_log["program_data"]

                for idx, data in program_data.items():
                    if is_mint and "bonding_curve" in data:
                        await self.market.populate_market(program, "mints", sig, data)
                    elif "bonding_curve" in data or "sol_amount" in data:
                        await self.market.populate_market(program, "swaps", sig, data)
        except Exception as e:
            logging.error(f"{cc.RED}Error in handle_single_log: {e}{cc.RESET}")
            traceback.print_exc()

    async def collect(self, log, debug=True):
        try:
            result = await self.process_log(log)
            if result:
                if result["err"]:
                    return None
                log_details = await self.validate(result["logs"], result["signature"], debug)
                if log_details:
                    return {
                        "sig": result['signature'],
                        "slot": result['slot'],
                        "is_mint": log_details[0],
                        "program_data": log_details[1]
                    }
        except Exception as e:
            logging.error(f"{cc.RED}Error when processing log, {e}{cc.RESET}")
            traceback.print_exc()

    async def process_log(self, message):
        if 'params' in message:
            if 'result' in message['params']:
                data = message.get('params', {}).get('result', {})
                slot = data.get('context', {}).get('slot', 0)
                val = data.get('value', {})
                logs = val.get('logs', [])
                sig = val.get('signature', "")
                err = val.get('err', {})
                return {"slot": slot, "logs": logs, "signature": sig, "err": err}
        return None

    async def validate(self, log_list, sig, debug=True):
        is_mint, program_data = False, {}
        for log in log_list:
            if "InitializeMint" in log:
                if debug:
                    logging.info(f"{cc.LIGHT_MAGENTA}New mint found! {sig} {cc.RESET}")
                is_mint = True
            elif "Program data" in log:
                idx = log.find("Program data: ")
                raw_data = log[idx + len("Program data: "):]
                if raw_data.startswith("G3K"):
                    raw_data = self.serializer.parse_pumpfun_creation(raw_data)
                elif raw_data.startswith("vdt"):
                    raw_data = self.serializer.parse_pumpfun_transaction(raw_data)
                program_data[len(program_data)] = raw_data
        return [is_mint, program_data]

    async def monitor_integrity_and_backup(self):
        """Monitor database integrity and create backups periodically."""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            if not await self.market.check_integrity():
                logging.error(f"{cc.RED}Database integrity compromised! Creating emergency backup.")
            else:
                logging.info("Database integrity check passed.")
            await self.market.create_backup()
            await asyncio.sleep(3600)  # Run every hour

    async def run(self):
        """Run the main process."""
        self.setup_signal_handlers()
        await self.market.init_db()

        try:
            await asyncio.gather(
                self.subscribe(PUMP_FUN),
                self.market_handler(),
                self.monitor_integrity_and_backup()
            )
        finally:
            # Ensure we stop all tasks in flight, then shut the Market down
            logging.info("Stopping all active log-handling tasks...")
            for t in self.active_tasks:
                t.cancel()
            # Wait for them to finish
            await asyncio.gather(*self.active_tasks, return_exceptions=True)

            await self.market.shutdown()
            logging.info("Gracefully shutting down logging...")
            sys.exit(0)

async def _main():
    logs = DexBetterLogs(WS_URL)
    try:
        await logs.run()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        logging.info("Gracefully shutting down the program...")
        sys.exit(0)

if __name__ == "__main__":
    asyncio.run(_main())
