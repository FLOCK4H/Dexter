import websockets
import asyncio
import json
import logging
import aiohttp
import traceback
import os
import signal
import datetime as dt

from dexter_config import ensure_directories, load_config, log_startup_summary, validate_config
from dexter_local_postgres import ensure_local_postgres_running
from dexter_operator import publish_runtime_snapshot

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
    def __init__(self, rpc_endpoint, db_dsn=None, app_config=None, enable_market=True):
        self.logs = asyncio.Queue()
        self.session = aiohttp.ClientSession()
        self.rpc_endpoint = rpc_endpoint
        self.serializer = Interpreters()
        self.mint_data = None
        self.stop_event = asyncio.Event()
        self.app_config = app_config
        self.market = None
        if enable_market:
            if not db_dsn:
                raise ValueError("db_dsn is required when market storage is enabled.")
            self.market = Market(
                self.session,
                self.serializer,
                stop_event=self.stop_event,
                db_dsn=db_dsn,
                parent=self,
                app_config=app_config,
            )
        self.active_tasks = set()
        self.run_tasks = set()
        self.loop = None
        self._shutdown_requested = False
        self._shutdown_sentinel = object()
        self._signal_handlers_registered = False
        self.subscribed = False
        self.processed_logs = 0
        self.last_event_at = None
        self.last_error = None
        self.last_integrity_check_at = None
        self.current_status = "idle"

    async def publish_snapshot(self, *, status: str):
        self.current_status = status
        if not self.app_config:
            return
        publish_runtime_snapshot(
            self.app_config.paths.collector_snapshot_file,
            {
                "component": "collector",
                "status": status,
                "network": self.app_config.runtime.network,
                "subscribed": self.subscribed,
                "processed_logs": self.processed_logs,
                "queue_depth": self.logs.qsize(),
                "last_event_at": self.last_event_at,
                "last_integrity_check_at": self.last_integrity_check_at,
                "last_error": self.last_error,
            },
        )

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        if self._signal_handlers_registered:
            return
        self.loop = asyncio.get_running_loop()
        try:
            for signum in (signal.SIGINT, signal.SIGTERM):
                self.loop.add_signal_handler(signum, lambda signum=signum: self._request_shutdown(signum))
        except NotImplementedError:
            signal.signal(signal.SIGINT, self.handle_exit)
            signal.signal(signal.SIGTERM, self.handle_exit)
        self._signal_handlers_registered = True

    def handle_exit(self, signum, frame):
        """Signal handler for termination."""
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self._request_shutdown, signum)
            return
        self._request_shutdown(signum)

    def _request_shutdown(self, signum=None):
        if self._shutdown_requested:
            return
        self._shutdown_requested = True
        if signum is not None:
            logging.info(f"Signal {signum} received. Shutting down gracefully...")
        else:
            logging.info("Shutdown requested. Shutting down gracefully...")
        self.stop_event.set()
        self.logs.put_nowait(self._shutdown_sentinel)
        for task in list(self.run_tasks):
            if not task.done():
                task.cancel()

    def _is_expected_shutdown_error(self, exc: BaseException) -> bool:
        if not (self.stop_event.is_set() or self._shutdown_requested):
            return False
        if isinstance(
            exc,
            (
                aiohttp.ClientConnectionError,
                ConnectionError,
                TimeoutError,
                websockets.ConnectionClosed,
            ),
        ):
            return True
        message = str(exc).lower()
        return any(
            token in message
            for token in (
                "server disconnected",
                "session is closed",
                "connector is closed",
                "cannot write to closing transport",
            )
        )

    async def subscribe(self, program: str = PUMP_FUN):
        while not self.stop_event.is_set():
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
                        self.subscribed = True
                        self.last_error = None
                        await self.publish_snapshot(status="connected")

                    async for message in ws:
                        if self.stop_event.is_set():
                            break
                        hMessage = json.loads(message)
                        await self.logs.put([hMessage, program])
                        self.last_error = None
                        await self.publish_snapshot(status="streaming")

            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._is_expected_shutdown_error(e):
                    break
                self.subscribed = False
                self.last_error = str(e)
                await self.publish_snapshot(status="error")
                logging.error(f"{cc.RED}Error when subscribing to {program}, {e}{cc.RESET}")
                traceback.print_exc()
            finally:
                if not self.stop_event.is_set():
                    await asyncio.sleep(1)

    async def market_handler(self):
        """
        Consume logs from self.logs asynchronously, spawning a new Task for each
        log so that heavy processing does not block other logs.
        """
        while not self.stop_event.is_set():
            try:
                item = await self.logs.get()
            except asyncio.CancelledError:
                break
            if item is self._shutdown_sentinel:
                break
            log, program = item
            if self.stop_event.is_set():
                break
            if log:
                # Create a task to process this log concurrently
                task = asyncio.create_task(self.handle_single_log(log, program))
                self.active_tasks.add(task)

                # Once the task finishes, remove it from active tasks
                task.add_done_callback(self.active_tasks.discard)

    async def handle_single_log(self, log, program):
        """
        Actual processing of a single log in a separate Task.
        """
        try:
            if self.market is None:
                return
            clean_log = await self.collect(log)
            if clean_log:
                self.processed_logs += 1
                self.last_event_at = dt.datetime.now(dt.timezone.utc).isoformat()
                await self.publish_snapshot(status="processing")
                is_mint = clean_log["is_mint"]
                sig = clean_log["sig"]
                program_data = clean_log["program_data"]
                slot = clean_log["slot"]
                raw_logs = clean_log.get("raw_logs", [])

                for idx, data in program_data.items():
                    if not isinstance(data, dict):
                        logging.debug(
                            "Ignoring undecoded program payload for %s at log index %s: %r",
                            sig,
                            idx,
                            data,
                        )
                        continue
                    raw_event_fingerprint = await self.market.record_raw_event(
                        program,
                        sig,
                        slot,
                        idx,
                        "mints" if is_mint and "bonding_curve" in data else "swaps",
                        is_mint and "bonding_curve" in data,
                        data,
                        raw_logs=raw_logs,
                    )
                    if is_mint and "bonding_curve" in data:
                        await self.market.populate_market(
                            program,
                            "mints",
                            sig,
                            data,
                            last_event_fingerprint=raw_event_fingerprint,
                            last_event_slot=slot,
                        )
                    elif "bonding_curve" in data or "sol_amount" in data:
                        await self.market.populate_market(
                            program,
                            "swaps",
                            sig,
                            data,
                            last_event_fingerprint=raw_event_fingerprint,
                            last_event_slot=slot,
                        )
        except Exception as e:
            if self._is_expected_shutdown_error(e):
                return
            self.last_error = str(e)
            await self.publish_snapshot(status="error")
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
                        "program_data": log_details[1],
                        "raw_logs": result["logs"],
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
                if not isinstance(raw_data, dict):
                    logging.debug(
                        "Skipping undecoded program data for %s at log index %s.",
                        sig,
                        len(program_data),
                    )
                    continue
                program_data[len(program_data)] = raw_data
        return [is_mint, program_data]

    async def monitor_integrity_and_backup(self):
        """Monitor database integrity and create backups periodically."""
        if self.market is None:
            return

        interval_seconds = self.app_config.backup.interval_seconds if self.app_config else 3600
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                pass
            if self.stop_event.is_set():
                break
            integrity_ok = await self.market.check_integrity()
            if not integrity_ok:
                logging.error(f"{cc.RED}Database integrity compromised! Creating emergency backup.")
            else:
                logging.info("Database integrity check passed.")
            self.last_integrity_check_at = dt.datetime.now(dt.timezone.utc).isoformat()
            await self.publish_snapshot(status="monitoring")
            await self.market.create_backup(force=not integrity_ok)
            if self.market.phase2 is not None:
                await self.market.phase2.prune_raw_event_retention()

    async def publish_runtime_snapshot_loop(self):
        if self.app_config is None:
            return
        while not self.stop_event.is_set():
            await self.publish_snapshot(status="running" if not self.stop_event.is_set() else "stopped")
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=5)
            except asyncio.TimeoutError:
                continue

    async def run(self, *, manage_signal_handlers: bool = True):
        """Run the main process."""
        if manage_signal_handlers:
            self.setup_signal_handlers()
        if self.market is None:
            raise RuntimeError("DexBetterLogs.run requires market storage to be enabled.")
        await self.market.init_db()

        try:
            self.run_tasks = {
                asyncio.create_task(self.subscribe(PUMP_FUN), name="collector.subscribe"),
                asyncio.create_task(self.market_handler(), name="collector.market_handler"),
                asyncio.create_task(self.monitor_integrity_and_backup(), name="collector.monitor"),
                asyncio.create_task(self.publish_runtime_snapshot_loop(), name="collector.snapshot"),
            }
            await asyncio.gather(*self.run_tasks)
        except asyncio.CancelledError:
            pass
        finally:
            self.stop_event.set()
            self.logs.put_nowait(self._shutdown_sentinel)
            # Ensure we stop all tasks in flight, then shut the Market down
            for task in list(self.run_tasks):
                if not task.done():
                    task.cancel()
            if self.run_tasks:
                await asyncio.gather(*self.run_tasks, return_exceptions=True)
                self.run_tasks.clear()
            logging.info("Stopping all active log-handling tasks...")
            for t in list(self.active_tasks):
                t.cancel()
            # Wait for them to finish
            if self.active_tasks:
                await asyncio.gather(*self.active_tasks, return_exceptions=True)
                self.active_tasks.clear()

            await self.market.shutdown()
            await self.publish_snapshot(status="stopped")
            logging.info("Gracefully shutting down logging...")

async def _main(mode_override=None, network_override=None):
    config = load_config(mode_override, network_override=network_override)
    ensure_local_postgres_running(config)
    errors, warnings = validate_config(config, "collector")
    if errors:
        raise RuntimeError("; ".join(errors))
    for warning in warnings:
        logging.warning(warning)

    ensure_directories(config)
    log_startup_summary(logging.getLogger("dexter.collector"), config, "collector")

    logs = DexBetterLogs(
        config.rpc.ws_url,
        db_dsn=config.database.dsn,
        app_config=config,
        enable_market=True,
    )
    exit_code = 0
    try:
        await logs.run()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        traceback.print_exc()
        exit_code = 1
    finally:
        logging.info("Gracefully shutting down the program...")
    return exit_code

def run(mode_override=None, network_override=None):
    return asyncio.run(_main(mode_override=mode_override, network_override=network_override))

if __name__ == "__main__":
    raise SystemExit(run())
