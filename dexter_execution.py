"""Dexter adapters for the packaged native market binding.

No protocol account layouts, discriminators or fee formulas belong here.
"""
from __future__ import annotations

import base64
from decimal import Decimal
import inspect
import json
import os
from types import SimpleNamespace

import base58
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey as PublicKey
from solders.transaction import VersionedTransaction
from solana.rpc.types import TxOpts

from dexter_mev import build_mev_tip_instruction, submit_signed_transaction_via_mev
from DexLab.utils import DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS


def sdk_client(rpc_url: str):
    try:
        from dexter_sdk import Client
    except ImportError as exc:
        raise RuntimeError(
            "Market bindings are missing. Run python Dexter.py to prepare this checkout."
        ) from exc
    urls = os.getenv("HTTP_URLS", "")
    # A devnet command must not silently fall back to mainnet RPCs.
    urls = urls if rpc_url in [url.strip() for url in urls.split(",")] else rpc_url
    return Client(urls)


def slippage_percent(value) -> float:
    value = Decimal(str(value))
    if not value.is_finite() or value <= 0:
        raise ValueError("slippage must be finite and positive")
    # Preserve Dexter's legacy 1.30 == 30% setting at this boundary.
    percent = (value - 1) * 100 if 1 <= value < 2 else value
    if not 0 < percent < 100:
        raise ValueError("slippage must be between 0 and 100 percent")
    return float(percent)


def decode_instructions(payload) -> list[Instruction]:
    return [Instruction(
        PublicKey.from_string(ix["program_id"]), base64.b64decode(ix["data"], validate=True),
        [AccountMeta(PublicKey.from_string(a["pubkey"]), a["is_signer"], a["is_writable"])
         for a in ix["accounts"]],
    ) for ix in payload]


class NativeMarket:
    def __init__(self, session, rpc_url: str, *, sdk=None):
        self.session = session
        self.rpc_url = rpc_url
        self.sdk = sdk if sdk is not None else sdk_client(rpc_url)

    async def fetch_pool_state(self, pool: str):
        return SimpleNamespace(**await self.sdk.request("pool_state", pool=pool))

    async def find_pool_for_mint(self, mint: str, creator=None):
        try:
            route = await self.sdk.request("route", mint=mint, market_priority="pump_swap")
        except ValueError as exc:
            if "route_unavailable_or_migration_pending" in str(exc):
                return None
            raise
        return await self.fetch_pool_state(route["pool"])

    async def fetch_price_snapshot(self, *, pool=None, pool_state=None):
        result = await self.sdk.request("snapshot", pool=pool or pool_state.pool)
        result["price"] = Decimal(str(result["price"]))
        result["liquidity_sol"] = Decimal(str(result["liquidity_sol"]))
        return result


class NativeExecutor:
    def __init__(self, session, priv_key: str, async_client, mev_config=None, *,
                 market=None, sdk=None, allow_live_send=False, on_route=None):
        self.session = session
        self.async_client = async_client
        self.priv_key = Keypair.from_bytes(base58.b58decode(priv_key))
        self.mev_config = mev_config
        self.sdk = sdk if sdk is not None else (market.sdk if market else sdk_client(str(async_client._provider.endpoint_uri)))
        self.allow_live_send = allow_live_send
        self.on_route = on_route
        self.last_build = None
        self.market_priority = os.getenv("DEXTER_MARKET_PRIORITY", "pump_swap,pump_fun")

    def priority_fee_accounts_for_buy(self, mint_address, bonding_curve_pda=None):
        return [str(self.priv_key.pubkey()), str(mint_address)] + ([str(bonding_curve_pda)] if bonding_curve_pda else [])

    def priority_fee_accounts_for_sell(self, mint_address, bonding_curve_pda=None, **kwargs):
        return self.priority_fee_accounts_for_buy(mint_address, bonding_curve_pda)

    async def get_curve_context(self, bonding_curve, **kwargs):
        state = await self.sdk.request("curve", pool=str(bonding_curve))
        return SimpleNamespace(**state), None

    async def quote_buy(self, mint: str, lamports: int):
        return int((await self.sdk.request("quote_buy", mint=mint, lamports=lamports))["token_amount_raw"])

    async def build(self, mint: str, side: str, *, lamports=None, token_amount_raw=None,
                    sell_pct=100, slippage=1.30, priority_micro_lamports=0):
        params = dict(mint=str(mint), user=str(self.priv_key.pubkey()), side=side,
                      market_priority=self.market_priority, sell_pct=sell_pct,
                      slippage_pct=slippage_percent(slippage),
                      priority_micro_lamports=int(priority_micro_lamports))
        if lamports is not None:
            params["lamports"] = int(lamports)
        if token_amount_raw is not None:
            params["token_amount_raw"] = int(token_amount_raw)
        result = await self.sdk.request("build", **params)
        self.last_build = result
        if self.on_route:
            callback_result = self.on_route(str(mint), result)
            if inspect.isawaitable(callback_result):
                await callback_result
        return result

    async def transaction(self, plan, *, sim=False, additional_instructions=(), additional_signers=()):
        units = int(plan.get("compute_units", DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS))
        if not 1 <= units <= 1_400_000:
            raise ValueError("Dexter SDK plan has an invalid compute limit")
        instructions = [set_compute_unit_limit(units)]
        instructions.append(set_compute_unit_price(int(plan["priority_micro_lamports"])))
        instructions.extend(decode_instructions(plan["instructions"]))
        instructions.extend(additional_instructions)
        if not sim and self.mev_config:
            tip = build_mev_tip_instruction(self.priv_key.pubkey(), self.mev_config)
            if tip:
                instructions.append(tip)
        # Refuse additional signers supplied by a malformed SDK response.
        signer = self.priv_key.pubkey()
        permitted = {signer, *(key.pubkey() for key in additional_signers)}
        if any(a.is_signer and a.pubkey not in permitted for ix in instructions for a in ix.accounts):
            raise ValueError("Dexter SDK plan requires an unexpected signer")
        blockhash = (await self.async_client.get_latest_blockhash(commitment="confirmed")).value.blockhash
        message = MessageV0.try_compile(signer, instructions, [], blockhash)
        return VersionedTransaction(message, [self.priv_key, *additional_signers])

    async def _trade(self, mint, side, *, sim=False, **params):
        if not sim and not self.allow_live_send:
            raise RuntimeError("live sends are disabled for this Dexter executor")
        try:
            plan = await self.build(mint, side, **params)
        except ValueError as exc:
            if "route_unavailable_or_migration_pending" in str(exc):
                return "migration_pending"
            raise
        tx = await self.transaction(plan, sim=sim)
        if sim:
            return await self.async_client.simulate_transaction(tx, sig_verify=True, commitment="confirmed")
        if self.mev_config:
            result = await self.async_client.simulate_transaction(tx, sig_verify=True, commitment="confirmed")
            if result.value.err:
                raise RuntimeError(f"Dexter SDK preflight failed: {result.value.err}")
            return await submit_signed_transaction_via_mev(tx, self.mev_config)
        response = await self.async_client.send_transaction(
            tx, opts=TxOpts(skip_preflight=False, skip_confirmation=True, preflight_commitment="confirmed", max_retries=0))
        return str(response.value)

    async def pump_buy(self, mint_address, bonding_curve_pda, sol_amount, creator=None,
                       token_amount=0, sim=False, priority_micro_lamports=0,
                       slippage=1.30, **kwargs):
        return await self._trade(mint_address, "buy", sim=sim, lamports=sol_amount,
                                 priority_micro_lamports=priority_micro_lamports, slippage=slippage)

    async def pump_sell(self, mint_address, bonding_curve_pda=None, token_amount=None,
                        min_sol_output=0, creator=None, sim=False,
                        priority_micro_lamports=0, slippage=1.30, *, pool=None):
        # PumpSwap's old method takes (mint, amount, *, pool=...).
        amount = bonding_curve_pda if pool is not None else token_amount
        return await self._trade(mint_address, "sell", sim=sim, token_amount_raw=int(amount or 0),
                                 priority_micro_lamports=priority_micro_lamports, slippage=slippage)

    async def close(self):
        # aiohttp and RPC client lifetimes belong to the Dexter runtime.
        pass


DEFAULT_PUMP_FUN_IPFS_UPLOAD_URL = "https://pump.fun/api/ipfs"


class NativeTokenLauncher(NativeExecutor):
    async def pump_create_token(self, *, name, symbol, uri=None, image_path=None,
            description="", twitter=None, telegram=None, website=None, show_name=True,
            upload_url=DEFAULT_PUMP_FUN_IPFS_UPLOAD_URL, buy_sol_lamports=0,
            slippage_pct=15.0, priority_micro_lamports=0, sim=False, send=False):
        if send and not self.allow_live_send:
            raise RuntimeError("live sends are disabled for this Dexter launcher")
        if not uri:
            if not image_path:
                raise ValueError("metadata URI or image is required")
            from pathlib import Path
            import mimetypes
            from aiohttp import FormData
            path = Path(image_path)
            form = FormData()
            for key, value in dict(name=name,symbol=symbol,description=description,
                                   twitter=twitter,telegram=telegram,website=website,
                                   showName=str(show_name).lower()).items():
                if value is not None:
                    form.add_field(key,str(value))
            form.add_field('file',path.read_bytes(),filename=path.name,
                           content_type=mimetypes.guess_type(path.name)[0] or 'application/octet-stream')
            async with self.session.post(upload_url,data=form) as response:
                response.raise_for_status()
                uri = (await response.json())['metadataUri']
        mint = Keypair()
        plan = await self.sdk.request('create',user=str(self.priv_key.pubkey()),mint=str(mint.pubkey()),
            name=name,symbol=symbol,uri=uri,lamports=int(buy_sol_lamports),
            slippage_pct=float(slippage_pct),priority_micro_lamports=int(priority_micro_lamports))
        tx = await self.transaction(plan,sim=True,additional_signers=[mint])
        derived = plan['derived']
        result = {key:value for key,value in plan.items() if key != 'instructions'}
        result.update(derived,metadata_uri=uri,buy_sol_lamports=int(buy_sol_lamports),
                      mode='build',confirmed=False,token_balance_raw=0,
                      transaction=base64.b64encode(bytes(tx)).decode())
        expected = int(plan['expected_tokens_out'])
        result['quote'] = {'expected_tokens_out':expected,'spendable_sol_in':int(buy_sol_lamports)}
        if expected:
            result['expected_buy_price'] = str(Decimal(buy_sol_lamports)/Decimal(1000)/Decimal(expected))
        if sim or send:
            simulation = await self.async_client.simulate_transaction(tx,sig_verify=True,commitment='confirmed')
            result.update(mode='simulate',simulation=json.loads(simulation.to_json()))
            if simulation.value.err or not send:
                return result
        if send:
            response = await self.async_client.send_transaction(tx,opts=TxOpts(
                skip_preflight=False,skip_confirmation=True,preflight_commitment='confirmed',max_retries=0))
            result.update(mode='send',signature=str(response.value))
            confirmation = await self.async_client.confirm_transaction(response.value,commitment='confirmed')
            result['confirmed'] = bool(confirmation.value and confirmation.value[0] and not confirmation.value[0].err)
            if result['confirmed'] and buy_sol_lamports:
                balance = await self.sdk.request('token_balance',mint=plan['mint'],user=plan['owner'])
                result['token_balance_raw'] = int(balance['amount'])
                result['buy_price'] = result.get('expected_buy_price')
        return result
