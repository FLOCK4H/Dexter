"""Explicit, bounded mainnet buy/sell smoke test through the native SDK.

Defaults to simulation. The USD ledger is shared across markets and counts
fees, rent and trading loss. A signed transaction's signature is journaled
before submission; an uncertain send blocks subsequent tests.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
from decimal import Decimal, ROUND_DOWN
import json
import os
import re
from pathlib import Path
import time

import aiohttp
from dexter_network import RpcPoolClient as AsyncClient
from solana.rpc.types import TxOpts

from dexter_config import load_config
from dexter_execution import NativeExecutor, NativeMarket
from dexter_network import create_http_session

MAINNET_GENESIS = '5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d'
EXIT_RESERVE_LAMPORTS = 3_000_000


def safe_error(error):
    from dexter_config import redact_url
    return re.sub(r'https?://[^\s\'"<>]+',lambda match:redact_url(match.group()),str(error))


class SmokeBudget:
    def __init__(self, path: Path, wallet: str, price: Decimal, budget_usd: Decimal):
        if not price.is_finite() or price <= 0:
            raise ValueError('A fresh positive SOL/USD price is required')
        if not budget_usd.is_finite() or not 0 < budget_usd <= 1:
            raise ValueError('Smoke-test budget must be positive and at most $1')
        self.path = path
        self.data = json.loads(path.read_text()) if path.exists() else {'wallet':wallet,'transactions':[]}
        if self.data['wallet'] != wallet:
            raise ValueError('Smoke ledger belongs to a different wallet')
        self.price = max(price, Decimal(str(self.data.get('sol_usd', price))))
        self.cap = int((budget_usd * Decimal('1000000000') / self.price * Decimal('.98')).to_integral_value(rounding=ROUND_DOWN))
        self.data['sol_usd'] = str(self.price)
        self.data['budget_usd'] = str(budget_usd)

    @property
    def outlay(self):
        return max(0, -sum(int(tx.get('wallet_delta_lamports', 0)) for tx in self.data['transactions']))

    def check(self, debit: int, reserve: int = 0):
        if any(tx['status'] == 'submitted' for tx in self.data['transactions']):
            raise RuntimeError('An earlier signature is unresolved; reconcile it before another send')
        if self.outlay + max(0, debit) + reserve > self.cap:
            raise RuntimeError('Trade, account rent, fees and exit reserve would exceed the $1 test budget')

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix('.tmp')
        temporary.write_text(json.dumps(self.data, indent=2), encoding='utf-8')
        temporary.replace(self.path)

    def submitted(self, *, signature, mint, side, market):
        record = dict(signature=signature,mint=mint,side=side,market=market,status='submitted',created_at=time.time())
        self.data['transactions'].append(record)
        self.save()
        return record

    def settled(self, record, transaction):
        meta = transaction['meta']
        keys = transaction['transaction']['message']['accountKeys']
        keys = [k.get('pubkey') if isinstance(k,dict) else k for k in keys]
        index = keys.index(self.data['wallet'])
        def tokens(items):
            return sum(int(item['uiTokenAmount']['amount']) for item in items
                       if item.get('owner') == self.data['wallet'] and item.get('mint') == record['mint'])
        record.update(status='failed' if meta.get('err') else 'confirmed',
                      error=meta.get('err'), fee_lamports=meta['fee'],
                      token_delta_raw=tokens(meta.get('postTokenBalances',[]))-tokens(meta.get('preTokenBalances',[])),
                      wallet_delta_lamports=meta['postBalances'][index]-meta['preBalances'][index])
        self.save()


async def run(args):
    config = load_config('simulate', 'mainnet')
    ledger_path = config.paths.state_dir / 'mamba-smoke-budget.json'
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    lock = ledger_path.with_suffix('.lock')
    # Exclusive lock prevents two smoke processes from spending the same budget.
    handle = lock.open('x')
    try:
        return await _run_locked(args, config, ledger_path)
    finally:
        handle.close()
        lock.unlink(missing_ok=True)


async def _run_locked(args, config, ledger_path):
    async with create_http_session(timeout=aiohttp.ClientTimeout(total=25)) as session:
        async def rpc(method, params):
            urls = list(dict.fromkeys([config.rpc.http_url,*os.getenv('HTTP_URLS','').split(',')]))
            for url in filter(None,urls):
                try:
                    async with session.post(url, json={'jsonrpc':'2.0','id':1,'method':method,'params':params}) as response:
                        response.raise_for_status()
                        data = await response.json()
                    if data.get('error') is None and 'result' in data:
                        return data['result']
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    continue
            raise RuntimeError(f'RPC {method} failed on all configured endpoints')

        if await rpc('getGenesisHash', []) != MAINNET_GENESIS:
            raise RuntimeError('Smoke RPC is not Solana mainnet')
        async with session.get('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd') as response:
            response.raise_for_status()
            price = Decimal(str((await response.json())['solana']['usd']))
        async with AsyncClient(config.rpc.http_url) as client:
            market = NativeMarket(session, config.rpc.http_url)
            executor = NativeExecutor(session, config.rpc.private_key, client, market=market)
            executor.market_priority = args.market
            wallet = str(executor.priv_key.pubkey())
            budget = SmokeBudget(ledger_path, wallet, price, Decimal(args.budget_usd))
            # Resume by checking the ORIGINAL signature, never by sending again.
            for record in budget.data['transactions']:
                if record['status']=='submitted':
                    transaction = await rpc('getTransaction',[record['signature'],{
                        'encoding':'json','commitment':'confirmed','maxSupportedTransactionVersion':0}])
                    if transaction:
                        budget.settled(record,transaction)
            initial = await executor.sdk.request('token_balance', mint=args.mint, user=wallet)
            if initial['amount'] != 0 and not args.exit_only:
                raise RuntimeError('Choose a mint with zero wallet balance to preserve existing holdings')
            if args.exit_only:
                opened = [t for t in budget.data['transactions'] if t['mint']==args.mint and t['side']=='buy' and t['status']=='confirmed']
                if not opened:
                    raise RuntimeError('No confirmed smoke buy exists for this mint')
            notional = Decimal(args.notional_usd)
            if not notional.is_finite() or not 0 < notional <= Decimal('.01'):
                raise ValueError('Smoke buy notional must be positive and at most $0.01')
            lamports = max(1, int(notional * Decimal('1000000000') / price))
            report = {'wallet':wallet,'mint':args.mint,'market':args.market,'execute':args.execute,
                      'sol_usd':str(price),'buy_lamports':lamports,'steps':[]}

            async def submit(side, amount):
                plan = await executor.build(args.mint, side, lamports=amount if side=='buy' else None,
                    token_amount_raw=amount if side=='sell' else None, slippage=5, priority_micro_lamports=0)
                tx = await executor.transaction(plan, sim=True)
                if len(bytes(tx)) > 1232:
                    raise RuntimeError('Transaction exceeds Solana packet limit')
                before = (await rpc('getBalance',[wallet,{'commitment':'confirmed'}]))['value']
                simulation = (await rpc('simulateTransaction',[base64.b64encode(bytes(tx)).decode(),{
                    'encoding':'base64','sigVerify':True,'commitment':'confirmed',
                    'accounts':{'encoding':'base64','addresses':[wallet]}}]))['value']
                step = {'side':side,'market':plan['market'],'pool':plan['pool'],
                        'simulation_error':simulation.get('err'),'units_consumed':simulation.get('unitsConsumed')}
                report['steps'].append(step)
                if simulation.get('err'):
                    step['logs'] = simulation.get('logs',[])[-12:]
                    return False
                accounts = simulation.get('accounts')
                if not accounts or not accounts[0]:
                    raise RuntimeError('Simulation did not return the wallet balance; refusing send')
                fee = (await client.get_fee_for_message(tx.message, commitment='confirmed')).value
                if fee is None:
                    raise RuntimeError('Fee quote unavailable')
                # Add the fee conservatively even if simulation already charged it.
                debit = max(0, before-int(accounts[0]['lamports'])) + int(fee)
                budget.check(debit, EXIT_RESERVE_LAMPORTS if side=='buy' else 0)
                step.update(max_debit_lamports=debit,fee_lamports=int(fee))
                if not args.execute:
                    return True
                signature = str(tx.signatures[0])
                record = budget.submitted(signature=signature,mint=args.mint,side=side,market=plan['market'])
                step['signature'] = signature
                # One signature, one broadcast. Never rebuild/re-sign after timeout.
                try:
                    await client.send_transaction(tx, opts=TxOpts(skip_preflight=False,skip_confirmation=True,
                        preflight_commitment='confirmed',max_retries=0))
                except Exception:
                    step['submission_response'] = 'unknown; checking original signature'
                for _ in range(40):
                    transaction = await rpc('getTransaction',[signature,{'encoding':'json','commitment':'confirmed','maxSupportedTransactionVersion':0}])
                    if transaction:
                        budget.settled(record,transaction)
                        step['status'] = record['status']
                        step['wallet_delta_lamports'] = record['wallet_delta_lamports']
                        return record['status']=='confirmed'
                    await asyncio.sleep(1)
                raise RuntimeError(f'Confirmation unresolved for {signature}; ledger blocks further sends')

            try:
                if not args.exit_only:
                    if not await submit('buy',lamports):
                        return report
                if args.execute or args.exit_only:
                    for _ in range(10):
                        balance = await executor.sdk.request('token_balance',mint=args.mint,user=wallet)
                        if balance['amount']>0:
                            break
                        await asyncio.sleep(.5)
                    if balance['amount'] <= 0:
                        raise RuntimeError('No acquired balance is available for the exit')
                    acquired = sum(int(t.get('token_delta_raw',0)) for t in budget.data['transactions']
                                   if t['mint']==args.mint and t['status']=='confirmed')
                    if acquired <= 0:
                        raise RuntimeError('Smoke ledger has no acquired position to sell')
                    await submit('sell',min(int(balance['amount']),acquired))
                    remaining = await executor.sdk.request('token_balance',mint=args.mint,user=wallet)
                    report['remaining_tokens_raw'] = remaining['amount']
                report['net_outlay_lamports'] = budget.outlay
                report['net_outlay_usd'] = str(Decimal(budget.outlay)*price/Decimal('1000000000'))
                return report
            except Exception as exc:
                report['error']=safe_error(exc)
                raise
            finally:
                target = config.paths.state_dir / f'mamba-smoke-{args.market}.json'
                target.write_text(json.dumps(report,indent=2),encoding='utf-8')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mint',required=True)
    parser.add_argument('--market',choices=['pump_fun','pump_swap'],required=True)
    parser.add_argument('--execute',action='store_true',help='Send one buy and one exact-amount sell after successful simulation')
    parser.add_argument('--exit-only',action='store_true',help='Recover a position from a confirmed smoke buy')
    parser.add_argument('--budget-usd',default='1')
    parser.add_argument('--notional-usd',default='0.01')
    args = parser.parse_args(argv)
    try:
        report = asyncio.run(run(args))
    except Exception as exc:
        print(json.dumps({'error':safe_error(exc),'error_type':type(exc).__name__},indent=2))
        return 1
    print(json.dumps(report,indent=2))
    return int(any(s.get('simulation_error') or s.get('status')=='failed' for s in report['steps']))


if __name__ == '__main__':
    raise SystemExit(main())
