from __future__ import annotations

import base64
import base58
from dataclasses import dataclass
from decimal import Decimal
import logging
import time
from typing import Any

from aiohttp import ClientSession


PUMP_SWAP_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
WSOL_MINT = "So11111111111111111111111111111111111111112"

ACCOUNT_DISCRIMINATOR_LEN = 8
CREATOR_OFFSET = 11
BASE_MINT_OFFSET = 43
QUOTE_MINT_OFFSET = 75
LP_MINT_OFFSET = 107
POOL_BASE_TOKEN_ACCOUNT_OFFSET = 139
POOL_QUOTE_TOKEN_ACCOUNT_OFFSET = 171
LP_SUPPLY_OFFSET = 203
COIN_CREATOR_OFFSET = 211
IS_MAYHEM_MODE_OFFSET = 243
POOL_ACCOUNT_DATA_SIZE = 244
POOL_ACCOUNT_DATA_FILTER_SIZES = (POOL_ACCOUNT_DATA_SIZE, POOL_ACCOUNT_DATA_SIZE + 1)


@dataclass(frozen=True)
class PumpSwapPoolState:
    pool: str
    creator: str
    base_mint: str
    quote_mint: str
    lp_mint: str
    pool_base_token_account: str
    pool_quote_token_account: str
    lp_supply: int
    coin_creator: str
    is_mayhem_mode: bool


class PumpSwapMarket:
    def __init__(self, session: ClientSession, rpc_url: str):
        self.session = session
        self.rpc_url = rpc_url
        self._mint_decimals_cache: dict[str, int] = {}
        self._prefer_paginated_program_accounts = False
        self._last_paginated_lookup_warning_at = 0.0
        self._paginated_lookup_warning_interval_seconds = 60.0

    @staticmethod
    def _should_retry_with_pagination(exc: Exception) -> bool:
        message = str(exc).lower()
        return "too many accounts requested" in message or "getprogramaccountsv2" in message

    def _log_paginated_lookup_fallback(self) -> None:
        now = time.time()
        if (now - self._last_paginated_lookup_warning_at) < self._paginated_lookup_warning_interval_seconds:
            return
        self._last_paginated_lookup_warning_at = now
        logging.warning(
            "PumpSwap RPC requires paginated getProgramAccountsV2; using paginated pool lookups."
        )

    async def _get_program_accounts_paginated(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        accounts: list[dict[str, Any]] = []
        pagination_key: str | None = None
        for _ in range(5):
            paginated_options = dict(options)
            paginated_options["limit"] = 100
            if pagination_key:
                paginated_options["paginationKey"] = pagination_key
            result = await self._rpc_call(
                "getProgramAccountsV2",
                [PUMP_SWAP_PROGRAM_ID, paginated_options],
            )
            page_accounts = list((result or {}).get("accounts") or [])
            accounts.extend(page_accounts)
            pagination_key = (result or {}).get("paginationKey")
            if not pagination_key or not page_accounts:
                break
        return accounts

    async def _get_program_accounts(self, filters: list[dict[str, Any]]) -> list[dict[str, Any]]:
        options = {
            "encoding": "base64",
            "commitment": "confirmed",
            "filters": filters,
        }
        if self._prefer_paginated_program_accounts:
            return await self._get_program_accounts_paginated(options)
        try:
            result = await self._rpc_call(
                "getProgramAccounts",
                [PUMP_SWAP_PROGRAM_ID, options],
            )
            return list(result or [])
        except RuntimeError as exc:
            if not self._should_retry_with_pagination(exc):
                raise
            self._prefer_paginated_program_accounts = True
            self._log_paginated_lookup_fallback()

        return await self._get_program_accounts_paginated(options)

    async def _rpc_call(self, method: str, params: list[Any]) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        async with self.session.post(self.rpc_url, json=payload, timeout=15) as response:
            response.raise_for_status()
            data = await response.json()
        if data.get("error"):
            raise RuntimeError(f"{method} failed: {data['error']}")
        return data.get("result")

    @staticmethod
    def _decode_account_data(account: dict[str, Any]) -> bytes:
        data = account.get("data")
        if isinstance(data, list) and data:
            return base64.b64decode(data[0])
        raise ValueError("account payload is missing base64 data")

    @staticmethod
    def _decode_pubkey(data: bytes, offset: int) -> str:
        return base58.b58encode(data[offset:offset + 32]).decode("utf-8")

    def _parse_pool_state(self, pool: str, account_data: bytes) -> PumpSwapPoolState:
        if len(account_data) < POOL_ACCOUNT_DATA_SIZE:
            raise ValueError(f"unexpected PumpSwap pool account size: {len(account_data)}")

        return PumpSwapPoolState(
            pool=pool,
            creator=self._decode_pubkey(account_data, CREATOR_OFFSET),
            base_mint=self._decode_pubkey(account_data, BASE_MINT_OFFSET),
            quote_mint=self._decode_pubkey(account_data, QUOTE_MINT_OFFSET),
            lp_mint=self._decode_pubkey(account_data, LP_MINT_OFFSET),
            pool_base_token_account=self._decode_pubkey(account_data, POOL_BASE_TOKEN_ACCOUNT_OFFSET),
            pool_quote_token_account=self._decode_pubkey(account_data, POOL_QUOTE_TOKEN_ACCOUNT_OFFSET),
            lp_supply=int.from_bytes(account_data[LP_SUPPLY_OFFSET:LP_SUPPLY_OFFSET + 8], "little"),
            coin_creator=self._decode_pubkey(account_data, COIN_CREATOR_OFFSET),
            is_mayhem_mode=bool(account_data[IS_MAYHEM_MODE_OFFSET]),
        )

    async def fetch_pool_state(self, pool: str) -> PumpSwapPoolState:
        result = await self._rpc_call(
            "getAccountInfo",
            [pool, {"encoding": "base64", "commitment": "confirmed"}],
        )
        value = (result or {}).get("value")
        if not value:
            raise ValueError(f"pump.swap pool {pool} was not found")
        account_data = self._decode_account_data(value)
        return self._parse_pool_state(pool, account_data)

    async def _find_pool_candidates(self, mint: str, creator: str | None = None) -> list[PumpSwapPoolState]:
        filters: list[dict[str, Any]] = [
            {"memcmp": {"offset": BASE_MINT_OFFSET, "bytes": mint}},
            {"memcmp": {"offset": QUOTE_MINT_OFFSET, "bytes": WSOL_MINT}},
        ]
        if creator:
            filters.append({"memcmp": {"offset": CREATOR_OFFSET, "bytes": creator}})

        result: list[dict[str, Any]] = []
        seen_pubkeys: set[str] = set()
        for account_size in POOL_ACCOUNT_DATA_FILTER_SIZES:
            sized_filters = [{"dataSize": account_size}, *filters]
            sized_result = await self._get_program_accounts(sized_filters)
            for entry in sized_result or []:
                pubkey = entry.get("pubkey")
                if not pubkey or pubkey in seen_pubkeys:
                    continue
                seen_pubkeys.add(pubkey)
                result.append(entry)
            if result:
                break

        pools: list[PumpSwapPoolState] = []
        for entry in result or []:
            pubkey = entry.get("pubkey")
            account = entry.get("account", {})
            if not pubkey or not account:
                continue
            try:
                pool = self._parse_pool_state(pubkey, self._decode_account_data(account))
                if pool.base_mint != mint or pool.quote_mint != WSOL_MINT:
                    continue
                if creator and pool.creator != creator:
                    continue
                pools.append(pool)
            except Exception as exc:
                logging.debug("Ignoring undecodable PumpSwap pool %s: %s", pubkey, exc)
        return pools

    async def find_pool_for_mint(self, mint: str, creator: str | None = None) -> PumpSwapPoolState | None:
        pools = await self._find_pool_candidates(mint, creator=creator)
        if not pools and creator:
            pools = await self._find_pool_candidates(mint, creator=None)
        if not pools:
            return None

        # Canonical Pump.fun migrations target the WSOL-quoted index-0 pool.
        return sorted(pools, key=lambda pool: (pool.creator != (creator or pool.creator), pool.pool))[0]

    async def _get_token_balance_raw(self, token_account: str) -> int:
        result = await self._rpc_call(
            "getTokenAccountBalance",
            [token_account, {"commitment": "confirmed"}],
        )
        value = (result or {}).get("value", {})
        return int(value.get("amount") or 0)

    async def _get_mint_decimals(self, mint: str) -> int:
        if mint in self._mint_decimals_cache:
            return self._mint_decimals_cache[mint]

        result = await self._rpc_call(
            "getTokenSupply",
            [mint, {"commitment": "confirmed"}],
        )
        value = (result or {}).get("value", {})
        decimals = int(value.get("decimals") or 0)
        self._mint_decimals_cache[mint] = decimals
        return decimals

    async def fetch_price_snapshot(
        self,
        *,
        pool: str | None = None,
        pool_state: PumpSwapPoolState | None = None,
    ) -> dict[str, Any]:
        if pool_state is None:
            if not pool:
                raise ValueError("pool or pool_state is required")
            pool_state = await self.fetch_pool_state(pool)

        base_reserves_raw = await self._get_token_balance_raw(pool_state.pool_base_token_account)
        quote_reserves_raw = await self._get_token_balance_raw(pool_state.pool_quote_token_account)
        base_decimals = await self._get_mint_decimals(pool_state.base_mint)

        price = Decimal("0")
        if base_reserves_raw > 0:
            price = (
                Decimal(quote_reserves_raw) / Decimal("1000000000")
            ) / (
                Decimal(base_reserves_raw) / (Decimal(10) ** base_decimals)
            )

        return {
            "pool": pool_state.pool,
            "creator": pool_state.creator,
            "base_mint": pool_state.base_mint,
            "quote_mint": pool_state.quote_mint,
            "price": price,
            "base_reserves_raw": base_reserves_raw,
            "quote_reserves_raw": quote_reserves_raw,
            "base_decimals": base_decimals,
            "liquidity_sol": Decimal(quote_reserves_raw) / Decimal("1000000000"),
            "lp_supply": pool_state.lp_supply,
            "is_mayhem_mode": pool_state.is_mayhem_mode,
        }
