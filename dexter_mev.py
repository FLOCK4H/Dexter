from __future__ import annotations

import asyncio
import base64
import json
import random
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from typing import Literal

from aiohttp import ClientSession, ClientTimeout
from solders.instruction import Instruction  # type: ignore
from solders.pubkey import Pubkey  # type: ignore
from solders.system_program import TransferParams, transfer  # type: ignore
from solders.transaction import VersionedTransaction  # type: ignore


MevProviderName = Literal["jito", "helius", "nextblock", "zero_slot", "temporal", "bloxroute"]

VALID_MEV_PROVIDERS: frozenset[str] = frozenset(
    {"jito", "helius", "nextblock", "zero_slot", "temporal", "bloxroute"}
)
MEV_PROVIDERS_REQUIRING_KEYS: frozenset[str] = frozenset(
    {"nextblock", "zero_slot", "temporal", "bloxroute"}
)

JITO_BLOCK_ENGINE_ENDPOINTS = (
    "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1",
    "https://london.mainnet.block-engine.jito.wtf/api/v1",
    "https://amsterdam.mainnet.block-engine.jito.wtf/api/v1",
    "https://ny.mainnet.block-engine.jito.wtf/api/v1",
    "https://tokyo.mainnet.block-engine.jito.wtf/api/v1",
    "https://singapore.mainnet.block-engine.jito.wtf/api/v1",
    "https://slc.mainnet.block-engine.jito.wtf/api/v1",
    "https://mainnet.block-engine.jito.wtf/api/v1",
)
HELIUS_SENDER_ENDPOINTS = (
    "http://fra-sender.helius-rpc.com",
    "http://ams-sender.helius-rpc.com",
    "http://lon-sender.helius-rpc.com",
    "http://slc-sender.helius-rpc.com",
    "http://ewr-sender.helius-rpc.com",
    "http://sg-sender.helius-rpc.com",
    "http://tyo-sender.helius-rpc.com",
)
NEXTBLOCK_ENDPOINTS = (
    "http://fra.nextblock.io",
    "http://london.nextblock.io",
    "http://ny.nextblock.io",
    "http://slc.nextblock.io",
    "http://tokyo.nextblock.io",
)
ZERO_SLOT_ENDPOINTS = (
    "http://de1.0slot.trade",
    "http://ny.0slot.trade",
    "http://ams.0slot.trade",
    "http://jp.0slot.trade",
    "http://la.0slot.trade",
)
TEMPORAL_HTTP_ENDPOINTS = (
    "http://fra2.nozomi.temporal.xyz/?c=",
    "http://ams1.nozomi.temporal.xyz/?c=",
    "http://pit1.nozomi.temporal.xyz/?c=",
    "http://tyo1.nozomi.temporal.xyz/?c=",
    "http://sgp1.nozomi.temporal.xyz/?c=",
    "http://ewr1.nozomi.temporal.xyz/?c=",
)
BLOXROUTE_ENDPOINTS = (
    "http://germany.solana.dex.blxrbdn.com",
    "http://uk.solana.dex.blxrbdn.com",
    "http://amsterdam.solana.dex.blxrbdn.com",
    "http://global.solana.dex.blxrbdn.com",
    "http://tokyo.solana.dex.blxrbdn.com",
    "http://la.solana.dex.blxrbdn.com",
    "http://ny.solana.dex.blxrbdn.com",
)

MEV_TIP_ACCOUNTS: dict[MevProviderName, tuple[str, ...]] = {
    "jito": (
        "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
        "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
        "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
        "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
        "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
        "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
        "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
        "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    ),
    "helius": (
        "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE",
        "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ",
        "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta",
        "5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn",
        "2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD",
        "2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ",
        "wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF",
        "3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT",
        "4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey",
        "4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or",
    ),
    "nextblock": (
        "NextbLoCkVtMGcV47JzewQdvBpLqT9TxQFozQkN98pE",
        "NexTbLoCkWykbLuB1NkjXgFWkX9oAtcoagQegygXXA2",
        "NeXTBLoCKs9F1y5PJS9CKrFNNLU1keHW71rfh7KgA1X",
        "NexTBLockJYZ7QD7p2byrUa6df8ndV2WSd8GkbWqfbb",
        "neXtBLock1LeC67jYd1QdAa32kbVeubsfPNTJC1V5At",
        "nEXTBLockYgngeRmRrjDV31mGSekVPqZoMGhQEZtPVG",
        "NEXTbLoCkB51HpLBLojQfpyVAMorm3zzKg7w9NFdqid",
        "nextBLoCkPMgmG8ZgJtABeScP35qLa2AMCNKntAP7Xc",
    ),
    "zero_slot": (
        "Eb2KpSC8uMt9GmzyAEm5Eb1AAAgTjRaXWFjKyFXHZxF3",
        "FCjUJZ1qozm1e8romw216qyfQMaaWKxWsuySnumVCCNe",
        "ENxTEjSQ1YabmUpXAdCgevnHQ9MHdLv8tzFiuiYJqa13",
        "6rYLG55Q9RpsPGvqdPNJs4z5WTxJVatMB8zV3WJhs5EK",
        "Cix2bHfqPcKcM233mzxbLk14kSggUUiz2A87fJtGivXr",
    ),
    "temporal": (
        "TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq",
        "noz3jAjPiHuBPqiSPkkugaJDkJscPuRhYnSpbi8UvC4",
        "noz3str9KXfpKknefHji8L1mPgimezaiUyCHYMDv1GE",
        "noz6uoYCDijhu1V7cutCpwxNiSovEwLdRHPwmgCGDNo",
        "noz9EPNcT7WH6Sou3sr3GGjHQYVkN3DNirpbvDkv9YJ",
        "nozc5yT15LazbLTFVZzoNZCwjh3yUtW86LoUyqsBu4L",
        "nozFrhfnNGoyqwVuwPAW4aaGqempx4PU6g6D9CJMv7Z",
        "nozievPk7HyK1Rqy1MPJwVQ7qQg2QoJGyP71oeDwbsu",
        "noznbgwYnBLDHu8wcQVCEw6kDrXkPdKkydGJGNXGvL7",
        "nozNVWs5N8mgzuD3qigrCG2UoKxZttxzZ85pvAQVrbP",
        "nozpEGbwx4BcGp6pvEdAh1JoC2CQGZdU6HbNP1v2p6P",
        "nozrhjhkCr3zXT3BiT4WCodYCUFeQvcdUkM7MqhKqge",
        "nozrwQtWhEdrA6W8dkbt9gnUaMs52PdAv5byipnadq3",
        "nozUacTVWub3cL4mJmGCYjKZTnE9RbdY5AP46iQgbPJ",
        "nozWCyTPppJjRuw2fpzDhhWbW355fzosWSzrrMYB1Qk",
        "nozWNju6dY353eMkMqURqwQEoM3SFgEKC6psLCSfUne",
        "nozxNBgWohjR75vdspfxR5H9ceC7XXH99xpxhVGt3Bb",
    ),
    "bloxroute": (
        "HWEoBxYs7ssKuudEjzjmpfJVX7Dvi7wescFsVx2L5yoY",
        "95cfoy472fcQHaw4tPGBTKpn6ZQnfEPfBgDQx6gcRmRg",
        "3UQUKjhMKaY2S6bjcQD6yHB7utcZt5bfarRCmctpRtUd",
        "FogxVNs6Mm2w9rnGL1vkARSwJxvLE8mujTv3LK8RnUhF",
    ),
}


@dataclass(frozen=True)
class MevConfig:
    enabled: bool
    provider: MevProviderName
    tip_sol: Decimal
    tip_lamports: int
    jito_key: str = ""
    nextblock_key: str = ""
    zero_slot_key: str = ""
    temporal_key: str = ""
    bloxroute_key: str = ""

    def requires_api_key(self) -> bool:
        return self.provider in MEV_PROVIDERS_REQUIRING_KEYS

    def active_provider_key(self) -> str | None:
        if self.provider == "jito":
            value = self.jito_key.strip()
            return value or None
        if self.provider == "nextblock":
            value = self.nextblock_key.strip()
            return value or None
        if self.provider == "zero_slot":
            value = self.zero_slot_key.strip()
            return value or None
        if self.provider == "temporal":
            value = self.temporal_key.strip()
            return value or None
        if self.provider == "bloxroute":
            value = self.bloxroute_key.strip()
            return value or None
        return None


def normalize_mev_provider(raw: str | None) -> MevProviderName:
    value = str(raw or "jito").strip().lower()
    if value not in VALID_MEV_PROVIDERS:
        raise ValueError(
            "MEV_PROVIDER must be one of: jito, helius, nextblock, zero_slot, temporal, bloxroute"
        )
    return value  # type: ignore[return-value]


def mev_tip_lamports_from_sol(raw: str | Decimal | None, *, default: str = "0.00001") -> tuple[Decimal, int]:
    value = raw if raw not in (None, "") else default
    try:
        tip_sol = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value for MEV_TIP: {value}") from exc
    if not tip_sol.is_finite():
        raise ValueError("MEV_TIP must be a finite number")
    if tip_sol <= 0:
        raise ValueError("MEV_TIP must be > 0")
    if tip_sol > Decimal("1"):
        raise ValueError("MEV_TIP must be <= 1 SOL")

    lamports = int(
        (tip_sol * Decimal("1000000000")).to_integral_value(rounding=ROUND_CEILING)
    )
    if lamports <= 0:
        raise ValueError("MEV_TIP is too small")
    return tip_sol, lamports


def tip_account_for_provider(provider: MevProviderName) -> Pubkey:
    return Pubkey.from_string(random.choice(MEV_TIP_ACCOUNTS[provider]))


def build_mev_tip_instruction(payer: Pubkey, mev_config: MevConfig | None) -> Instruction | None:
    if mev_config is None or not mev_config.enabled or mev_config.tip_lamports <= 0:
        return None
    return transfer(
        TransferParams(
            from_pubkey=payer,
            to_pubkey=tip_account_for_provider(mev_config.provider),
            lamports=int(mev_config.tip_lamports),
        )
    )


def tx_signature(tx: VersionedTransaction) -> str:
    if not tx.signatures:
        raise ValueError("signed transaction is missing a signature")
    return str(tx.signatures[0])


def tx_base64(tx: VersionedTransaction) -> str:
    return base64.b64encode(bytes(tx)).decode("ascii")


async def submit_signed_transaction_via_mev(
    tx: VersionedTransaction,
    mev_config: MevConfig,
    *,
    timeout_seconds: float = 8.0,
) -> str:
    if not mev_config.enabled:
        raise ValueError("MEV is not enabled")
    if mev_config.requires_api_key() and mev_config.active_provider_key() is None:
        raise ValueError(f"missing API key for {mev_config.provider}")

    signature = tx_signature(tx)
    b64_tx = tx_base64(tx)

    timeout = ClientTimeout(total=timeout_seconds)
    async with ClientSession(timeout=timeout) as session:
        submissions = [
            _submit_request(session, name, url, headers, payload)
            for name, url, headers, payload in _provider_requests(session, mev_config, b64_tx)
        ]
        results = await asyncio.gather(*submissions, return_exceptions=True)

    errors: list[str] = []
    for result in results:
        if isinstance(result, tuple):
            ok, detail = result
            if ok:
                return signature
            errors.append(detail)
        else:
            errors.append(str(result))

    raise RuntimeError("; ".join(errors[:4]) or "MEV submission failed")


def _provider_requests(
    _session: ClientSession,
    mev_config: MevConfig,
    b64_tx: str,
) -> list[tuple[str, str, dict[str, str], dict[str, object]]]:
    content_headers = {"Content-Type": "application/json"}

    if mev_config.provider == "jito":
        headers = dict(content_headers)
        auth = mev_config.active_provider_key()
        if auth:
            headers["x-jito-auth"] = auth
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [[b64_tx], {"encoding": "base64"}],
        }
        return [
            ("jito", f"{endpoint}/bundles", headers, payload)
            for endpoint in JITO_BLOCK_ENGINE_ENDPOINTS
        ]

    if mev_config.provider == "helius":
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                b64_tx,
                {
                    "encoding": "base64",
                    "skipPreflight": True,
                    "maxRetries": 0,
                },
            ],
        }
        return [
            ("helius", f"{endpoint}/fast?swqos_only=true", dict(content_headers), payload)
            for endpoint in HELIUS_SENDER_ENDPOINTS
        ]

    if mev_config.provider == "nextblock":
        payload = {
            "transaction": {"content": b64_tx},
            "frontRunningProtection": False,
        }
        headers = {
            **content_headers,
            "Authorization": str(mev_config.active_provider_key() or ""),
        }
        return [
            ("nextblock", f"{endpoint}/api/v2/submit", headers, payload)
            for endpoint in NEXTBLOCK_ENDPOINTS
        ]

    if mev_config.provider == "zero_slot":
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                b64_tx,
                {
                    "encoding": "base64",
                    "skipPreflight": True,
                },
            ],
        }
        key = str(mev_config.active_provider_key() or "")
        return [
            ("zero_slot", f"{endpoint}?api-key={key}", dict(content_headers), payload)
            for endpoint in ZERO_SLOT_ENDPOINTS
        ]

    if mev_config.provider == "temporal":
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                b64_tx,
                {
                    "encoding": "base64",
                },
            ],
        }
        key = str(mev_config.active_provider_key() or "")
        return [
            ("temporal", f"{endpoint}{key}", dict(content_headers), payload)
            for endpoint in TEMPORAL_HTTP_ENDPOINTS
        ]

    headers = {
        **content_headers,
        "Authorization": str(mev_config.active_provider_key() or ""),
    }
    payload = {
        "transaction": {"content": b64_tx},
        "skipPreFlight": True,
        "frontRunningProtection": False,
        "submitProtection": "SP_LOW",
        "fastBestEffort": False,
        "useStakedRPCs": True,
        "allowBackRun": False,
        "revenueAddress": "",
    }
    return [
        ("bloxroute", f"{endpoint}/api/v2/submit", headers, payload)
        for endpoint in BLOXROUTE_ENDPOINTS
    ]


async def _submit_request(
    session: ClientSession,
    provider_name: str,
    url: str,
    headers: dict[str, str],
    payload: dict[str, object],
) -> tuple[bool, str]:
    try:
        async with session.post(url, headers=headers, json=payload) as response:
            body = await response.text()
    except Exception as exc:
        return False, f"{provider_name} request failed: {exc}"

    if response.status >= 400:
        return False, f"{provider_name} returned HTTP {response.status}: {_truncate(body)}"

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    if isinstance(data, dict) and data.get("error") is not None:
        return False, f"{provider_name} returned error: {_truncate(json.dumps(data['error']))}"

    return True, provider_name


def _truncate(value: str, *, limit: int = 240) -> str:
    text = str(value or "").strip().replace("\n", " ")
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."
