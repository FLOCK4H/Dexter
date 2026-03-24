from __future__ import annotations

import base58
from decimal import Decimal
import json
import logging
import struct
from typing import Optional

from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price  # type: ignore
from solders.instruction import AccountMeta, Instruction  # type: ignore
from solders.keypair import Keypair  # type: ignore
from solders.message import MessageV0  # type: ignore
from solders.pubkey import Pubkey as PublicKey  # type: ignore
from solders.transaction import VersionedTransaction  # type: ignore
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Processed
from solana.rpc.types import TokenAccountOpts, TxOpts
from spl.token.constants import TOKEN_PROGRAM_ID
from spl.token.instructions import (
    CloseAccountParams,
    close_account,
    create_idempotent_associated_token_account,
    get_associated_token_address,
)

try:
    from .pump_swap_market import PumpSwapMarket
except Exception:
    from pump_swap_market import PumpSwapMarket
try:
    from .utils import DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS
except Exception:
    from DexLab.utils import DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS
try:
    from ..dexter_mev import MevConfig, build_mev_tip_instruction, submit_signed_transaction_via_mev
except Exception:
    from dexter_mev import MevConfig, build_mev_tip_instruction, submit_signed_transaction_via_mev


PUMP_SWAP_PROGRAM_ID = PublicKey.from_string("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
GLOBAL_CONFIG = PublicKey.from_string("ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw")
EVENT_AUTHORITY = PublicKey.from_string("GS4CU59F31iL7aR2Q8zVS8DRrcRnXX1yjQ66TqNVQnaR")
GLOBAL_VOLUME_ACCUMULATOR = PublicKey.from_string("C2aFPdENg4A2HQsmrd5rTw5TaYBX5Ku887cWjbFKtZpw")
FEE_CONFIG = PublicKey.from_string("5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx")
FEE_PROGRAM = PublicKey.from_string("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")
ASSOCIATED_TOKEN_PROGRAM = PublicKey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
SYSTEM_PROGRAM = PublicKey.from_string("11111111111111111111111111111111")
WSOL_MINT = PublicKey.from_string("So11111111111111111111111111111111111111112")
TOKEN_2022_PROGRAM_ID = PublicKey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")

SELL_DISCRIMINATOR = bytes([51, 230, 133, 164, 1, 127, 131, 173])
DEFAULT_PROTOCOL_FEE_RECIPIENT = PublicKey.from_string("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV")
DEFAULT_PROTOCOL_FEE_RECIPIENT_ATA = PublicKey.from_string("94qWNrtmfn42h3ZjUZwWvK1MEo9uVmmrBPd2hpNjYDjb")
MAYHEM_FEE_RECIPIENT = PublicKey.from_string("GesfTA3X2arioaHp8bbKdjG9vJtskViWACZoYvxp4twS")
TOTAL_SWAP_FEE_BPS = 25


class PumpSwapExecutor:
    def __init__(
        self,
        market: PumpSwapMarket,
        priv_key: str,
        async_client: AsyncClient,
        mev_config: Optional[MevConfig] = None,
    ):
        self.market = market
        self.async_client = async_client
        self.priv_key = Keypair.from_bytes(base58.b58decode(str(priv_key)))
        self.mev_config = mev_config

    def priority_fee_accounts_for_sell(
        self,
        *,
        mint_address: str,
        pool: str | None = None,
        creator: str | None = None,
    ) -> list[str]:
        values = [
            str(self.priv_key.pubkey()),
            str(mint_address),
            str(pool or ""),
            str(creator or ""),
            str(PUMP_SWAP_PROGRAM_ID),
            str(GLOBAL_CONFIG),
            str(FEE_CONFIG),
            str(FEE_PROGRAM),
            str(SYSTEM_PROGRAM),
            str(ASSOCIATED_TOKEN_PROGRAM),
            str(TOKEN_PROGRAM_ID),
            str(TOKEN_2022_PROGRAM_ID),
            str(WSOL_MINT),
        ]
        return [value for value in values if value]

    async def get_token_program_id(self, mint: PublicKey) -> PublicKey:
        info = await self.async_client.get_account_info(mint, commitment=Processed)
        account_info = getattr(info, "value", None)
        if account_info is None:
            raise ValueError(f"account not found for mint {mint}")

        owner = getattr(account_info, "owner", None)
        if owner is None and isinstance(account_info, dict):
            owner = account_info.get("owner")
        if owner is None:
            raise ValueError(f"owner not found for mint {mint}")
        return PublicKey.from_string(str(owner))

    async def resolve_user_token_account(
        self,
        owner: PublicKey,
        mint: PublicKey,
        token_program_id: PublicKey,
    ) -> Optional[PublicKey]:
        expected_ata = get_associated_token_address(owner, mint, token_program_id)
        try:
            response = await self.async_client.get_account_info(expected_ata, commitment=Processed)
            if response.value:
                return expected_ata
        except Exception as exc:
            logging.warning("Failed to read ATA %s for %s: %s", expected_ata, mint, exc)

        try:
            response = await self.async_client.get_token_accounts_by_owner_json_parsed(
                owner,
                TokenAccountOpts(mint=mint, program_id=token_program_id),
                commitment=Processed,
            )
        except Exception as exc:
            logging.error("Error resolving token accounts for %s: %s", mint, exc)
            return None

        fallback_account = None
        for account in getattr(response, "value", []) or []:
            account_pubkey = getattr(account, "pubkey", None)
            if account_pubkey is None:
                continue

            if fallback_account is None:
                fallback_account = PublicKey.from_string(str(account_pubkey))

            parsed_data = getattr(getattr(account, "account", None), "data", None)
            parsed = getattr(parsed_data, "parsed", None)
            token_amount = 0
            if isinstance(parsed, dict):
                token_amount = int(parsed.get("info", {}).get("tokenAmount", {}).get("amount", 0) or 0)

            if token_amount > 0:
                resolved = PublicKey.from_string(str(account_pubkey))
                if resolved != expected_ata:
                    logging.warning(
                        "Using token account %s for PumpSwap sell of %s because the ATA lookup was unavailable.",
                        resolved,
                        mint,
                    )
                return resolved

        return fallback_account

    async def _account_exists(self, pubkey: PublicKey) -> bool:
        try:
            response = await self.async_client.get_account_info(pubkey, commitment=Processed)
        except Exception:
            return False
        return bool(getattr(response, "value", None))

    async def _token_balance_raw(self, token_account: PublicKey) -> int:
        response = await self.async_client.get_token_account_balance(token_account, commitment=Processed)
        value = getattr(response, "value", None)
        if value is None and isinstance(response, dict):
            value = response.get("value")
        amount = getattr(value, "amount", None)
        if amount is None and isinstance(value, dict):
            amount = value.get("amount")
        return int(amount or 0)

    def _protocol_fee_accounts(self, quote_program: PublicKey, *, is_mayhem_mode: bool) -> tuple[PublicKey, PublicKey]:
        recipient = MAYHEM_FEE_RECIPIENT if is_mayhem_mode else DEFAULT_PROTOCOL_FEE_RECIPIENT
        if not is_mayhem_mode:
            return recipient, DEFAULT_PROTOCOL_FEE_RECIPIENT_ATA

        if quote_program == TOKEN_PROGRAM_ID:
            return recipient, get_associated_token_address(recipient, WSOL_MINT, TOKEN_PROGRAM_ID)
        if quote_program == TOKEN_2022_PROGRAM_ID:
            return recipient, get_associated_token_address(recipient, WSOL_MINT, TOKEN_2022_PROGRAM_ID)
        raise ValueError(f"unsupported quote token program for mayhem fee recipient ATA: {quote_program}")

    @staticmethod
    def _slippage_multiplier(slippage: float | int | Decimal) -> Decimal:
        tolerance = Decimal(str(slippage or 0))
        if tolerance <= 0:
            return Decimal("1")
        if tolerance > Decimal("5"):
            return Decimal("1") + (tolerance / Decimal("100"))
        if tolerance > Decimal("1"):
            return tolerance
        return Decimal("1") + tolerance

    def _encode_sell_instruction_data(self, base_amount_in: int, min_quote_amount_out: int) -> bytes:
        return SELL_DISCRIMINATOR + struct.pack("<QQ", int(base_amount_in), int(min_quote_amount_out))

    async def _compile_transaction(self, instructions: list[Instruction]):
        latest_blockhash = (await self.async_client.get_latest_blockhash(commitment=Processed)).value.blockhash
        msg = MessageV0.try_compile(
            payer=self.priv_key.pubkey(),
            instructions=instructions,
            address_lookup_table_accounts=[],
            recent_blockhash=latest_blockhash,
        )
        return VersionedTransaction(msg, [self.priv_key])

    async def _simulate_or_send(
        self,
        tx: VersionedTransaction,
        sim: bool = False,
        *,
        use_mev: bool = False,
    ):
        if sim:
            return await self.async_client.simulate_transaction(
                tx,
                sig_verify=False,
                commitment=Processed,
            )

        if use_mev and self.mev_config is not None:
            return await submit_signed_transaction_via_mev(tx, self.mev_config)

        opts = TxOpts(skip_preflight=True, skip_confirmation=True)
        result = await self.async_client.send_transaction(tx, opts=opts)
        result_json = result.to_json()
        return json.loads(result_json).get("result")

    async def estimate_sell_quote_out(self, pool: str, token_amount_in: int) -> int:
        pool_state = await self.market.fetch_pool_state(pool)
        snapshot = await self.market.fetch_price_snapshot(pool_state=pool_state)
        base_reserves_raw = int(snapshot.get("base_reserves_raw", 0) or 0)
        quote_reserves_raw = int(snapshot.get("quote_reserves_raw", 0) or 0)
        if token_amount_in <= 0 or base_reserves_raw <= 0 or quote_reserves_raw <= 0:
            return 0

        gross_quote_out = (quote_reserves_raw * int(token_amount_in)) // (base_reserves_raw + int(token_amount_in))
        return (gross_quote_out * (10_000 - TOTAL_SWAP_FEE_BPS)) // 10_000

    async def pump_sell(
        self,
        mint_address: str,
        token_amount: int,
        *,
        pool: str | None = None,
        creator: str | None = None,
        sim: bool = False,
        priority_micro_lamports: int = 0,
        slippage: float = 1.30,
    ):
        if token_amount <= 0:
            return "zero_amount"

        user = self.priv_key.pubkey()
        mint = PublicKey.from_string(mint_address)

        pool_state = None
        pool_address = pool
        if pool_address:
            pool_state = await self.market.fetch_pool_state(pool_address)
        else:
            pool_state = await self.market.find_pool_for_mint(mint_address, creator=creator)
            if pool_state is None:
                return "pool_unavailable"
            pool_address = pool_state.pool

        if pool_state is None or not pool_address:
            return "pool_unavailable"
        if pool_state.base_mint != mint_address:
            return "unexpected_pool"

        base_program = await self.get_token_program_id(mint)
        quote_mint = PublicKey.from_string(pool_state.quote_mint)
        quote_program = await self.get_token_program_id(quote_mint)

        user_base_token_account = await self.resolve_user_token_account(user, mint, base_program)
        if user_base_token_account is None:
            return "missing_ata"

        user_quote_token_account = get_associated_token_address(user, quote_mint, quote_program)
        protocol_fee_recipient, protocol_fee_recipient_token_account = self._protocol_fee_accounts(
            quote_program,
            is_mayhem_mode=bool(pool_state.is_mayhem_mode),
        )

        estimated_quote_out = await self.estimate_sell_quote_out(pool_address, int(token_amount))
        if estimated_quote_out <= 0:
            return "zero_quote"
        min_quote_amount_out = int(
            Decimal(estimated_quote_out) / self._slippage_multiplier(slippage)
        )

        creator_vault_authority, _ = PublicKey.find_program_address(
            [b"creator_vault", bytes(PublicKey.from_string(pool_state.coin_creator))],
            PUMP_SWAP_PROGRAM_ID,
        )
        coin_creator_vault_ata = get_associated_token_address(
            creator_vault_authority,
            quote_mint,
            quote_program,
        )

        instructions: list[Instruction] = []
        if priority_micro_lamports > 0:
            instructions.append(set_compute_unit_limit(DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS))
            instructions.append(set_compute_unit_price(priority_micro_lamports))
        if not sim and self.mev_config is not None:
            mev_tip_ix = build_mev_tip_instruction(user, self.mev_config)
            if mev_tip_ix is not None:
                instructions.append(mev_tip_ix)

        instructions.append(
            create_idempotent_associated_token_account(
                payer=user,
                owner=user,
                mint=quote_mint,
                token_program_id=quote_program,
            )
        )

        accounts = [
            AccountMeta(pubkey=PublicKey.from_string(pool_address), is_signer=False, is_writable=True),
            AccountMeta(pubkey=user, is_signer=True, is_writable=True),
            AccountMeta(pubkey=GLOBAL_CONFIG, is_signer=False, is_writable=False),
            AccountMeta(pubkey=mint, is_signer=False, is_writable=False),
            AccountMeta(pubkey=quote_mint, is_signer=False, is_writable=False),
            AccountMeta(pubkey=user_base_token_account, is_signer=False, is_writable=True),
            AccountMeta(pubkey=user_quote_token_account, is_signer=False, is_writable=True),
            AccountMeta(pubkey=PublicKey.from_string(pool_state.pool_base_token_account), is_signer=False, is_writable=True),
            AccountMeta(pubkey=PublicKey.from_string(pool_state.pool_quote_token_account), is_signer=False, is_writable=True),
            AccountMeta(pubkey=protocol_fee_recipient, is_signer=False, is_writable=False),
            AccountMeta(pubkey=protocol_fee_recipient_token_account, is_signer=False, is_writable=True),
            AccountMeta(pubkey=base_program, is_signer=False, is_writable=False),
            AccountMeta(pubkey=quote_program, is_signer=False, is_writable=False),
            AccountMeta(pubkey=SYSTEM_PROGRAM, is_signer=False, is_writable=False),
            AccountMeta(pubkey=ASSOCIATED_TOKEN_PROGRAM, is_signer=False, is_writable=False),
            AccountMeta(pubkey=EVENT_AUTHORITY, is_signer=False, is_writable=False),
            AccountMeta(pubkey=PUMP_SWAP_PROGRAM_ID, is_signer=False, is_writable=False),
            AccountMeta(pubkey=coin_creator_vault_ata, is_signer=False, is_writable=True),
            AccountMeta(pubkey=creator_vault_authority, is_signer=False, is_writable=False),
            AccountMeta(pubkey=FEE_CONFIG, is_signer=False, is_writable=False),
            AccountMeta(pubkey=FEE_PROGRAM, is_signer=False, is_writable=False),
        ]

        instructions.append(
            Instruction(
                program_id=PUMP_SWAP_PROGRAM_ID,
                accounts=accounts,
                data=self._encode_sell_instruction_data(int(token_amount), min_quote_amount_out),
            )
        )

        user_token_balance_raw = await self._token_balance_raw(user_base_token_account)
        if int(token_amount) >= user_token_balance_raw > 0:
            instructions.append(
                close_account(
                    CloseAccountParams(
                        program_id=base_program,
                        account=user_base_token_account,
                        dest=user,
                        owner=user,
                        signers=[],
                    )
                )
            )

        if quote_mint == WSOL_MINT:
            instructions.append(
                close_account(
                    CloseAccountParams(
                        program_id=quote_program,
                        account=user_quote_token_account,
                        dest=user,
                        owner=user,
                        signers=[],
                    )
                )
            )

        tx = await self._compile_transaction(instructions)
        return await self._simulate_or_send(tx, sim=sim, use_mev=not sim and self.mev_config is not None)
