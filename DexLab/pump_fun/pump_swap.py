from solders.transaction import VersionedTransaction # type: ignore
from solders.keypair import Keypair # type: ignore
from solders.pubkey import Pubkey as PublicKey # type: ignore
from solders import message
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TokenAccountOpts, TxOpts
from solders.transaction import Transaction # type: ignore
from solders.instruction import AccountMeta, Instruction # type: ignore
from solders.signature import Signature # type: ignore
from spl.token.constants import TOKEN_PROGRAM_ID
from spl.token.instructions import (
    get_associated_token_address,
)
import base58
from borsh_construct import CStruct, U64
from dataclasses import dataclass
from decimal import Decimal
import logging
import asyncio, json
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price # type: ignore
from aiohttp import ClientSession, FormData
import mimetypes
from pathlib import Path
import time
import struct
from typing import Any, List, Optional
try: from .pump_bond import get_bonding_curve_state
except: from .pump_bond import get_bonding_curve_state
try:
    from ..utils import DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS
except Exception:
    from DexLab.utils import DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS
try:
    from ..utils import create_compatible_idempotent_associated_token_account
except Exception:
    from DexLab.utils import create_compatible_idempotent_associated_token_account
try:
    from ...dexter_mev import MevConfig, build_mev_tip_instruction, submit_signed_transaction_via_mev
except Exception:
    from dexter_mev import MevConfig, build_mev_tip_instruction, submit_signed_transaction_via_mev

from solana.rpc.commitment import Processed
from solders.message    import MessageV0 # type: ignore
from dexter_price import get_solana_price_usd as load_solana_price_usd

PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
MAYHEM_PROGRAM = "MAyhSmzXzV1pTf7LsNkrNwkWKTo4ougAJ1PPg47MD4e"
GLOBAL_ACCOUNT = "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"
EVENT_AUTHORITY = "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"
METADATA_PROGRAM = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
ATA_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
RENT_SYSVAR = "SysvarRent111111111111111111111111111111111"
GLOBAL_VOLUME_ACCUMULATOR = "Hq2wp8uJ9jCPsYgNHex8RtqdvMPfVGoYwjvF1ATiwn2Y"
FEE_CONFIG = "8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt"
FEE_PROGRAM = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ"
DEFAULT_FEE_RECIPIENT = "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV"
DEFAULT_PUMP_FUN_IPFS_UPLOAD_URL = "https://pump.fun/api/ipfs"
DEFAULT_BUY_QUOTE_FEE_BPS = 125
MAYHEM_FEE_RECIPIENTS = [
    "GesfTA3X2arioaHp8bbKdjG9vJtskViWACZoYvxp4twS",
    "4budycTjhs9fD6xw62VBducVTNgMgJJ5BgtKq7mAZwn6",
    "8SBKzEQU4nLSzcwF4a74F2iaUDQyTfjGndn6qUWBnrpR",
    "4UQeTP1T39KZ9Sfxzo3WR5skgsaP6NZa87BAkuazLEKH",
    "8sNeir4QsLsJdYpc9RZacohhK1Y5FLU3nC5LXgYB4aa6",
    "Fh9HmeLNUMVCvejxCtCL2DbYaRyBFVJ5xrWkLnMH6fdk",
    "463MEnMeGyJekNZFQSTUABBEbLnvMTALbT6ZmsxAbAdq",
    "6AUH3WEHucYZyC61hqpqYUWVto5qA5hjHuNQ32GNnNxA",
]
TOKEN_2022_PROGRAM_ID = PublicKey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
LEGACY_TOKEN_PROGRAM_ID = PublicKey.from_string(str(TOKEN_PROGRAM_ID))

BUY_INSTRUCTION_SCHEMA = CStruct(
    "amount" / U64,
    "max_sol_cost" / U64
)

BUY_EXACT_SOL_IN_DISCRIMINATOR = bytes([56, 252, 116, 8, 158, 223, 205, 95])
SELL_INSTRUCTION_SCHEMA = CStruct(
    "amount" / U64,
    "min_sol_output" / U64
)

CREATE_DISCRIMINATOR = bytes([24, 30, 200, 40, 5, 28, 7, 119])
BUY_DISCRIMINATOR = bytes([102, 6, 61, 18, 1, 218, 235, 234])
CREATE_V2_DISCRIMINATOR = bytes([214, 144, 76, 236, 95, 139, 49, 180])
EXTEND_ACCOUNT_DISCRIMINATOR = bytes([234, 102, 194, 203, 150, 72, 62, 229])
SELL_DISCRIMINATOR = bytes([51, 230, 133, 164, 1, 127, 131, 173])

suppress_logs = [
    "socks",
    "requests",
    "httpx",
    "trio.async_generator_errors",
    "trio",
    "trio.abc.Instrument",
    "trio.abc",
    "trio.serve_listeners",
    "httpcore.http11",
    "httpcore",
    "httpcore.connection",
    "httpcore.proxy",
]

# Set all of them to CRITICAL (no logs)
for log_name in suppress_logs:
    logging.getLogger(log_name).setLevel(logging.CRITICAL)
    logging.getLogger(log_name).handlers.clear()
    logging.getLogger(log_name).propagate = False

def get_solana_price_usd():
    return str(load_solana_price_usd(logger=logging.getLogger(__name__)))


@dataclass(frozen=True)
class PumpFunGlobalState:
    fee_recipient: PublicKey
    initial_virtual_token_reserves: int
    initial_virtual_sol_reserves: int
    initial_real_token_reserves: int
    token_total_supply: int
    fee_basis_points: int
    creator_fee_basis_points: int
    create_v2_enabled: bool
    reserved_fee_recipient: PublicKey
    mayhem_mode_enabled: bool
    is_cashback_enabled: bool


@dataclass(frozen=True)
class PumpFunCreateQuote:
    spendable_sol_in: int
    expected_tokens_out: int
    min_tokens_out: int
    protocol_fee_bps: int
    creator_fee_bps: int
    total_fee_bps: int


def _guess_image_content_type(path: Path) -> str:
    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or "application/octet-stream"


def _encode_borsh_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack("<I", len(encoded)) + encoded


def _price_from_lamports_and_tokens(spendable_sol_in: int, token_balance: int) -> Optional[str]:
    if spendable_sol_in <= 0 or token_balance <= 0:
        return None

    price = (
        Decimal(spendable_sol_in)
        / Decimal(1_000_000_000)
        / (Decimal(token_balance) / Decimal(1_000_000))
    )
    rendered = format(price.normalize(), "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered

class PumpFun:
    def __init__(
        self,
        session: ClientSession,
        priv_key: str,
        async_client: AsyncClient,
        mev_config: Optional[MevConfig] = None,
    ):
        self.session = session
        self.priv_key = Keypair.from_bytes(
                base58.b58decode(str(priv_key))
            )
        self.async_client = async_client
        self.mev_config = mev_config

    def _derive_uva_pda(self, payer: PublicKey):
        user_acc, _ = PublicKey.find_program_address(
            [b"user_volume_accumulator", bytes(payer)], PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
        )
        return user_acc

    def priority_fee_accounts_for_buy(self, mint_address: str, bonding_curve_pda: str) -> list[str]:
        return [
            str(self.priv_key.pubkey()),
            str(mint_address),
            str(bonding_curve_pda),
            PUMP_FUN,
            GLOBAL_ACCOUNT,
            FEE_CONFIG,
            FEE_PROGRAM,
            ATA_PROGRAM,
            str(TOKEN_PROGRAM_ID),
            str(TOKEN_2022_PROGRAM_ID),
        ]

    def priority_fee_accounts_for_sell(self, mint_address: str, bonding_curve_pda: str) -> list[str]:
        return self.priority_fee_accounts_for_buy(mint_address, bonding_curve_pda)

    async def get_token_program_id(self, token_address: PublicKey) -> PublicKey:
        """
        Fetch the owner of the mint account to determine which token program it uses.
        """
        info = await self.async_client.get_account_info(token_address, commitment=Processed)
        account_info = getattr(info, "value", None)
        if account_info is None:
            raise Exception("account not found")

        owner = getattr(account_info, "owner", None)
        if owner is None and isinstance(account_info, dict):
            owner = account_info.get("owner")
        if owner is None:
            raise Exception("owner not found for token account")

        return PublicKey.from_string(str(owner))

    async def get_ata_auto(self, owner: PublicKey, mint: PublicKey) -> PublicKey:
        token_program_id = await self.get_token_program_id(mint)
        if token_program_id == TOKEN_2022_PROGRAM_ID:
            return self.get_ata_for_token2022(owner, mint)
        if token_program_id == LEGACY_TOKEN_PROGRAM_ID:
            return self.get_ata_for_token(owner, mint)
        raise Exception("Invalid token program id")

    def get_ata_for_token(self, owner: PublicKey, mint: PublicKey) -> PublicKey:
        return get_associated_token_address(owner, mint, LEGACY_TOKEN_PROGRAM_ID)

    def get_ata_for_token2022(self, owner: PublicKey, mint: PublicKey) -> PublicKey:
        return get_associated_token_address(owner, mint, TOKEN_2022_PROGRAM_ID)

    def get_bonding_curve_v2_pda(self, mint: PublicKey) -> PublicKey:
        bonding_curve_v2, _ = PublicKey.find_program_address(
            [b"bonding-curve-v2", bytes(mint)],
            PublicKey.from_string(PUMP_FUN)
        )
        return bonding_curve_v2

    def get_bonding_curve_pda(self, mint: PublicKey) -> PublicKey:
        bonding_curve, _ = PublicKey.find_program_address(
            [b"bonding-curve", bytes(mint)],
            PublicKey.from_string(PUMP_FUN)
        )
        return bonding_curve

    def get_mint_authority_pda(self) -> PublicKey:
        mint_authority, _ = PublicKey.find_program_address(
            [b"mint-authority"],
            PublicKey.from_string(PUMP_FUN)
        )
        return mint_authority

    def get_metadata_pda(self, mint: PublicKey) -> PublicKey:
        metadata, _ = PublicKey.find_program_address(
            [b"metadata", bytes(PublicKey.from_string(METADATA_PROGRAM)), bytes(mint)],
            PublicKey.from_string(METADATA_PROGRAM)
        )
        return metadata

    def get_mayhem_global_params_pda(self) -> PublicKey:
        global_params, _ = PublicKey.find_program_address(
            [b"global-params"],
            PublicKey.from_string(MAYHEM_PROGRAM)
        )
        return global_params

    def get_mayhem_sol_vault_pda(self) -> PublicKey:
        sol_vault, _ = PublicKey.find_program_address(
            [b"sol-vault"],
            PublicKey.from_string(MAYHEM_PROGRAM)
        )
        return sol_vault

    def get_mayhem_state_pda(self, mint: PublicKey) -> PublicKey:
        mayhem_state, _ = PublicKey.find_program_address(
            [b"mayhem-state", bytes(mint)],
            PublicKey.from_string(MAYHEM_PROGRAM)
        )
        return mayhem_state

    @staticmethod
    def _read_pubkey(data: bytes, offset: int) -> tuple[PublicKey, int]:
        end = offset + 32
        return PublicKey.from_bytes(data[offset:end]), end

    @staticmethod
    def _read_u64(data: bytes, offset: int) -> tuple[int, int]:
        return struct.unpack_from("<Q", data, offset)[0], offset + 8

    @staticmethod
    def _read_u32(data: bytes, offset: int) -> tuple[int, int]:
        return struct.unpack_from("<I", data, offset)[0], offset + 4

    @staticmethod
    def _read_u128(data: bytes, offset: int) -> tuple[int, int]:
        return int.from_bytes(data[offset:offset + 16], "little"), offset + 16

    async def fetch_global_state(self) -> PumpFunGlobalState:
        response = await self.async_client.get_account_info(
            PublicKey.from_string(GLOBAL_ACCOUNT),
            commitment=Processed,
        )
        account_info = getattr(response, "value", None)
        if account_info is None:
            raise ValueError("pump.fun global account not found")

        data = bytes(getattr(account_info, "data", b"") or b"")
        if len(data) < 8:
            raise ValueError("pump.fun global account is too short")

        body = data[8:]
        offset = 0
        offset += 1  # initialized
        offset += 32  # authority
        fee_recipient, offset = self._read_pubkey(body, offset)
        initial_virtual_token_reserves, offset = self._read_u64(body, offset)
        initial_virtual_sol_reserves, offset = self._read_u64(body, offset)
        initial_real_token_reserves, offset = self._read_u64(body, offset)
        token_total_supply, offset = self._read_u64(body, offset)
        fee_basis_points, offset = self._read_u64(body, offset)
        offset += 32  # withdraw_authority
        offset += 1  # enable_migrate
        offset += 8  # pool_migration_fee
        creator_fee_basis_points, offset = self._read_u64(body, offset)
        offset += 32 * 7  # fee_recipients
        offset += 32  # set_creator_authority
        offset += 32  # admin_set_creator_authority
        create_v2_enabled = bool(body[offset]) if offset < len(body) else False
        offset += 1
        offset += 32  # whitelist_pda
        reserved_fee_recipient, offset = self._read_pubkey(body, offset)
        mayhem_mode_enabled = bool(body[offset]) if offset < len(body) else False
        offset += 1
        offset += 32 * 7  # reserved_fee_recipients
        is_cashback_enabled = bool(body[offset]) if offset < len(body) else False

        return PumpFunGlobalState(
            fee_recipient=fee_recipient,
            initial_virtual_token_reserves=int(initial_virtual_token_reserves),
            initial_virtual_sol_reserves=int(initial_virtual_sol_reserves),
            initial_real_token_reserves=int(initial_real_token_reserves),
            token_total_supply=int(token_total_supply),
            fee_basis_points=int(fee_basis_points),
            creator_fee_basis_points=int(creator_fee_basis_points),
            create_v2_enabled=bool(create_v2_enabled),
            reserved_fee_recipient=reserved_fee_recipient,
            mayhem_mode_enabled=bool(mayhem_mode_enabled),
            is_cashback_enabled=bool(is_cashback_enabled),
        )

    async def resolve_create_fee_bps(self, global_state: PumpFunGlobalState) -> tuple[int, int]:
        protocol_fee_bps = int(global_state.fee_basis_points)
        creator_fee_bps = int(global_state.creator_fee_basis_points)

        response = await self.async_client.get_account_info(
            PublicKey.from_string(FEE_CONFIG),
            commitment=Processed,
        )
        account_info = getattr(response, "value", None)
        if account_info is None:
            return protocol_fee_bps, creator_fee_bps

        data = bytes(getattr(account_info, "data", b"") or b"")
        if len(data) < 8:
            return protocol_fee_bps, creator_fee_bps

        body = data[8:]
        offset = 0
        offset += 1  # bump
        offset += 32  # admin
        offset += 8 * 3  # flat_fees
        if len(body) < offset + 4:
            return protocol_fee_bps, creator_fee_bps

        tier_count, offset = self._read_u32(body, offset)
        if tier_count <= 0:
            return protocol_fee_bps, creator_fee_bps

        tiers: list[tuple[int, int, int]] = []
        for _ in range(int(tier_count)):
            if len(body) < offset + 16 + 8 + 8 + 8:
                return protocol_fee_bps, creator_fee_bps
            threshold, offset = self._read_u128(body, offset)
            offset += 8  # lp_fee_bps
            tier_protocol_fee_bps, offset = self._read_u64(body, offset)
            tier_creator_fee_bps, offset = self._read_u64(body, offset)
            tiers.append(
                (
                    int(threshold),
                    int(tier_protocol_fee_bps),
                    int(tier_creator_fee_bps),
                )
            )

        if not tiers or global_state.initial_virtual_token_reserves <= 0:
            return protocol_fee_bps, creator_fee_bps

        market_cap_lamports = (
            int(global_state.initial_virtual_sol_reserves)
            * int(global_state.token_total_supply)
        ) // int(global_state.initial_virtual_token_reserves)
        selected = tiers[0]
        if market_cap_lamports >= tiers[0][0]:
            for candidate in tiers:
                if market_cap_lamports >= candidate[0]:
                    selected = candidate
                else:
                    break

        return selected[1], selected[2]

    async def quote_create_buy_exact_sol_in(
        self,
        spendable_sol_in: int,
        *,
        slippage_pct: float,
        global_state: Optional[PumpFunGlobalState] = None,
    ) -> PumpFunCreateQuote:
        if spendable_sol_in <= 0:
            raise ValueError("buy_sol_lamports must be > 0")

        global_state = global_state or await self.fetch_global_state()
        protocol_fee_bps, creator_fee_bps = await self.resolve_create_fee_bps(global_state)
        total_fee_bps = min(10_000, int(protocol_fee_bps) + int(creator_fee_bps))

        slippage_pct = float(slippage_pct)
        if slippage_pct <= 1.0:
            slippage_pct *= 100.0
        if slippage_pct < 0 or slippage_pct > 99:
            raise ValueError("slippage_pct must be in [0, 99]")

        scale = 10_000
        net_sol = (int(spendable_sol_in) * scale) // (scale + total_fee_bps)
        protocol_fee = (net_sol * int(protocol_fee_bps) + (scale - 1)) // scale
        creator_fee = (net_sol * int(creator_fee_bps) + (scale - 1)) // scale
        if net_sol + protocol_fee + creator_fee > int(spendable_sol_in):
            net_sol -= net_sol + protocol_fee + creator_fee - int(spendable_sol_in)

        if net_sol <= 1:
            raise ValueError("buy_sol_lamports too small after fees")

        virtual_sol_reserves = int(global_state.initial_virtual_sol_reserves)
        virtual_token_reserves = int(global_state.initial_virtual_token_reserves)
        if virtual_sol_reserves <= 0 or virtual_token_reserves <= 0:
            raise ValueError("pump.fun global reserves are invalid")

        expected_tokens_out = ((net_sol - 1) * virtual_token_reserves) // (
            virtual_sol_reserves + net_sol - 1
        )
        expected_tokens_out = min(
            int(expected_tokens_out),
            int(global_state.initial_real_token_reserves),
        )
        if expected_tokens_out <= 0:
            raise ValueError("buy_sol_lamports is too small for any token amount")

        slippage_bps = int(slippage_pct * 100)
        min_tokens_out = (expected_tokens_out * (scale - slippage_bps)) // scale
        if min_tokens_out <= 0:
            raise ValueError("buy_sol_lamports is too small after slippage")

        return PumpFunCreateQuote(
            spendable_sol_in=int(spendable_sol_in),
            expected_tokens_out=int(expected_tokens_out),
            min_tokens_out=int(min_tokens_out),
            protocol_fee_bps=int(protocol_fee_bps),
            creator_fee_bps=int(creator_fee_bps),
            total_fee_bps=int(total_fee_bps),
        )

    async def upload_token_metadata(
        self,
        *,
        name: str,
        symbol: str,
        description: str,
        image_path: str,
        twitter: Optional[str] = None,
        telegram: Optional[str] = None,
        website: Optional[str] = None,
        show_name: bool = False,
        upload_url: str = DEFAULT_PUMP_FUN_IPFS_UPLOAD_URL,
    ) -> dict[str, Any]:
        image = Path(image_path).expanduser()
        if not image.is_file():
            raise FileNotFoundError(f"image file not found: {image}")

        form = FormData()
        with image.open("rb") as image_file:
            form.add_field(
                "file",
                image_file.read(),
                filename=image.name,
                content_type=_guess_image_content_type(image),
            )
        form.add_field("name", name)
        form.add_field("symbol", symbol)
        form.add_field("description", description)
        form.add_field("showName", "true" if show_name else "false")

        for key, value in {
            "twitter": twitter,
            "telegram": telegram,
            "website": website,
        }.items():
            if value and str(value).strip():
                form.add_field(key, str(value).strip())

        async with self.session.post(upload_url, data=form) as response:
            body = await response.text()
            if response.status >= 400:
                raise ValueError(f"pump.fun IPFS upload failed ({response.status}): {body}")

        payload = json.loads(body)
        metadata_uri = payload.get("metadataUri") or payload.get("metadata_uri")
        if not metadata_uri:
            raise ValueError("pump.fun IPFS upload response did not include metadataUri")

        return {
            "metadata_uri": str(metadata_uri),
            "metadata": payload.get("metadata") or {},
            "raw": payload,
        }

    def build_create_instruction(
        self,
        *,
        mint: PublicKey,
        name: str,
        symbol: str,
        uri: str,
        is_mayhem_mode: bool,
    ) -> Instruction:
        payer = self.priv_key.pubkey()
        mint_authority = self.get_mint_authority_pda()
        bonding_curve = self.get_bonding_curve_pda(mint)
        event_authority = PublicKey.from_string(EVENT_AUTHORITY)

        if is_mayhem_mode:
            associated_bonding_curve = get_associated_token_address(
                bonding_curve,
                mint,
                TOKEN_2022_PROGRAM_ID,
            )
            global_params = self.get_mayhem_global_params_pda()
            sol_vault = self.get_mayhem_sol_vault_pda()
            mayhem_state = self.get_mayhem_state_pda(mint)
            mayhem_token_vault = get_associated_token_address(
                sol_vault,
                mint,
                TOKEN_2022_PROGRAM_ID,
            )
            instruction_data = (
                CREATE_V2_DISCRIMINATOR
                + _encode_borsh_string(name)
                + _encode_borsh_string(symbol)
                + _encode_borsh_string(uri)
                + bytes(payer)
                + bytes([1, 0])
            )
            return Instruction(
                program_id=PublicKey.from_string(PUMP_FUN),
                accounts=[
                    AccountMeta(pubkey=mint, is_signer=True, is_writable=True),
                    AccountMeta(pubkey=mint_authority, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=bonding_curve, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=associated_bonding_curve, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=PublicKey.from_string(GLOBAL_ACCOUNT), is_signer=False, is_writable=False),
                    AccountMeta(pubkey=payer, is_signer=True, is_writable=True),
                    AccountMeta(pubkey=PublicKey.from_string("11111111111111111111111111111111"), is_signer=False, is_writable=False),
                    AccountMeta(pubkey=TOKEN_2022_PROGRAM_ID, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=PublicKey.from_string(ATA_PROGRAM), is_signer=False, is_writable=False),
                    AccountMeta(pubkey=PublicKey.from_string(MAYHEM_PROGRAM), is_signer=False, is_writable=True),
                    AccountMeta(pubkey=global_params, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=sol_vault, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=mayhem_state, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=mayhem_token_vault, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=event_authority, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=PublicKey.from_string(PUMP_FUN), is_signer=False, is_writable=False),
                ],
                data=instruction_data,
            )

        associated_bonding_curve = get_associated_token_address(
            bonding_curve,
            mint,
            LEGACY_TOKEN_PROGRAM_ID,
        )
        metadata = self.get_metadata_pda(mint)
        instruction_data = (
            CREATE_DISCRIMINATOR
            + _encode_borsh_string(name)
            + _encode_borsh_string(symbol)
            + _encode_borsh_string(uri)
            + bytes(payer)
        )
        return Instruction(
            program_id=PublicKey.from_string(PUMP_FUN),
            accounts=[
                AccountMeta(pubkey=mint, is_signer=True, is_writable=True),
                AccountMeta(pubkey=mint_authority, is_signer=False, is_writable=False),
                AccountMeta(pubkey=bonding_curve, is_signer=False, is_writable=True),
                AccountMeta(pubkey=associated_bonding_curve, is_signer=False, is_writable=True),
                AccountMeta(pubkey=PublicKey.from_string(GLOBAL_ACCOUNT), is_signer=False, is_writable=False),
                AccountMeta(pubkey=PublicKey.from_string(METADATA_PROGRAM), is_signer=False, is_writable=False),
                AccountMeta(pubkey=metadata, is_signer=False, is_writable=True),
                AccountMeta(pubkey=payer, is_signer=True, is_writable=True),
                AccountMeta(pubkey=PublicKey.from_string("11111111111111111111111111111111"), is_signer=False, is_writable=False),
                AccountMeta(pubkey=LEGACY_TOKEN_PROGRAM_ID, is_signer=False, is_writable=False),
                AccountMeta(pubkey=PublicKey.from_string(ATA_PROGRAM), is_signer=False, is_writable=False),
                AccountMeta(pubkey=PublicKey.from_string(RENT_SYSVAR), is_signer=False, is_writable=False),
                AccountMeta(pubkey=event_authority, is_signer=False, is_writable=False),
                AccountMeta(pubkey=PublicKey.from_string(PUMP_FUN), is_signer=False, is_writable=False),
            ],
            data=instruction_data,
        )

    def build_extend_account_instruction(
        self,
        *,
        bonding_curve: PublicKey,
    ) -> Instruction:
        return Instruction(
            program_id=PublicKey.from_string(PUMP_FUN),
            accounts=[
                AccountMeta(pubkey=bonding_curve, is_signer=False, is_writable=True),
                AccountMeta(pubkey=self.priv_key.pubkey(), is_signer=True, is_writable=True),
                AccountMeta(pubkey=PublicKey.from_string("11111111111111111111111111111111"), is_signer=False, is_writable=False),
                AccountMeta(pubkey=PublicKey.from_string(EVENT_AUTHORITY), is_signer=False, is_writable=False),
                AccountMeta(pubkey=PublicKey.from_string(PUMP_FUN), is_signer=False, is_writable=False),
            ],
            data=EXTEND_ACCOUNT_DISCRIMINATOR,
        )

    def get_create_fee_recipient(self, global_state: PumpFunGlobalState) -> PublicKey:
        if not global_state.mayhem_mode_enabled:
            return global_state.fee_recipient
        if any(bytes(global_state.reserved_fee_recipient)):
            return global_state.reserved_fee_recipient
        return PublicKey.from_string(MAYHEM_FEE_RECIPIENTS[0])

    def get_fee_recipient(self, curve_state) -> PublicKey:
        if curve_state is not None and getattr(curve_state, "is_mayhem_mode", False):
            return PublicKey.from_string(MAYHEM_FEE_RECIPIENTS[0])
        return PublicKey.from_string(DEFAULT_FEE_RECIPIENT)

    def quote_buy_exact_sol_in_tokens_out(self, curve_state, spendable_sol_in: int) -> int:
        if curve_state is None:
            return 0

        virtual_sol_reserves = int(getattr(curve_state, "virtual_sol_reserves", 0))
        virtual_token_reserves = int(getattr(curve_state, "virtual_token_reserves", 0))
        if spendable_sol_in <= 0 or virtual_sol_reserves <= 0 or virtual_token_reserves <= 0:
            return 0

        total_fee_bps = DEFAULT_BUY_QUOTE_FEE_BPS
        net_sol = (spendable_sol_in * 10_000) // (10_000 + total_fee_bps)
        fees = (net_sol * total_fee_bps + 9_999) // 10_000
        if net_sol + fees > spendable_sol_in:
            net_sol -= (net_sol + fees - spendable_sol_in)

        if net_sol <= 1:
            return 0

        return ((net_sol - 1) * virtual_token_reserves) // (virtual_sol_reserves + net_sol - 1)

    async def get_curve_context(
        self,
        bonding_curve: PublicKey,
        fallback_creator: Optional[str] = None,
        allow_creator_fallback: bool = False,
        retries: int = 8,
        retry_delay_seconds: float = 0.15,
    ):
        curve_state = None

        for attempt in range(retries):
            curve_state = await get_bonding_curve_state(self.async_client, bonding_curve)
            if curve_state is not None:
                if getattr(curve_state, "complete", False):
                    return curve_state, None

                creator = getattr(curve_state, "creator", None)
                if creator and any(creator):
                    creator_vault = self.get_creator_vault(
                        base58.b58encode(creator).decode("utf-8")
                    )
                    return curve_state, creator_vault

            if attempt < retries - 1:
                await asyncio.sleep(retry_delay_seconds)

        if allow_creator_fallback and fallback_creator:
            logging.warning(
                "Falling back to the caller-provided creator after retries failed to resolve the on-chain creator."
            )
            return curve_state, self.get_creator_vault(fallback_creator)

        raise ValueError("Unable to resolve creator_vault from the bonding curve state after retries.")

    def build_buy_accounts(
        self,
        mint: PublicKey,
        bonding_curve: PublicKey,
        fee_recipient: PublicKey,
        creator_vault: PublicKey,
        token_program_id: PublicKey
    ) -> List[AccountMeta]:
        buyer = self.priv_key.pubkey()
        return [
            AccountMeta(pubkey=PublicKey.from_string("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"), is_signer=False, is_writable=False), # global
            AccountMeta(pubkey=fee_recipient, is_signer=False, is_writable=True),  # feeRecipient
            AccountMeta(pubkey=mint, is_signer=False, is_writable=False),         # mint
            AccountMeta(pubkey=bonding_curve, is_signer=False, is_writable=True), # bondingCurve
            AccountMeta(
                pubkey=get_associated_token_address(bonding_curve, mint, token_program_id),
                is_signer=False,
                is_writable=True
            ),                                                                    # associatedBondingCurve
            AccountMeta(
                pubkey=get_associated_token_address(buyer, mint, token_program_id),
                is_signer=False,
                is_writable=True
            ),                                                                    # associatedUser
            AccountMeta(pubkey=buyer, is_signer=True, is_writable=True),         # user
            AccountMeta(pubkey=PublicKey.from_string("11111111111111111111111111111111"), is_signer=False, is_writable=False), # systemProgram
            AccountMeta(pubkey=token_program_id, is_signer=False, is_writable=False), # tokenProgram
            AccountMeta(pubkey=creator_vault, is_signer=False, is_writable=True), # creatorVault
            AccountMeta(pubkey=PublicKey.from_string("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"), is_signer=False, is_writable=False), # eventAuthority
            AccountMeta(pubkey=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"), is_signer=False, is_writable=False),   # program
            AccountMeta(pubkey=PublicKey.from_string(GLOBAL_VOLUME_ACCUMULATOR), is_signer=False, is_writable=True), # globalVolumeAccumulator
            AccountMeta(pubkey=self._derive_uva_pda(buyer), is_signer=False, is_writable=True), # userVolumeAccumulator
            AccountMeta(pubkey=PublicKey.from_string(FEE_CONFIG), is_signer=False, is_writable=False), # feeConfig
            AccountMeta(pubkey=PublicKey.from_string(FEE_PROGRAM), is_signer=False, is_writable=False), # feeProgram
        ]

    async def build_buy_instruction(
        self,
        mint: PublicKey,
        bonding_curve: PublicKey,
        fee_recipient: PublicKey,
        token_amount: int,      # how many tokens to buy
        lamports_budget: int,    # how many lamports to spend
        creator_vault: PublicKey,
        token_program_id: PublicKey
    ) -> Instruction:
        instruction_data = (
            BUY_DISCRIMINATOR
            + BUY_INSTRUCTION_SCHEMA.build({
                "amount": token_amount,
                "max_sol_cost": lamports_budget
            })
            + b"\x01\x01"
        )
        accounts = self.build_buy_accounts(
            mint=mint,
            bonding_curve=bonding_curve,
            fee_recipient=fee_recipient,
            creator_vault=creator_vault,
            token_program_id=token_program_id,
        )
        accounts.append(
            AccountMeta(
                pubkey=self.get_bonding_curve_v2_pda(mint),
                is_signer=False,
                is_writable=False
            )
        )

        return Instruction(
            program_id=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
            accounts=accounts,
            data=instruction_data
        )

    async def build_buy_exact_sol_in_instruction(
        self,
        mint: PublicKey,
        bonding_curve: PublicKey,
        fee_recipient: PublicKey,
        spendable_sol_in: int,
        min_tokens_out: int,
        creator_vault: PublicKey,
        token_program_id: PublicKey
    ) -> Instruction:
        # OptionBool::None is encoded as a single 0 byte.
        instruction_data = (
            BUY_EXACT_SOL_IN_DISCRIMINATOR
            + struct.pack("<QQ", spendable_sol_in, min_tokens_out)
            + b"\x00"
        )

        accounts = self.build_buy_accounts(
            mint=mint,
            bonding_curve=bonding_curve,
            fee_recipient=fee_recipient,
            creator_vault=creator_vault,
            token_program_id=token_program_id,
        )
        accounts.append(
            AccountMeta(
                pubkey=self.get_bonding_curve_v2_pda(mint),
                is_signer=False,
                is_writable=False
            )
        )

        return Instruction(
            program_id=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
            accounts=accounts,
            data=instruction_data
        )

    async def build_sell_instruction(
        self,
        mint: PublicKey,
        bonding_curve: PublicKey,
        fee_recipient: PublicKey,
        token_amount: int,       # how many tokens to sell
        lamports_min_output: int, # minimum lamports you want to receive
        vault: PublicKey,
        token_program_id: PublicKey,
        user_token_account: Optional[PublicKey] = None,
        cashback: bool = False
    ) -> Instruction:
        instruction_data = SELL_DISCRIMINATOR + SELL_INSTRUCTION_SCHEMA.build({
            "amount": token_amount,
            "min_sol_output": lamports_min_output
        })

        user = self.priv_key.pubkey()
        user_token_account = user_token_account or get_associated_token_address(user, mint, token_program_id)

        # The IDL's account list for sell:
        accounts = [
            AccountMeta(pubkey=PublicKey.from_string("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"), is_signer=False, is_writable=False),  # global
            AccountMeta(pubkey=fee_recipient, is_signer=False, is_writable=True),  # feeRecipient
            AccountMeta(pubkey=mint, is_signer=False, is_writable=False),          # mint
            AccountMeta(pubkey=bonding_curve, is_signer=False, is_writable=True),  # bondingCurve
            AccountMeta(
                pubkey=get_associated_token_address(bonding_curve, mint, token_program_id),
                is_signer=False,
                is_writable=True
            ),                                                                     # associatedBondingCurve
            AccountMeta(
                pubkey=user_token_account,
                is_signer=False,
                is_writable=True
            ),                                                                     # associatedUser
            AccountMeta(pubkey=user, is_signer=True, is_writable=True),           # user
            AccountMeta(pubkey=PublicKey.from_string("11111111111111111111111111111111"), is_signer=False, is_writable=False), # systemProgram
            AccountMeta(pubkey=PublicKey.from_string(str(vault)), is_signer=False, is_writable=True),  # vault
            AccountMeta(pubkey=token_program_id, is_signer=False, is_writable=False), # tokenProgram
            AccountMeta(pubkey=PublicKey.from_string("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"), is_signer=False, is_writable=False),  # eventAuthority
            AccountMeta(pubkey=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"), is_signer=False, is_writable=False),    # program
            AccountMeta(pubkey=PublicKey.from_string(FEE_CONFIG), is_signer=False, is_writable=False), # feeConfig
            AccountMeta(pubkey=PublicKey.from_string(FEE_PROGRAM), is_signer=False, is_writable=False), # feeProgram
        ]
        if cashback:
            accounts.append(
                AccountMeta(pubkey=self._derive_uva_pda(user), is_signer=False, is_writable=True)
            )
        accounts.append(
            AccountMeta(pubkey=self.get_bonding_curve_v2_pda(mint), is_signer=False, is_writable=False)
        )

        return Instruction(
            program_id=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
            accounts=accounts,
            data=instruction_data
        )

    async def check_ata_exists(self, owner: PublicKey, mint: PublicKey, token_program_id: PublicKey) -> bool:
        """
        Check if the associated token account (ATA) exists on-chain.
        """
        ata_address = get_associated_token_address(owner, mint, token_program_id)

        try:
            response = await self.async_client.get_account_info(ata_address, commitment=Processed)
            if response.value:
                return True
            else:
                return False
        except Exception as e:
            logging.error(f"Error checking ATA existence: {e}")
            return False

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
        except Exception as e:
            logging.warning("Failed to read ATA %s for %s: %s", expected_ata, mint, e)

        try:
            response = await self.async_client.get_token_accounts_by_owner_json_parsed(
                owner,
                TokenAccountOpts(mint=mint, program_id=token_program_id),
                commitment=Processed,
            )
        except Exception as e:
            logging.error("Error resolving token accounts for %s: %s", mint, e)
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
                token_amount = int(
                    parsed.get("info", {}).get("tokenAmount", {}).get("amount", 0) or 0
                )

            if token_amount > 0:
                resolved = PublicKey.from_string(str(account_pubkey))
                if resolved != expected_ata:
                    logging.warning(
                        "Using token account %s for sell of %s because the ATA lookup was unavailable.",
                        resolved,
                        mint,
                    )
                return resolved

        if fallback_account is not None and fallback_account != expected_ata:
            logging.warning(
                "Falling back to token account %s for sell of %s after ATA lookup missed.",
                fallback_account,
                mint,
            )
        return fallback_account

    async def make_check_ata(self, instructions: list, mint_address: PublicKey, token_program_id: PublicKey):
        """
        Check if the Associated Token Account (ATA) exists.
        If it doesn't, add an instruction to create it for the provided token program.
        """
        owner = self.priv_key.pubkey()
        
        # Check if the ATA exists
        ata_exists = await self.check_ata_exists(owner, mint_address, token_program_id)
        
        if not ata_exists:
            instructions.append(
                create_compatible_idempotent_associated_token_account(
                    payer=owner,
                    owner=owner,
                    mint=mint_address,
                    token_program_id=token_program_id,
                )
            )

        return instructions

    def get_creator_vault(self, creator):
        creator_vault_pda, _ = PublicKey.find_program_address(
            [b"creator-vault", bytes(PublicKey.from_string(creator))],
            PublicKey.from_string(PUMP_FUN)
        )
        return creator_vault_pda

    async def _compile_transaction(
        self,
        instructions: list[Instruction | Transaction],
        *,
        additional_signers: Optional[list[Keypair]] = None,
    ):
        try:
            latest_blockhash = (await self.async_client.get_latest_blockhash(commitment=Processed)).value.blockhash
            msg = MessageV0.try_compile(
                payer=self.priv_key.pubkey(),
                instructions=instructions,
                address_lookup_table_accounts=[],
                recent_blockhash=latest_blockhash
            )
            signers = [self.priv_key]
            if additional_signers:
                signers.extend(additional_signers)
            return VersionedTransaction(msg, signers)
        except Exception as e:
            logging.error(f"Failed to fetch latest blockhash: {e}")
            raise

    async def _simulate_or_send(
        self,
        tx: VersionedTransaction,
        sim: bool = False,
        *,
        use_mev: bool = False,
    ):
        try:
            if sim:
                return await self.async_client.simulate_transaction(
                    tx,
                    sig_verify=False,
                    commitment=Processed
                )

            if use_mev and self.mev_config is not None:
                return await submit_signed_transaction_via_mev(tx, self.mev_config)

            opts = TxOpts(skip_preflight=True, skip_confirmation=True)
            result = await self.async_client.send_transaction(tx, opts=opts)
            result_json = result.to_json()
            return json.loads(result_json).get('result')
        except Exception as e:
            logging.error(f"Transaction failed: {e}")
            raise

    async def _token_balance_raw(self, token_account: PublicKey) -> int:
        response = await self.async_client.get_token_account_balance(
            token_account,
            commitment=Processed,
        )
        value = getattr(response, "value", None)
        amount = getattr(value, "amount", None)
        if amount is None and isinstance(value, dict):
            amount = value.get("amount")
        return int(amount or 0)

    async def _confirm_signature(
        self,
        signature: str,
        *,
        retries: int = 20,
        delay_seconds: float = 1.0,
    ) -> bool:
        solders_signature = Signature.from_string(signature)
        for _ in range(retries):
            response = await self.async_client.get_signature_statuses([solders_signature])
            statuses = getattr(response, "value", None) or []
            status = statuses[0] if statuses else None
            if status is not None:
                err = getattr(status, "err", None)
                confirmation_status = getattr(status, "confirmation_status", None)
                confirmations = getattr(status, "confirmations", None)
                confirmation_status_text = ""
                if confirmation_status is not None:
                    confirmation_status_text = str(confirmation_status).split(".")[-1].lower()
                if err is not None:
                    raise ValueError(f"create transaction failed: {err}")
                if confirmation_status_text in {"confirmed", "finalized"} or confirmations is not None:
                    return True
            await asyncio.sleep(delay_seconds)
        return False

    async def build_create_transaction(
        self,
        *,
        name: str,
        symbol: str,
        uri: str,
        buy_sol_lamports: int = 0,
        slippage_pct: float = 15.0,
        priority_micro_lamports: int = 0,
        mint_keypair: Optional[Keypair] = None,
        global_state: Optional[PumpFunGlobalState] = None,
    ) -> tuple[VersionedTransaction, dict[str, Any]]:
        if not name or not symbol:
            raise ValueError("name and symbol are required")
        if not uri:
            raise ValueError("uri is required")

        global_state = global_state or await self.fetch_global_state()
        is_mayhem_mode = bool(global_state.mayhem_mode_enabled)
        token_program_id = TOKEN_2022_PROGRAM_ID if is_mayhem_mode else LEGACY_TOKEN_PROGRAM_ID
        payer = self.priv_key.pubkey()
        mint_keypair = mint_keypair or Keypair()
        mint = mint_keypair.pubkey()
        bonding_curve = self.get_bonding_curve_pda(mint)
        associated_user = get_associated_token_address(payer, mint, token_program_id)
        creator_vault = self.get_creator_vault(str(payer))
        fee_recipient = self.get_create_fee_recipient(global_state)

        instructions: list[Instruction | Transaction] = []
        if priority_micro_lamports > 0:
            instructions.append(set_compute_unit_price(priority_micro_lamports))

        if buy_sol_lamports > 0:
            instructions.append(set_compute_unit_limit(800_000))

        instructions.append(
            self.build_create_instruction(
                mint=mint,
                name=name,
                symbol=symbol,
                uri=uri,
                is_mayhem_mode=is_mayhem_mode,
            )
        )
        if is_mayhem_mode or buy_sol_lamports > 0:
            instructions.append(
                self.build_extend_account_instruction(
                    bonding_curve=bonding_curve,
                )
            )

        quote = None
        if buy_sol_lamports > 0:
            quote = await self.quote_create_buy_exact_sol_in(
                buy_sol_lamports,
                slippage_pct=slippage_pct,
                global_state=global_state,
            )
            instructions.append(
                create_compatible_idempotent_associated_token_account(
                    payer=payer,
                    owner=payer,
                    mint=mint,
                    token_program_id=token_program_id,
                )
            )
            instructions.append(
                await self.build_buy_exact_sol_in_instruction(
                    mint=mint,
                    bonding_curve=bonding_curve,
                    fee_recipient=fee_recipient,
                    spendable_sol_in=int(quote.spendable_sol_in),
                    min_tokens_out=int(quote.min_tokens_out),
                    creator_vault=creator_vault,
                    token_program_id=token_program_id,
                )
            )

        tx = await self._compile_transaction(
            instructions,
            additional_signers=[mint_keypair],
        )
        plan = {
            "mint": str(mint),
            "owner": str(payer),
            "bonding_curve": str(bonding_curve),
            "associated_user": str(associated_user),
            "creator_vault": str(creator_vault),
            "fee_recipient": str(fee_recipient),
            "market": "pump_fun",
            "token_program": str(token_program_id),
            "is_mayhem_mode": is_mayhem_mode,
            "create_variant": "create_v2" if is_mayhem_mode else "create",
            "buy_sol_lamports": int(buy_sol_lamports),
            "metadata_uri": uri,
            "quote": {
                "spendable_sol_in": int(quote.spendable_sol_in),
                "expected_tokens_out": int(quote.expected_tokens_out),
                "min_tokens_out": int(quote.min_tokens_out),
                "protocol_fee_bps": int(quote.protocol_fee_bps),
                "creator_fee_bps": int(quote.creator_fee_bps),
                "total_fee_bps": int(quote.total_fee_bps),
            } if quote is not None else {
                "spendable_sol_in": 0,
                "expected_tokens_out": 0,
                "min_tokens_out": 0,
                "protocol_fee_bps": 0,
                "creator_fee_bps": 0,
                "total_fee_bps": 0,
            },
        }
        return tx, plan

    async def pump_create_token(
        self,
        *,
        name: str,
        symbol: str,
        uri: Optional[str] = None,
        image_path: Optional[str] = None,
        description: str = "",
        twitter: Optional[str] = None,
        telegram: Optional[str] = None,
        website: Optional[str] = None,
        show_name: bool = False,
        buy_sol_lamports: int = 0,
        slippage_pct: float = 15.0,
        priority_micro_lamports: int = 0,
        sim: bool = False,
        send: bool = False,
        mint_keypair: Optional[Keypair] = None,
        upload_url: str = DEFAULT_PUMP_FUN_IPFS_UPLOAD_URL,
    ) -> dict[str, Any]:
        if uri and image_path:
            raise ValueError("uri and image_path are mutually exclusive")
        if not uri and not image_path:
            raise ValueError("either uri or image_path is required")
        if sim and send:
            raise ValueError("sim and send are mutually exclusive")

        metadata_upload = None
        if image_path:
            metadata_upload = await self.upload_token_metadata(
                name=name,
                symbol=symbol,
                description=description,
                image_path=image_path,
                twitter=twitter,
                telegram=telegram,
                website=website,
                show_name=show_name,
                upload_url=upload_url,
            )
            uri = str(metadata_upload["metadata_uri"])

        tx, plan = await self.build_create_transaction(
            name=name,
            symbol=symbol,
            uri=str(uri),
            buy_sol_lamports=buy_sol_lamports,
            slippage_pct=slippage_pct,
            priority_micro_lamports=priority_micro_lamports,
            mint_keypair=mint_keypair,
        )

        result: dict[str, Any] = {
            **plan,
            "mode": "build",
            "metadata_upload": metadata_upload,
        }
        expected_tokens_out = int(result["quote"]["expected_tokens_out"])
        if buy_sol_lamports > 0 and expected_tokens_out > 0:
            result["expected_buy_price"] = _price_from_lamports_and_tokens(
                buy_sol_lamports,
                expected_tokens_out,
            )

        if sim:
            simulation = await self._simulate_or_send(tx, sim=True)
            result["mode"] = "simulate"
            result["simulation"] = json.loads(simulation.to_json())
            return result

        if not send:
            return result

        signature = await self._simulate_or_send(tx, sim=False)
        confirmed = await self._confirm_signature(str(signature))
        token_balance = 0
        if buy_sol_lamports > 0 and confirmed:
            token_balance = await self._token_balance_raw(
                PublicKey.from_string(result["associated_user"])
            )
        if token_balance <= 0:
            token_balance = expected_tokens_out

        result.update(
            {
                "mode": "send",
                "signature": str(signature),
                "confirmed": bool(confirmed),
                "token_balance_raw": int(token_balance),
            }
        )
        if buy_sol_lamports > 0 and token_balance > 0:
            result["buy_price"] = _price_from_lamports_and_tokens(
                buy_sol_lamports,
                token_balance,
            )
        return result

    async def pump_buy(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            sol_amount: int,
            creator: str,
            token_amount: int = 0,
            sim: bool = False,
            priority_micro_lamports: int = 0,
            slippage: float = 1.3, # MAX: 1.99
            skip_ata_check: bool = False,
        ):

        instructions = []

        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)

        try:
            curve_state, creator_vault = await self.get_curve_context(bonding_curve_pda)
        except ValueError as exc:
            logging.warning(f"Skipping buy for {mint_address}: {exc}")
            return "creator_vault_unavailable"
        if curve_state is not None and getattr(curve_state, "complete", False):
            return "migrated"

        if token_amount <= 0:
            token_amount = self.quote_buy_exact_sol_in_tokens_out(curve_state, sol_amount)
            if token_amount <= 0:
                return "zero_quote"

        token_program_id = await self.get_token_program_id(mint_address)

        if priority_micro_lamports > 0:
            instructions.append(set_compute_unit_limit(DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS))
            instructions.append(set_compute_unit_price(priority_micro_lamports))
        if not sim and self.mev_config is not None:
            mev_tip_ix = build_mev_tip_instruction(self.priv_key.pubkey(), self.mev_config)
            if mev_tip_ix is not None:
                instructions.append(mev_tip_ix)

        if not skip_ata_check:
            instructions = await self.make_check_ata(instructions, mint_address, token_program_id)

        fee_recipient = self.get_fee_recipient(curve_state)
        buy_ix = await self.build_buy_instruction(
            mint_address,
            bonding_curve_pda,
            fee_recipient,
            token_amount,
            # slippage, 1.99x
            int(sol_amount * slippage),
            creator_vault,
            token_program_id
        )
        instructions.append(buy_ix)

        tx = await self._compile_transaction(instructions)
        return await self._simulate_or_send(tx, sim=sim, use_mev=not sim and self.mev_config is not None)

    async def pump_buy_exact_sol_in(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            sol_amount: int,
            creator: Optional[str] = None,
            sim: bool = False,
            priority_micro_lamports: int = 0,
            slippage: float = 1.3,
            skip_ata_check: bool = False,
        ):

        if sol_amount <= 0:
            return "zero_sol_amount"

        instructions = []

        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)

        try:
            curve_state, creator_vault = await self.get_curve_context(bonding_curve_pda)
        except ValueError as exc:
            logging.warning(f"Skipping buy_exact_sol_in for {mint_address}: {exc}")
            return "creator_vault_unavailable"
        if curve_state is not None and getattr(curve_state, "complete", False):
            return "migrated"

        quote_tokens_out = self.quote_buy_exact_sol_in_tokens_out(curve_state, sol_amount)
        min_tokens_out = int(quote_tokens_out / slippage) if slippage > 0 else 0
        if min_tokens_out <= 0:
            return "zero_quote"

        token_program_id = await self.get_token_program_id(mint_address)

        if priority_micro_lamports > 0:
            instructions.append(set_compute_unit_limit(DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS))
            instructions.append(set_compute_unit_price(priority_micro_lamports))
        if not sim and self.mev_config is not None:
            mev_tip_ix = build_mev_tip_instruction(self.priv_key.pubkey(), self.mev_config)
            if mev_tip_ix is not None:
                instructions.append(mev_tip_ix)

        if not skip_ata_check:
            instructions = await self.make_check_ata(instructions, mint_address, token_program_id)

        fee_recipient = self.get_fee_recipient(curve_state)
        buy_ix = await self.build_buy_exact_sol_in_instruction(
            mint=mint_address,
            bonding_curve=bonding_curve_pda,
            fee_recipient=fee_recipient,
            spendable_sol_in=sol_amount,
            min_tokens_out=min_tokens_out,
            creator_vault=creator_vault,
            token_program_id=token_program_id
        )
        instructions.append(buy_ix)

        tx = await self._compile_transaction(instructions)
        return await self._simulate_or_send(tx, sim=sim, use_mev=not sim and self.mev_config is not None)

    async def pump_sell(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            token_amount: int,
            lamports_min_output: int,
            creator: str,
            sim: bool = False,
            priority_micro_lamports: int = 0
        ):

        instructions = []

        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)

        try:
            curve_state, creator_vault = await self.get_curve_context(bonding_curve_pda)
        except ValueError as exc:
            logging.warning(f"Skipping sell for {mint_address}: {exc}")
            return "creator_vault_unavailable"
        if curve_state is not None and getattr(curve_state, "complete", False):
            return "migrated"

        token_program_id = await self.get_token_program_id(mint_address)
        user_token_account = await self.resolve_user_token_account(
            self.priv_key.pubkey(),
            mint_address,
            token_program_id,
        )
        if user_token_account is None:
            return "missing_ata"
        
        if priority_micro_lamports > 0:
            instructions.append(set_compute_unit_limit(DEFAULT_PRIORITY_FEE_CLAMP_COMPUTE_UNITS))
            instructions.append(set_compute_unit_price(priority_micro_lamports))
        if not sim and self.mev_config is not None:
            mev_tip_ix = build_mev_tip_instruction(self.priv_key.pubkey(), self.mev_config)
            if mev_tip_ix is not None:
                instructions.append(mev_tip_ix)

        fee_recipient = self.get_fee_recipient(curve_state)
        sell_ix = await self.build_sell_instruction(
            mint=mint_address,
            bonding_curve=bonding_curve_pda,
            fee_recipient=fee_recipient,
            token_amount=token_amount,
            lamports_min_output=lamports_min_output,
            vault=creator_vault,
            token_program_id=token_program_id,
            user_token_account=user_token_account,
            cashback=bool(getattr(curve_state, "is_cashback_coin", False))
        )
        instructions.append(sell_ix)

        tx = await self._compile_transaction(instructions)
        return await self._simulate_or_send(tx, sim=sim, use_mev=not sim and self.mev_config is not None)

    async def getTransaction(self, tx_id: str, session: ClientSession):
        start_time = time.time()
        attempt = 1
        try:
            while attempt < 25:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [
                        tx_id,
                        {
                            "commitment": "confirmed",
                            "encoding": "json",
                            "maxSupportedTransactionVersion": 0
                        }
                    ]
                }
                headers = {
                    "Content-Type": "application/json"
                }

                async with session.post(self.rpc_endpoint, json=payload, headers=headers, timeout=10) as response:
                    if response.status != 200:
                        logging.error(f"HTTP Error {response.status}: {await response.text()}")
                        raise Exception(f"HTTP Error {response.status}")

                    data = await response.json()
                    logging.info(f"Attempt {attempt}")

                    if data and data.get('result') is not None:
                        logging.info(f"Elapsed: {time.time() - start_time:.2f}s")
                        result = data['result']
                        return result

                await asyncio.sleep(0.5)
                attempt += 1
        except Exception as e:
            logging.error(f"Error: {e}")
            return None

    async def close(self):
        await self.async_client.close()
        await self.session.close()
