import asyncio, base58
from construct import Struct, Int64ul, Flag, Bytes
from typing import Final
import struct
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey # type: ignore
from solana.rpc.commitment import Processed

DISCRIMINATOR: Final[bytes] = struct.pack("<Q", 6966180631402821399)
class BondingCurveState:
    _STRUCT = Struct(
        "virtual_token_reserves" / Int64ul,
        "virtual_sol_reserves" / Int64ul,
        "real_token_reserves" / Int64ul,
        "real_sol_reserves" / Int64ul,
        "token_total_supply" / Int64ul,
        "complete" / Flag,
        "creator" / Bytes(32),
    )

    def __init__(self, data: bytes) -> None:
        parsed = self._STRUCT.parse(data[8:])
        self.__dict__.update(parsed)

def get_associated_bonding_curve_address(mint: Pubkey, program_id: Pubkey = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")) -> tuple[Pubkey, int]:
    # by derive
    return Pubkey.find_program_address(
        [
            b"bonding-curve",
            bytes(mint)
        ],
        program_id
    )

async def get_bonding_curve_state(conn: AsyncClient, curve_address: Pubkey) -> BondingCurveState:
    try:
        response = await conn.get_account_info(curve_address, commitment=Processed)
        if not response.value or not response.value.data:
            raise ValueError("Invalid curve state: No data")

        data = response.value.data
        if data[:8] != DISCRIMINATOR:
            raise ValueError("Invalid curve state discriminator")
        return BondingCurveState(data)
    except Exception as e:  
        return None
    
async def get_creator(conn: AsyncClient, bonding_curve: str):
    bc_state = await get_bonding_curve_state(
        conn,
        Pubkey.from_string(bonding_curve)
    )
    if bc_state is None:
        return False
    creator = bc_state.creator
    return base58.b58encode(creator).decode("utf-8")

async def check_has_migrated(ac: AsyncClient, bc: Pubkey) -> bool:
    bc_state = await get_bonding_curve_state(
        ac,
        bc
    )
    if bc_state is None:
        return False
    return bc_state.complete