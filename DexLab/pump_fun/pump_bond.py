"""Compatibility curve helpers, delegated to the native Dexter market binding."""
from types import SimpleNamespace as BondingCurveState
from solders.pubkey import Pubkey
from dexter_execution import sdk_client


def get_associated_bonding_curve_address(mint, program_id=None):
    if program_id is not None:
        raise ValueError("Custom program IDs are unsupported; use Dexter's Pump market")
    from dexter_sdk import derive_pump_curve
    address,bump=derive_pump_curve(str(mint))
    return Pubkey.from_string(address),bump


async def get_bonding_curve_state(conn, curve_address):
    sdk=sdk_client(str(conn._provider.endpoint_uri))
    return BondingCurveState(**await sdk.request("curve",pool=str(curve_address)))


async def get_creator(conn, bonding_curve):
    return (await get_bonding_curve_state(conn,bonding_curve)).creator


async def check_has_migrated(conn, bonding_curve):
    return (await get_bonding_curve_state(conn,bonding_curve)).complete
