"""Compatibility import for native Pump trading and token launch.

Protocol builders now live in the native binding. Prefer dexter_execution directly.
"""
from solders.pubkey import Pubkey as PublicKey
from dexter_execution import NativeTokenLauncher as PumpFun, DEFAULT_PUMP_FUN_IPFS_UPLOAD_URL

__all__ = ["PumpFun", "PublicKey", "DEFAULT_PUMP_FUN_IPFS_UPLOAD_URL"]
