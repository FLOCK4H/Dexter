"""Compatibility imports for the native Dexter pool reader."""
from types import SimpleNamespace as PumpSwapPoolState
from dexter_execution import NativeMarket as PumpSwapMarket

__all__ = ["PumpSwapMarket", "PumpSwapPoolState"]
