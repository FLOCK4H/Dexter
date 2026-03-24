from .colors import cc, ColorCodes

__all__ = [
    "DexBetterLogs",
    "Interpreters",
    "Market",
    "cc",
    "ColorCodes",
    "usd_to_lamports",
    "lamports_to_tokens",
    "usd_to_microlamports",
]


def __getattr__(name):
    if name == "DexBetterLogs":
        from .wsLogs import DexBetterLogs

        return DexBetterLogs
    if name == "Interpreters":
        from .serializers import Interpreters

        return Interpreters
    if name == "Market":
        from .market import Market

        return Market
    if name in {"usd_to_lamports", "lamports_to_tokens", "usd_to_microlamports"}:
        from .utils import lamports_to_tokens, usd_to_lamports, usd_to_microlamports

        exports = {
            "usd_to_lamports": usd_to_lamports,
            "lamports_to_tokens": lamports_to_tokens,
            "usd_to_microlamports": usd_to_microlamports,
        }
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
