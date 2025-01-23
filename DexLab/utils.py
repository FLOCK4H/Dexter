# utils.py
from decimal import Decimal

async def usd_to_lamports(usd_amount: float, sol_price_usd: Decimal) -> int:
    """
    Convert USD to lamports based on the current SOL price in USD.

    Args:
        usd_amount (float): The amount in USD to convert.
        sol_price_usd (Decimal): The price of 1 SOL in USD.

    Returns:
        int: The equivalent amount in lamports.
    """
    sol_per_usd = Decimal(usd_amount) / sol_price_usd
    lamports = int(sol_per_usd * Decimal(1000000000))  # Convert SOL to lamports
    return lamports

async def lamports_to_tokens(lamports: int, price: Decimal) -> Decimal:
    """
    Convert lamports to tokens based on the current price.

    Args:
        lamports (int): The amount in lamports to convert.
        price (Decimal): The price of 1 token in SOL lamports.

    Returns:
        Decimal: The equivalent amount in tokens.
    """
    lams_to_human = Decimal(lamports) / Decimal(1e9)
    tokens = lams_to_human / Decimal(price)
    token_amount = tokens * Decimal(1e6)
    return int(token_amount)

def usd_to_microlamports(usd_fee: float, sol_price_usd: Decimal, compute_units: int) -> int:
    """
    Returns:
        int: micro-lamports per compute unit (SetComputeUnitPrice).
    """
    sol_fee = Decimal(usd_fee) / sol_price_usd
    lamports_total = sol_fee * Decimal("1e9")
    micro_total = lamports_total / Decimal("1e4")
    base_per_unit = micro_total * compute_units
    return int(base_per_unit)