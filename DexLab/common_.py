
try:
    from .config_reader import get_api_key, get_private_key, get_wallet, read_config
except ImportError:
    from config_reader import get_api_key, get_private_key, get_wallet, read_config

config = read_config(".config")
API_KEY = get_api_key(config)
PRIV_KEY = get_private_key(config)
WALLET = get_wallet(config)
gWS_URL = f"" # e.g. helius
WS_URL = f"" # e.g. helius
PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
METAPLEX = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
RPC_URL = f"" # e.g. helius
SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SOL_ADDRESS = "So11111111111111111111111111111111111111112"
STAKED_API = "" # e.g. helius
SWAP_URL = "" # e.g. quicknode