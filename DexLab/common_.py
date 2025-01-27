
try:
    from config_reader import get_api_key, get_private_key, get_wallet, read_config
except ImportError:
    from .config_reader import get_api_key, get_private_key, get_wallet, read_config
import os

config = read_config("config/.config")
if not config:
    CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config/.config")
    config = read_config(CONFIG_PATH)
    print(CONFIG_PATH)
API_KEY = get_api_key(config)
PRIV_KEY = get_private_key(config)
WALLET = get_wallet(config)
PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SOL_ADDRESS = "So11111111111111111111111111111111111111112"
STAKED_API = f"https://staked.helius-rpc.com?api-key={API_KEY}" # e.g. helius
WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={API_KEY}" # e.g. helius
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={API_KEY}" # e.g. helius
