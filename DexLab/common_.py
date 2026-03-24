from dexter_config import load_config, load_env_file

load_env_file()
_APP_CONFIG = load_config()

HTTP_URL = _APP_CONFIG.rpc.http_url
WS_URL = _APP_CONFIG.rpc.ws_url
PRIV_KEY = _APP_CONFIG.rpc.private_key

PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_SWAP = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SOL_ADDRESS = "So11111111111111111111111111111111111111112"
STAKED_API = HTTP_URL # e.g. helius
WS_URL = WS_URL # e.g. helius
RPC_URL = HTTP_URL # e.g. helius
