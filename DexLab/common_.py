
import os
import dotenv
from pathlib import Path

dotenv.load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)

API_KEY = os.getenv("HL_API_KEY")
PRIV_KEY = os.getenv("PRIVATE_KEY")

PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SOL_ADDRESS = "So11111111111111111111111111111111111111112"
STAKED_API = f"https://staked.helius-rpc.com?api-key={API_KEY}" # e.g. helius
WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={API_KEY}" # e.g. helius
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={API_KEY}" # e.g. helius
