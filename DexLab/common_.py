
import os
import dotenv
from pathlib import Path

dotenv.load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)

HTTP_URL = os.getenv("HTTP_URL")
WS_URL = os.getenv("WS_URL")
PRIV_KEY = os.getenv("PRIVATE_KEY")

PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SOL_ADDRESS = "So11111111111111111111111111111111111111112"
STAKED_API = HTTP_URL # e.g. helius
WS_URL = WS_URL # e.g. helius
RPC_URL = HTTP_URL # e.g. helius
