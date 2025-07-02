import json
import base58  # Install this library with: pip install base58
from pathlib import Path
from solders.keypair import Keypair  # type: ignore Install this library with: pip install solders

# Define the path to the keypair.json file
keypair_json_path = Path("./keypair.json")

# Check if the file exists
if not keypair_json_path.exists():
    print("Error: keypair.json not found. Please ensure the file exists in the current directory.")
    exit(1)

# Read and parse the keypair.json file
with keypair_json_path.open("r", encoding="utf-8") as file:
    keypair_data = json.load(file)

# Convert the JSON array to bytes (Uint8Array equivalent)
secret_key_bytes = bytes(keypair_data)

# Create a Solana Keypair from the secret key
keypair = Keypair.from_bytes(secret_key_bytes)

# Encode the private key as a Base58 string
private_key_base58 = base58.b58encode(secret_key_bytes).decode("utf-8")

# Get the public key as a Base58 string
public_key_base58 = str(keypair.pubkey())

# Print the Base58 encoded keys
print("Base58 Private Key:", private_key_base58)
print("Public Key:", public_key_base58)