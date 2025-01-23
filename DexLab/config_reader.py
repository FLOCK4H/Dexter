import base64

def read_config(file_path=".config"):
    config_data = {}
    try:
        with open(file_path, "r") as config_file:
            for line in config_file:
                line = line.strip()
                if line.startswith("#") or not line:
                    continue
                
                key, value = line.split("=", 1)
                config_data[key.strip()] = value.strip()
    except Exception as e:
        raise RuntimeError(f"Failed to read config file: {e}")
    return config_data

def get_wallet(config_data):
    wallet = config_data.get("WALLET_ADDRESS")
    if not wallet:
        raise ValueError("WALLET_ADDRESS is missing in the configuration file.")
    return wallet

def get_private_key(config_data):
    private_key = config_data.get("PRIVATE_KEY")
    if not private_key:
        raise ValueError("PRIVATE_KEY is missing in the configuration file.")
    return private_key

def get_api_key(config_data):
    key = config_data.get("HL_API_KEY")
    if not key:
        raise ValueError("HL_API_KEY is missing in the configuration file.")
    
    try:
        _key = key
    except Exception as e:
        raise ValueError(f"Failed to decode HL_API_KEY: {e}")
    return _key