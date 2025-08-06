<div align="center">
  <img src="https://github.com/user-attachments/assets/870e374f-5058-43d1-bbf7-cf22c28daaae" width="256"/>

  <i>​🇧​​🇱​​🇴​​🇴​​🇩​​🇾​ ​🇫​​🇦​​🇸​​🇹​.</i>
  
</div>

# Dexter 2.1

**Fast and advanced Solana Pump.fun Sniper Bot powered by algorithms** that analyzes previously recorded token data, calculates Pump.fun token creators score basing on multiple factors such as their average profits or total swaps, and buys their new tokens in under a second.

> [!IMPORTANT]
> This project is `FREE` and open-source, you are in full control of the software.
> There is no reliance on external APIs, you can choose to use Solana's public endpoint if that's what you want.
> Please read the documentation (this file) and source code fully, there are no catches, you can choose to support us by sending SOL to our wallet, or simply not.

# Features

1. Manual transaction building -> **No API, just ultra-fast and optimized transactions (swaps).**
2. Database that keeps track of new, and stagnant or migrated tokens.
3. Algorithms for generating a **leaderboard for Pump.fun creators**, and **calculating token's composite score basing on price trend and transaction momentum**.
4. Trading algorithm that incrementally handles current profit step: Buy / Sell.
5. Fully async and concurrent operation.
6. Settings system

![image](https://github.com/user-attachments/assets/afa042e2-89ee-496e-816e-dd3e0ac61f7f)

**If this comes useful feel free to airdrop some Solana:**

`FL4CKfetEWBMXFs15ZAz4peyGbCuzVaoszRKcoVt3WfC`

**Thanks 💙**

**Telegram: [FLOCK4H.CAVE](https://t.me/flock4hcave)**

**Discord: [FLOCK4H.CAVE](https://discord.gg/thREUECv2a)**

# Overview

What was used to make it all work?
- Helius standard websockets -> [helius.dev](https://www.helius.dev)
- Asynchronous programming
- Solders, and Solana libraries
- PostgreSQL v17
- An Azure/GCP server (optional)

# Setup

> [!NOTE]
> Dexter was designed to work both on Linux and Windows. Minimum requirements are: 100GB Free Disk Space (for Database that will grow daily), and 8-32GB of RAM.

We've tested Dexter on a server with these specifications: 

- 16GB RAM
- 4 VCPUs
- 150GB SSD
- Ubuntu Linux distro

> [!TIP]
> **Where to get a server?** </br>
> [Google Cloud Platform](https://cloud.google.com/) </br>
> [Microsoft Azure](https://azure.microsoft.com/en-us) </br>
> [Amazon Web Services](https://aws.amazon.com)

<h4>Libraries</h4>

List of libraries is in `req.txt` file, easily install it with:

```
$ pip install -r req.txt
```

<h4>Required Stack</h4>

1. **[PostgreSQL](https://www.postgresql.org/download/)**
2. **[Python](https://www.python.org/downloads/) >= 3.8**
3. **(Optional, only for windows), [Windows Terminal](https://apps.microsoft.com/detail/9n0dx20hk701?hl=en-US&gl=US)**

<h4>Step 1</h4>

**First, we need to initialize `PostgreSQL` database**, there is a script named `database.py` that might automate most of the process:

```
  # Make sure you run your terminal with administrator privileges
  # or use sudo if on Linux
  # Run this inside a terminal to create user, database, tables and indexes

  $ python database.py
```

The success is not guaranteed due to the nature of databases, but if this message appeared:

`PostgreSQL database, tables, and indexes initialized successfully.`

You should've successfuly created your database.
If you prefer to use different credentials than default, or don't/can't use `database.py` script, here is how it should be structured:

<details>
<summary>Click to expand</summary>

  ```
  mints (
          mint_id TEXT PRIMARY KEY,
          name TEXT,
          symbol TEXT,
          owner TEXT,
          market_cap DOUBLE PRECISION,
          price_history TEXT,
          price_usd DOUBLE PRECISION,
          liquidity DOUBLE PRECISION,
          open_price DOUBLE PRECISION,
          high_price DOUBLE PRECISION,
          low_price DOUBLE PRECISION,
          current_price DOUBLE PRECISION,
          age DOUBLE PRECISION DEFAULT 0,
          tx_counts TEXT,
          volume TEXT,
          holders TEXT,
          mint_sig TEXT,
          bonding_curve TEXT,
          created INT,
          timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
  
  stagnant_mints (
          mint_id TEXT PRIMARY KEY,
          name TEXT,
          symbol TEXT,
          owner TEXT,
          holders TEXT,
          price_history TEXT,
          tx_counts TEXT,
          volume TEXT,
          peak_price_change DOUBLE PRECISION,
          peak_market_cap DOUBLE PRECISION,
          final_market_cap DOUBLE PRECISION,
          final_ohlc TEXT,
          mint_sig TEXT,
          bonding_curve TEXT,
          slot_delay TEXT,
          timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
  ```

</details>

**When Pump.fun market is under heavy load, the database concurrent connections number rises, and after time will result in an error in `wsLogs.py` or in `Dexter.py` files. This is because
either:**

1. You need to increase max_connections in postgresql.conf
2. You don't meet RAM requirements

<h6>Please start by increasing your max_connections, in our case `24GB RAM=500 connections`</h6>

```
# postgresql.conf

{...}

#------------------------------------------------------------------------------
# CONNECTIONS AND AUTHENTICATION
#------------------------------------------------------------------------------

# - Connection Settings -

listen_addresses = '*'		# what IP address(es) to listen on;
					# comma-separated list of addresses;
					# defaults to 'localhost'; use '*' for all
					# (change requires restart)
port = 5432				# (change requires restart)
max_connections = 500			# (change requires restart) ✔
#reserved_connections = 0		# (change requires restart)
#superuser_reserved_connections = 3	# (change requires restart)
#unix_socket_directories = ''		# comma-separated list of directories
					# (change requires restart)
#unix_socket_group = ''			# (change requires restart)
#unix_socket_permissions = 0777		# begin with 0 to use octal notation
					# (change requires restart)
#bonjour = off				# advertise server via Bonjour
					# (change requires restart)
#bonjour_name = ''			# defaults to the computer name
					# (change requires restart)
```

<h4>Step 2</h4>

**For the `Dexter` to be able to gather data and trade tokens, we need to provide it with the APIs in `.env` in the root project directory, or `common_.py` file located in `DexLab` folder.**

**.env file structure**

```
HTTP_URL=
WS_URL=
PRIVATE_KEY=YOUR_SOLANA_API_KEY # example: 4bAWEz915ggjuZrZLmkspVsFKyZjWLllObBrUhRwnxsfgFrhf...
```

Example using `Helius shared RPC` (all shared endpoint providers should work) 

```
# common_.py

STAKED_API = f"https://staked.helius-rpc.com?api-key={API_KEY}" # e.g. helius
WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={API_KEY}" # e.g. helius
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={API_KEY}" # e.g. helius
```

If you are using `solana-cli` to create your wallet you may want to convert your keypair into Private Key and Public Key, here is how to do that:

<details>
	
<summary>Click to expand</summary>

```
import json
import base58  # Install this library with: pip install base58
from pathlib import Path
from solders.keypair import Keypair  # Install this library with: pip install solders

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
```

</details>

> [!IMPORTANT]
> The script does not connect to any external API, meaning - your keys are safe. </br>
> That doesn't mean you should use your main wallet for the bot, it's unwise to do so.

Now, **staying in the root directory of the project**, we launch `wsLogs.py` file to start collecting mints.

```
  $ python DexLab/wsLogs.py
```

![image](https://github.com/user-attachments/assets/5f0f50f2-50d5-40b2-b54e-bd24e5fbfd16)

**There is a backup system for the database, if (on windows) you've installed PostgreSQL elsewhere than `C:\Program Files\PostgreSQL\17\bin\pg_dump.exe` you will need to modify `market.py` in `DexLab` folder.**

```
# Change this to your pg_dump path
PG_DUMP_PATH = r"C:\Program Files\PostgreSQL\17\bin\pg_dump.exe" 
```

<h4>Step 3</h4>

Last step is to change settings of `Dexter` to your liking, wait until there are some mints in the database, and start sniping.

Settings and their descriptions can be found in `settings.py` file in the root directory of the project:

```
# Description: This file contains all the settings for the Dexter.
from decimal import Decimal
# =================
# TRUST FACTOR PART
# =================

# !IMPORTANT: Tweak these values to adjust the trust-factor algorithm
# Minimum total swaps on all tokens for creators with more than 2 mints
# Scale: 500 HIGHRISK, 1000 MEDIUMRISK, 2500 LOWRISK
TOTAL_SWAPS_ABOVE_2_MINTS = 500 

# Minimum total swaps on all tokens for creators with 1 mint
# Scale: 500 HIGHRISK, 1000 MEDIUMRISK, 2000 LOWRISK
TOTAL_SWAPS_1_MINT = 250 

# Median peak market cap for creators with more than 2 mints
# Scale: 5000 HIGHRISK, 10000 MEDIUMRISK, 35000 LOWRISK
MEDIAN_PEAK_MC_ABOVE_2_MINTS = 25000 

# Median peak market cap for creators with 1 mint
# Scale: 20000 HIGHRISK, 50000 MEDIUMRISK, 70000 LOWRISK
MEDIAN_PEAK_MC_1_MINT = 30000

# Minimum swaps before the highest price is reached
# Scale: 50 HIGHRISK, 100 MEDIUMRISK, 250 LOWRISK
HIGHEST_PRICE_MIN_SWAPS = 50 

# Snipe price to peak price ratio
# Scale: 2.5 HIGHRISK, 3.0 MEDIUMRISK, 3.5 LOWRISK
SNIPE_PRICE_TO_PEAK_PRICE_RATIO = 2.5

# Minimum trust factor to consider a creator successful
# Scale: 0.5 HIGHRISK, 0.6 MEDIUMRISK, 0.8 LOWRISK
TRUST_FACTOR_RATIO = 0.65

# Price closest to X second after the first transaction.
# Example: 1.5 means the price closest to 1.5 seconds after the first transaction.
SNIPING_PRICE_TIME = 2.5

# Refer to set_trust_level method in Dexter.py to understand how trust levels are determined
# Dexter automatically handles USD to SOL conversion, no need to worry about it
AMOUNT_BUY_TL_2 = 15 # 4 USD for creators with Trust Level 2
AMOUNT_BUY_TL_1 = 10 # 2 USD for creators with Trust Level 1
BUY_FEE = 0.15 # 0.1 USD
SELL_FEE = 0.15 # 0.1 USD
SLIPPAGE = 1.90 # 90%, 30% is safer
PRICE_STEP_UNITS = 40 # 40% price step

# If you want to just swap on pump.fun until the bonding curve is reached:
PROFIT_MARGIN = Decimal('0.8') # 80% - This will clamp the maximum profit range of the creator to 40% of his median profit range
# Example: If the creator's median profit range is 720%, the maximum profit range for sell will be 576%
# If you want to sell on raydium you must do it manually through your wallet or other services, Dexter is a Pump.fun bot.
# For selling on Raydium set this to 1.0

# Algorithm parameters
# !WARNING DO NOT CHANGE IF YOU DO NOT KNOW WHAT YOU ARE DOING
# Price trend weight and transaction momentum weight affect the composite score of a token
# E.g. if price trend weight is 0.4 and transaction momentum weight is 0.6, the composite score is:
# 0.4 * price trend score + 0.6 * transaction momentum score
# !default values are 0.4 and 0.6
PRICE_TREND_WEIGHT = Decimal('0.4')
TX_MOMENTUM_WEIGHT = Decimal('0.6')

# Session algorithm parameters
# If composite score is above INCREMENT_THRESHOLD, bot will increase current step at half of a jump
# Example: If current step is 40% and composite score is above 25, bot will increase step to 80%
# !default values are 25 and 20
INCREMENT_THRESHOLD = Decimal('25')

# Example: If current step is 80% but there are more sells than buys, bot will sell
# !default value is 20
# This is a heuristic, rarely being triggered
DECREMENT_THRESHOLD = Decimal('20')
```

After all parameters are checked and set, `Dexter` can be launched by:

```
  $ python Dexter.py
```

Dexter will automatically buy the token when owner of the captured mint is in the leaderboard created from database entries.
Profit range is median success rate of the owner, currently adjusted to be 40% of it, if you prefer to aim for selling tokens right before/after they migrate you need to set `profit margin to 1.0` in `settings.py`.
Have in mind that migrated tokens are out of Pump.fun market, which means swaps are handled by Raydium, read `settings.py` for more information.
After price change is bigger than current step, we sell the token.

Transactions on 3MB/s home network take around 1-2 seconds, where on industrial grade network like Azure's, it's as fast as 0.2-1s for the transaction to confirm.

# Usage

`Dexter.py` and `wsLogs.py` are independent processes.

Launch `Dexter.py` to analyze current database data, convert that into a leaderboard list, and snipe new tokens.

Launch `wsLogs.py` to collect entries for sniping.

```
  $ python DexLab/wsLogs.py
  $ python Dexter.py
```

# FAQ

**1. Did you have any success with the Dexter?**
 - Yes, with around 300k creators in the database we were able to profit x4 2 times, x2 2 times and 1.5x 7 times sniping extremely safely only 3/4 entries a day investing 0.1 SOL in every token.

**2. What settings are the best?**
 - Default settings in `settings.py` file may suffice for now. They depend on the market volatility, health, and current events, adjust settings basing on your observations.

**3. Can the Dexter drain my wallet?**
 - Everything can drain your wallet if not used correctly, there's a lot of guardrails to prevent you from doing that, but as devs we can go as far. When Dexter buys or sells a token it logs transaction's signature, if the transaction isn't to be found on `Solscan` please increase the priority fee. This is fee paid to Solana so transaction gets confirmed quicker/ at all.

**4. Help, I'm stuck at...**
 - Support: [FLOCK4H.CAVE](https://t.me/flock4hcave)

# License 

Copyright (c) 2025 FLOCK4H

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
