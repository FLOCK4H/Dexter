# Dexter

**Fast and advanced Solana Pump.fun Sniper Bot** that analyzes previously recorded token data, calculates Pump.fun creators score basing on multiple factors, and buys popular creators new tokens in under a second.  

# Features

1. Manual transaction building -> **No API, just ultra-fast and optimized transactions.**
2. Database that keeps track of new, and stagnant or migrated tokens.
3. Algorithm for generating a **leaderboard for Pump.fun creators**.
4. Trading algorithm that incrementally handles current profit step: Buy / Sell.
5. Fully async and concurrent operation.

# Overview

What was used to make it all work?
- Helius websockets -> [helius.dev](https://www.helius.dev)
- Asynchronous programming
- Solders, and Solana libraries

# Setup

<h4>Libraries</h4>

```
solana==0.35.1
solders==0.21.0
aiohttp, websockets, base58, borsh_construct, requests, asyncpg

$ pip install solana==0.35.1 solders==0.21.0 aiohttp websockets base58 borsh_construct requests asyncpg
```

<h4>Step 1</h4>

First, we need to initialize `PostgreSQL` database, there is a script named `database.py` that might automate most of the process:

```
  # Run this inside a shell to create user, database, tables and indexes
  $ sudo python database.py
```

This is not guaranteed, but if this message appeared:

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

The `market.py` and `Dexter.py` files both access the database.

<h4>Step 2</h4>

For the `Dexter` to be able to gather data and trade tokens, we need to provide it with the APIs in `common_.py` file located in `DexLab` folder.

In our case, we used `Helius shared RPC`

```
# common_.py

gWS_URL = f"" # e.g. helius
WS_URL = f"" # e.g. helius
RPC_URL = f"" # e.g. helius
STAKED_API = "" # e.g. helius
```

Then we simply modify `.config` file to include our `API_KEY`, `PRIV_KEY`, and `WALLET`.

`Note: There are two .config files, so modify both the same.`

```
HL_API_KEY=YOUR_API_KEY
WALLET_ADDRESS=YOUR_WALLET_ADDRESS
PRIVATE_KEY=YOUR_PRIVATE_KEY
```

> [!IMPORTANT]
> The script does not connect to any external API, meaning - your keys are safe

Now, we can just head to `DexLab` folder and launch `wsLogs.py` file to start collecting mints.

```
  $ python wsLogs.py
```

<h4>Step 3</h4>

Last step is to tweak parameters of Dexter to your liking, wait until there are some mints in the database, and start sniping.

Relevant places in code:

```
-> Dexter.py

# Here change buy price
amount = 2 if trust_level == 1 else 4 # 2USD or 4USD

# 0.1USD fee, 50k compute units
fee = usd_to_microlamports(0.1, self.analyzer.sol_price_usd, 50_000) if trust_level == 1 else usd_to_microlamports(0.1, self.analyzer.sol_price_usd, 50_000)

# Algorithm parameters
PRICE_TREND_WEIGHT = Decimal('0.4')
TX_MOMENTUM_WEIGHT = Decimal('0.6')

# Session algorithm parameters
INCREMENT_THRESHOLD = Decimal('25')
DECREMENT_THRESHOLD = Decimal('20')

-> DexAI/trust_factor.py

# Here is the main condition to set, this makes the algorithm choose best creators, tweak it to your liking
if (mint_count >= 2 and median_peak_mc >= 7500 and total_swaps >= 5) or (mint_count >= 1 and median_peak_mc >= 7000 and total_swaps >= 5): #1

# 1: Check if highest price is at least 150% of the sniping price
peak_price = Decimal(str(token_info["high_price"]))
if peak_price < sniping_price * Decimal("2.5"):

# 2: Ensure the highest price was reached after at least 100 swaps
if peak_index < 100:

-> DexLab/pump_swap.py
# slippage, 1.52x
int(sol_amount * 1.52)
```

After all parameters are checked and set, `Dexter` can be launched by:

```
  $ python Dexter.py
```

![dexterr](https://github.com/user-attachments/assets/18b25e1f-07c2-4278-b025-0c03f2f6cafd)

Dexter will automatically buy the token when owner of the captured mint is in the leaderboard created from database entries.
Profit range is median success rate of the owner, currently adjusted to be 40% of it.
After price change is bigger than current step, we sell the token.

Transactions on 3MB/s home network take around 1-2 seconds, where on industrial grade network like Azure's, it's as fast as 0.2-1s for the transaction to confirm.

# Usage

`Dexter.py` and `wsLogs.py` are independent processes.

Launch `Dexter.py` to analyze current database data, convert that into a leaderboard list, and snipe new tokens.

Launch `wsLogs.py` to collect entries for later sniping.

```
  $ python wsLogs.py
  $ python Dexter.py
```

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
