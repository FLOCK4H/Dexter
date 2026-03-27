# Dexter 3.0



https://github.com/user-attachments/assets/6a09fae0-ad94-4ab0-a385-b870280d963e



Dexter 3.0 is a TUI Solana app for Pump.fun and PumpSwap. **Main function is to gather Pump.fun creators and see how their tokens perform by running calculations with an algorithm, then composing a leaderboard out of creators. It monitors Pump.fun for new tokens, buys if creator of the token is in leaderboard.** The main surface is the interactive `dexter` menu: it edits the whole `.env`, onboards missing settings, launches runtime flows, handles create and manage workflows, and exposes operator controls without leaving the terminal. The CLI mirrors the same surface for automation and precise manual runs.

Windows note: Dexter's TUI uses `curses`. `pip install .` now installs `windows-curses` automatically on Windows, and if the TUI dependency is unavailable Dexter falls back to CLI help instead of crashing during import.

<div align="center">
  
<img width="512" height="512" alt="image" src="https://github.com/user-attachments/assets/d00e5aaa-fea4-40cb-bb33-10e3836a1fd5" />

</div>

## Start With The TUI

<img width="512" height="512" alt="image" src="https://github.com/user-attachments/assets/1e12b66e-772f-4159-b887-2f908eaabb7f" />

Run:

```bash
dexter
```

In an interactive terminal this opens Dexter's curses UI. It:

- writes directly to `.env`
- reloads config immediately after saves
- guides missing setup through onboarding
- asks for extra confirmation before `DEXTER_MAINNET_DRY_RUN=false` or `DEXTER_ALLOW_MAINNET_LIVE=true`

Main menus:

- `Run`: launch trader, collector, or analyzer
- `Create`: seed an existing mint or plan/build a Pump.fun create

<img width="600" height="918" alt="image" src="https://github.com/user-attachments/assets/375b1c41-89d5-4bcd-9426-e1acccdde338" />

- `Manage`: inspect or liquidate tracked positions

<img width="600" height="918" alt="image" src="https://github.com/user-attachments/assets/9d4080da-ebad-486f-92dd-bd9b4c760061" />
 
- `Configure`: edit the full `.env`

<img width="600" height="918" alt="image" src="https://github.com/user-attachments/assets/11fdfafe-21ff-45b7-a94b-775752614b2b" />

- `Help`: review modes, controls, and key runtime concepts

<img width="600" height="918" alt="image" src="https://github.com/user-attachments/assets/faadaf32-e061-49dd-87fc-09e25f9ec9ca" />


The configuration pages are:

- `Quick Setup`: network, mode, wallet, database, mainnet RPC
- `Runtime & Safety`: wsLogs supervision, datastore, shutdown behavior, mainnet gates
- `Risk & Strategy`: strategy profile, spend caps, reserve floor, retry behavior
- `Alerts & Paths`: Telegram, Discord, desktop notifications, logs, state, exports, backups

If you run `dexter` in a non-interactive shell, Dexter prints CLI help instead of launching the TUI. `dexter menu` and `dexter interactive` also open the TUI. If the TUI dependency is missing, Dexter prints an install hint and the CLI remains available through commands like `dexter help` and `dexter doctor`.

## Install And Required Setup

Dexter expects PostgreSQL-backed operation. Treat the database as required infrastructure.

```bash
python3 -m venv env
source env/bin/activate
pip install -r req.txt
pip install -e .
cp .env.example .env
```

Windows PowerShell:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r req.txt
python -m pip install -e .
Copy-Item .env.example .env
```

Windows PostgreSQL one-command setup:

```powershell
dexter database-setup
```

On Windows, `dexter database-setup` is the public-ready path: it installs PostgreSQL binaries when needed, creates or reuses a Dexter-owned local cluster under `.dexter/postgres/...`, stores Dexter's real local admin and app credentials in managed metadata, rewrites `.env`, and creates the Dexter role, database, tables, and schema for you. The managed cluster listens on `127.0.0.1:55432` by default so it does not collide with a separate system PostgreSQL on `5432`.

If you prefer a script from the repo root instead of the CLI command:

```powershell
powershell -ExecutionPolicy Bypass -File .\install_postgres_windows.ps1
```

Minimum setup before running Dexter seriously:

- database: on Windows, `dexter database-setup` writes `DATABASE_URL`, `DB_*`, `POSTGRES_ADMIN_*`, Dexter's managed local PostgreSQL paths, and the local `pg_dump.exe` path for you
- bootstrap admin: if you skip `database-setup`, set `POSTGRES_ADMIN_DSN` or `POSTGRES_ADMIN_*` when Dexter needs to create the DB, user, or schema
- wallet: set `PRIVATE_KEY` or `DEXTER_TRADING_PRIVATE_KEY` for `simulate`, `live`, or on-chain `create`
- mainnet RPC: set `HTTP_URL` and `WS_URL`
- safety gates: leave `DEXTER_MAINNET_DRY_RUN=true` and `DEXTER_ALLOW_MAINNET_LIVE=false` until you intentionally want live mainnet sends

Bootstrap the database when needed:

```bash
dexter database-setup
```

If PostgreSQL is already installed and running, or if you're on Linux/macOS and only need Dexter's schema:

```bash
dexter database-init
```

On Windows, `dexter database-init` also repairs a Dexter-managed `.env` by reloading the real local admin and app passwords from Dexter's managed metadata before it refreshes roles and tables.

Useful first checks:

```bash
dexter help
dexter doctor --network mainnet --mode read_only
dexter run --network mainnet --mode paper --doctor-first
```

## Mainnet Safety Model

- `read_only`: observe only
- `paper`: run the full strategy loop with simulated positions and local PnL
- `simulate`: sign and simulate transactions
- `live`: submit live transactions where the selected network and gates allow it

Mainnet rules:

- `DEXTER_MAINNET_DRY_RUN=true` keeps mainnet `live` in simulation behavior
- real mainnet sends require both `DEXTER_MAINNET_DRY_RUN=false` and `DEXTER_ALLOW_MAINNET_LIVE=true`
- `USE_MEV` only applies to live mainnet buy and sell submissions
- mainnet `create` sends are blocked in Dexter's current create flow; use `--dry-run` or `--simulate-tx`

## Few Mainnet-Oriented Examples

```bash
# Open Dexter's main feature
dexter

# Mainnet paper runtime with preflight checks
dexter run --network mainnet --mode paper --doctor-first

# Mainnet create planning
dexter create \
  --network mainnet \
  --mode live \
  --dry-run \
  --name DexterTest \
  --symbol DXT \
  --uri https://example.invalid/token.json \
  --buy-sol 0.01

# Watch the operator dashboard against mainnet-configured state
dexter dashboard --network mainnet --watch
```

## Command Reference

### `dexter help`

Show global help or the help for one command.

Args:

- `<command>`: optional command name, for example `dexter help create`

### `dexter run` / `dexter start`

Guided launcher for the runtime.

Args:

- `--mode {read_only,paper,simulate,live}`: override `DEXTER_RUNTIME_MODE`
- `--network {devnet,mainnet}`: override `DEXTER_NETWORK`
- `--target {trade,collector,analyze}`: choose what Dexter launches
- `--doctor-first`: run `doctor` before launch

### `dexter collector`

Start the collector directly.

Args:

- `--mode {read_only,paper,simulate,live}`
- `--network {devnet,mainnet}`

### `dexter trade`

Start the trader directly.

Args:

- `--mode {read_only,paper,simulate,live}`
- `--network {devnet,mainnet}`

### `dexter analyze`

Start the analyzer directly.

Args:

- `--mode {read_only,paper,simulate,live}`
- `--network {devnet,mainnet}`

### `dexter doctor`

Validate the current setup.

Args:

- `--mode {read_only,paper,simulate,live}`: load config as that mode
- `--network {devnet,mainnet}`: load config as that network
- `--component {all,collector,trader}`: choose the validation profile

Checks:

- env and safety gates
- database connectivity
- HTTP RPC reachability
- WebSocket RPC reachability
- wallet decoding
- writable directories
- backup tooling discovery

### `dexter create`

Seed an existing mint into Dexter or plan/build/send an on-chain Pump.fun create flow.

Common args:

- `--mode {read_only,paper,simulate,live}`
- `--network {devnet,mainnet}`
- `--dry-run`: preview or build only without the normal follow-through

Seeded handoff args:

- `--mint`: mint Dexter should take over
- `--owner`: creator or owner address for the session
- `--bonding-curve`: Pump.fun bonding curve when the token is still on Pump.fun
- `--market {pump_fun,pump_swap}`: starting market source
- `--trust-level`: initial trust level
- `--buy-price`: seed current or entry price
- `--token-balance`: seed held token balance
- `--cost-basis-lamports`: seed explicit cost basis
- `--profit-target-pct`: seed target percentage
- `--load-database`: opt back into standard DB bootstrap instead of DB-free handoff

On-chain create args:

- `--name`: token name
- `--symbol`: token symbol
- `--uri`: metadata URI
- `--image`: local image to upload to Pump.fun IPFS
- `--description`: description used with `--image`
- `--twitter`: twitter URL used with `--image`
- `--telegram`: telegram URL used with `--image`
- `--website`: website URL used with `--image`
- `--hide-name`: request `showName=false` during metadata upload
- `--ipfs-upload-url`: override the Pump.fun IPFS upload endpoint
- `--buy-sol`: bundled auto-buy size in SOL
- `--slippage-pct`: bundled auto-buy slippage percent
- `--priority-micro-lamports`: explicit compute-unit price
- `--simulate-tx`: force signed simulation instead of send
- `--no-follow`: do not launch Dexter after a live create send

Rules:

- seeded mode requires `--mint` and `--owner`
- on-chain mode requires `--name`, `--symbol`, `--uri` or `--image`, and `--buy-sol > 0`
- mainnet create sends are blocked; use `--dry-run` or `--simulate-tx`

### `dexter manage`

Inspect or liquidate positions from Dexter's local recovery store.

Args:

- `--mode {read_only,paper,simulate,live}`
- `--network {devnet,mainnet}`
- `--sell-mint <mint>`: sell one tracked mint
- `--sell-all`: sell every open tracked position for the selected network
- `--reason <text>`: reason attached to manual exits
- `--json`: emit the recovery-store view as JSON

Note:

- `manage` works from Dexter's local tracked-position store, not from a fresh wallet or chain inventory scan

### `dexter dashboard`

Render the operator dashboard.

Args:

- `--mode {read_only,paper,simulate,live}`
- `--network {devnet,mainnet}`
- `--watch`: refresh continuously
- `--interval <seconds>`: refresh interval for `--watch`
- `--limit <rows>`: rows per dashboard section
- `--json`: emit JSON instead of text

### `dexter control`

Manual operator controls.

Args:

- `action`: one of `pause`, `resume`, `force-sell`, `blacklist`, `whitelist`, `watchlist-add`, `watchlist-remove`
- `--mode {read_only,paper,simulate,live}`
- `--network {devnet,mainnet}`
- `--owner <creator>`: required for `blacklist` and `whitelist`
- `--mint <mint>`: required for `force-sell`, `watchlist-add`, and `watchlist-remove`
- `--reason <text>`: optional reason for `force-sell`

### `dexter replay`

Replay normalized session data.

Args:

- `--mode {read_only,paper,simulate,live}`
- `--network {devnet,mainnet}`
- `--session-id <id>`: replay one session
- `--mint-id <mint>`: replay the latest session for a mint
- `--json`: emit JSON

### `dexter export`

Export normalized research data.

Args:

- `--mode {read_only,paper,simulate,live}`
- `--network {devnet,mainnet}`
- `--kind {sessions,raw_events,leaderboard,positions,risk_events,strategy_profiles}`: choose dataset
- `--output <path>`: output JSONL path
- `--session-id <id>`: filter session exports
- `--mint-id <mint>`: filter session or raw-event exports
- `--leaderboard-version <version>`: filter leaderboard exports
- `--limit <n>`: cap exported rows

### `dexter backtest`

Run a strategy backtest.

Args:

- `--mode {read_only,paper,simulate,live}`
- `--network {devnet,mainnet}`
- `--strategy {aggressive,balanced,conservative}`: choose profile
- `--input <file>`: optional JSON or JSONL dataset
- `--limit <n>`: max records loaded when `--input` is omitted
- `--json`: emit JSON

### `dexter verify-migration`

Run the migration harness.

Args:

- `--mode {read_only,paper,simulate,live}`: config loading only
- `--network {devnet,mainnet}`: devnet-only in practice
- `--json`: emit JSON

### `dexter database-init`

Create or repair Dexter's PostgreSQL roles, database, tables, and schema after PostgreSQL itself exists. On Windows, this also repairs a Dexter-managed `.env` from Dexter's local PostgreSQL metadata when needed.

Args:

- `--network {devnet,mainnet}`: config-resolution override; the work itself is local PostgreSQL bootstrap

### `dexter database-setup`

Install or repair Dexter's managed local PostgreSQL on Windows, update `.env`, and run Dexter's schema bootstrap automatically.

Args:

- `--network {devnet,mainnet}`: optional Dexter config override
- `--major-version <n>`: WinGet PostgreSQL major version to install when PostgreSQL is missing
- `--admin-password <password>`: Dexter local `postgres` superuser password; defaults to the managed value or a generated password
- `--db-user <name>`: Dexter application role name
- `--db-password <password>`: Dexter application role password
- `--db-name <name>`: Dexter application database name
- `--db-port <port>`: local PostgreSQL port; defaults to the managed value or `55432`
- `--cluster-dir <path>`: Dexter-managed PostgreSQL data directory
- `--log-file <path>`: Dexter-managed PostgreSQL log file
- `--skip-install`: skip WinGet and assume PostgreSQL is already installed locally
- `--dry-run`: print the install and setup actions without changing the machine

## The TUI Settings Pages

### `Quick Setup`

This is the fastest path to a usable Dexter session. It covers:

- `DEXTER_NETWORK`
- `DEXTER_RUNTIME_MODE`
- `PRIVATE_KEY`
- `DATABASE_URL`
- `HTTP_URL`
- `WS_URL`

### `Runtime & Safety`

This page controls the runtime switches that matter most operationally:

- `DEXTER_ENABLE_WSLOGS`
- `DEXTER_DATASTORE_ENABLED`
- `DEXTER_CLOSE_POSITIONS_ON_SHUTDOWN`
- `DEXTER_MAINNET_DRY_RUN`
- `DEXTER_ALLOW_MAINNET_LIVE`

### `Risk & Strategy`

This page is where you shape how aggressive Dexter is:

- `DEXTER_STRATEGY_PROFILE`
- `DEXTER_PER_TRADE_SOL_CAP`
- `DEXTER_SESSION_SOL_CAP`
- `DEXTER_DAILY_SOL_CAP`
- `DEXTER_WALLET_RESERVE_FLOOR_SOL`
- `DEXTER_DAILY_DRAWDOWN_STOP_SOL`
- retry and execution knobs
- legacy trust-factor and profit-step settings

### `Alerts & Paths`

This page covers operational outputs and persistence:

- Telegram, Discord, and desktop notifications
- log, state, and export directories
- backup directory and backup tooling settings

## Operator Notes

- `PRIVATE_KEY` is Dexter's default signer for create, buy, sell, and balance lookup
- `DEXTER_TRADING_PRIVATE_KEY` overrides the trading signer when you want a separate execution key
- `DATABASE_URL` is Dexter's preferred database setting inside the TUI
- `dexter help <command>` is still the fastest CLI lookup when you only need one command's flags
