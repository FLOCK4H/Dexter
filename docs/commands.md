# Command reference

Run commands as `python Dexter.py COMMAND` or use `dexter` inside the activated
Dexter virtual environment. Run `python Dexter.py help COMMAND` for current flags.


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
