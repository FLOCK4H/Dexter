# Dexter 2.1

Dexter is a Pump.fun-focused Solana trading runtime descended from the original FLOCK4H Dexter 2.0 codebase, but the operator surface is now centered on a single `dexter` command, explicit network selection, safer mainnet gates, a DB-free create handoff, and a cleaner operator workflow.

## What Changed From Dexter 2.0

- The main operator entrypoint is now `dexter`, not a loose mix of separate scripts.
- `dexter help` and `dexter help <command>` work as expected.
- `create` is network-aware and split into two clear modes:
  - seeded handoff for an existing mint you already own or track
  - on-chain Pump.fun create with build, simulate, or live send behavior
- Every operator command except bare `dexter help` now makes the target network explicit with `--network`, including `create`, `manage`, `doctor`, `collector`, `trade`, `analyze`, `replay`, `backtest`, `verify-migration`, `export`, `dashboard`, `control`, and `database-init`.
- Mainnet is guarded by `DEXTER_MAINNET_DRY_RUN` and `DEXTER_ALLOW_MAINNET_LIVE`.
- Devnet is the intended place for live-runtime testing.
- Pump.fun migration handling and PumpSwap exits are now part of the operator flow.
- The research datastore adds replay, export, dashboard, and control tooling on top of the legacy runtime.
- Priority fee selection is dynamic, and mainnet live buy/sell sends can optionally route through MEV block engines.

## Safety Model

- `DEXTER_NETWORK=devnet` ignores `HTTP_URL` and `WS_URL` and uses Dexter's built-in devnet RPC endpoints.
- `DEXTER_NETWORK=mainnet` reads `HTTP_URL` and `WS_URL` from `.env`.
- `DEXTER_RUNTIME_MODE=paper` keeps trading local.
- `DEXTER_RUNTIME_MODE=simulate` signs and simulates, but does not submit.
- `DEXTER_RUNTIME_MODE=live` can submit on-chain only when the network and safety gates allow it.
- Mainnet live sends require both `DEXTER_MAINNET_DRY_RUN=false` and `DEXTER_ALLOW_MAINNET_LIVE=true`.
- `USE_MEV` is mainnet-only. Dexter ignores it on devnet to avoid sender errors there.

## Install

```bash
cp .env.example .env
python3 -m venv env
source env/bin/activate
pip install -r req.txt
pip install -e .
```

If you prefer editable installs only, `pip install -e .` is enough once your environment already has the repo dependencies.

## Quick Start

1. Fill out `.env`.
2. Run `dexter help` to see the command surface.
3. Run `dexter doctor --network devnet --mode read_only`.
4. If you need PostgreSQL bootstrap, use `dexter database-init` or `python3 database.py`.
5. Start safely on devnet first:

```bash
dexter run --network devnet --mode paper
```

## Core Commands

| Command | Purpose |
| --- | --- |
| `dexter run` / `dexter start` | Guided runtime entrypoint for trader, collector, or analyzer |
| `dexter create` | Seed an existing position into Dexter or build/simulate/send a Pump.fun create |
| `dexter manage` | Inspect or liquidate tracked local recovery-store positions |
| `dexter doctor` | Validate env, RPC reachability, database connectivity, and writable paths |
| `dexter collector` | Run the websocket collector standalone |
| `dexter trade` | Run the trader directly |
| `dexter analyze` | Run the creator analyzer |
| `dexter replay` | Replay a stored normalized session |
| `dexter backtest` | Evaluate a strategy profile offline |
| `dexter verify-migration` | Run the deterministic devnet-safe migration harness |
| `dexter export` | Export normalized research datasets |
| `dexter dashboard` | Render the local operator dashboard |
| `dexter control` | Pause, resume, force-sell, and manage watchlists/blacklists |
| `dexter database-init` | Bootstrap the Dexter database and tables with explicit network-aware config resolution |

Examples:

```bash
dexter help
dexter help create
dexter run --network devnet --mode paper
dexter doctor --network mainnet --mode live
dexter dashboard --network devnet --watch
dexter database-init --network devnet
```

## Network-Aware Create

`dexter create` now expects you to choose one of two modes.

### 1. Seeded Handoff

Use this when you already have the mint and want Dexter to take over position tracking and exit logic.

```bash
dexter create \
  --network devnet \
  --mode paper \
  --mint <mint> \
  --owner <creator> \
  --buy-price 0.000000041 \
  --token-balance 123456
```

Optional seeded flags:

- `--bonding-curve`
- `--market pump_fun|pump_swap`
- `--cost-basis-lamports`
- `--profit-target-pct`
- `--load-database`

By default, seeded create uses the DB-free handoff path.

### 2. On-Chain Pump.fun Create

Use this when Dexter should build the create transaction itself.

Devnet build or simulate:

```bash
dexter create \
  --network devnet \
  --mode simulate \
  --name DexterTest \
  --symbol DXT \
  --uri https://example.invalid/token.json \
  --buy-sol 0.01
```

Devnet live send:

```bash
dexter create \
  --network devnet \
  --mode live \
  --name DexterTest \
  --symbol DXT \
  --uri https://example.invalid/token.json \
  --buy-sol 0.01
```

Mainnet planning only:

```bash
dexter create \
  --network mainnet \
  --mode live \
  --dry-run \
  --name DexterTest \
  --symbol DXT \
  --uri https://example.invalid/token.json \
  --buy-sol 0.01
```

Notes:

- Bare `dexter create` now prints the mode guide instead of only raw validation failures.
- Mainnet create sends remain blocked here; use `--dry-run` or `--simulate-tx` for planning.
- `--network` matters. Do not assume create is using devnet just because your last session did.

## Runtime Modes

| Mode | Behavior |
| --- | --- |
| `read_only` | Observe only, never trade |
| `paper` | Local shadow position simulation |
| `simulate` | Build signed transactions and simulate them |
| `live` | Submit live transactions where the selected network and gates allow it |

## Mainnet MEV Routing

Dexter can translate the MEV sender pattern used in the local `solana-dev-kit` Rust repo for the major providers it references.

Supported provider names:

- `jito`
- `helius`
- `nextblock`
- `zero_slot`
- `temporal`
- `bloxroute`

Minimal `.env` example:

```dotenv
USE_MEV=true
MEV_PROVIDER=jito
MEV_TIP=0.00001
MEV_JITO_KEY=
```

Provider-specific keys:

- `MEV_JITO_KEY` is optional.
- `MEV_NEXTBLOCK_KEY` is required for `MEV_PROVIDER=nextblock`.
- `MEV_ZERO_SLOT_KEY` is required for `MEV_PROVIDER=zero_slot`.
- `MEV_TEMPORAL_KEY` is required for `MEV_PROVIDER=temporal`.
- `MEV_BLOXROUTE_KEY` is required for `MEV_PROVIDER=bloxroute`.

Behavior:

- Dexter only uses MEV routing for mainnet live buy/sell submissions.
- Dexter logs an announcement before each mainnet live buy/sell when MEV routing is active.
- Devnet ignores `USE_MEV`.
- The MEV tip is embedded into the signed transaction as a normal SOL transfer to the provider tip account before the provider-specific HTTP submit.

## Recommended `.env` Baseline

```dotenv
DEXTER_RUNTIME_MODE=read_only
DEXTER_NETWORK=devnet
DEXTER_ALLOW_MAINNET_LIVE=false
DEXTER_MAINNET_DRY_RUN=true
HTTP_URL=https://api.mainnet-beta.solana.com
WS_URL=wss://api.mainnet-beta.solana.com
PRIVATE_KEY=
DATABASE_URL=postgres://dexter_user:replace-me@127.0.0.1:5432/dexter_db
USE_MEV=false
MEV_PROVIDER=jito
MEV_TIP=0.00001
```

Use `.env.example` as the canonical list of supported variables.

## Operator Workflow

Recommended day-to-day flow:

1. `dexter doctor --network devnet --mode read_only`
2. `dexter run --network devnet --mode paper`
3. `dexter create --network devnet ...` for seeded or on-chain tests
4. `dexter manage --network devnet`
5. `dexter dashboard --network devnet --watch`
6. Move to mainnet only after you intentionally flip the live-send gates

## Research Data Tooling

Dexter's normalized research and operator layer powers:

- `replay`
- `export`
- `dashboard`
- `control`
- strategy profile snapshots
- position journals and fill history

This sits on top of the legacy market tables rather than replacing them.

## TUI

If you launch `dexter` with no arguments in an interactive terminal, Dexter opens the curses operator menu. The TUI edits `.env`, launches commands, and exposes the same command surface through menus.

For non-interactive shells and automation, prefer explicit commands:

```bash
dexter help
dexter run --network devnet --mode paper
```

## Notes

- `HTTP_URL` and `WS_URL` only control mainnet. Devnet stays hardcoded by design.
- `create`, `manage`, `doctor`, `collector`, `trade`, `analyze`, `replay`, `backtest`, `verify-migration`, `export`, `dashboard`, `control`, and `database-init` all take `--network` so the target cluster is explicit.
- `manage`, `dashboard`, and `control` operate against the selected network's local state view.
- `database.py` and `dexter database-init` can create or alter local PostgreSQL state. Use them intentionally.

## Credit

Dexter is based on the original FLOCK4H Dexter work and the current repo keeps that lineage while tightening the runtime, operator tooling, and safety model.
