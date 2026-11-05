# Dexter agent guide

This is the public setup and maintenance guide for coding agents working in this
repository. For operator installation, start with [docs/install.md](docs/install.md).
The documentation index is [llms.txt](llms.txt).

## Identify the product

- Canonical repository: https://github.com/FLOCK4H/Dexter
- Product: standalone Solana terminal for Pump.fun and PumpSwap.
- Included binding: Windows x64, CPython 3.10+, verified on Python 3.12.
- Use the repository bootstrap. The project does not document installation from
  a package index under an ambiguous `dexter` package name.
- Linux/macOS require matching native wheels; see [bindings/README.md](bindings/README.md).

## Install and check

From a checkout on Windows, create the local runtime without starting the trader:

```powershell
python dexter_bootstrap.py
.venv\Scripts\python.exe Dexter.py --diagnostics
.venv\Scripts\python.exe Dexter.py help
```

`python Dexter.py` opens the terminal home screen. First launch creates `.env`
from `.env.example` if absent. Existing config is preserved. Operator-owned wallet
keys and RPC credentials belong only in the ignored local `.env`.

After the operator configures wallet/RPCs and PostgreSQL, `python Dexter.py setup`
checks readiness and can prepare the managed database. Database installation uses
`python Dexter.py database-setup` on Windows. It may require administrator access.
The setup and diagnostics commands do not submit trades.

## Verification

Run the offline suite with the prepared runtime:

```powershell
.venv\Scripts\python.exe -m unittest discover -s tests
git diff --check
```

`python Dexter.py verify-migration --network devnet` runs a deterministic migration
harness; it is not an observed live migration. Follow `.github/workflows/tests.yml`
for CI. Use focused checks for the code changed, and report actual results.

## Source map

| Path | Responsibility |
| --- | --- |
| `Dexter.py`, `dexter_tui.py` | Trading runtime, terminal home and menus. |
| `dexter_bootstrap.py` | Standard-library bootstrap, venv and wheel checksum checks. |
| `dexter_config.py`, `.env.example` | Local configuration, defaults and gates. |
| `dexter_setup.py`, `database_setup.py` | Readiness and managed PostgreSQL setup. |
| `dexter_network.py`, `DexLab/wsLogs.py` | RPC pools, subscriptions and reconnect handling. |
| `dexter_execution.py`, `bindings/` | Python/native boundary and packaged market SDK. |
| `DexAI/`, `DexLab/`, `dexter_strategy.py` | Creator analysis, market storage and strategy. |
| `tests/`, `docs/` | Offline regression checks and public documentation. |

## Preserve the operating boundaries

- Keep Dexter independent of another checkout or its `.env`. Never read Mamba
  configuration during normal operation.
- Keep protocol account layouts, instruction builders and market quotes in the
  native SDK. Maintain bindings through the pinned source/update workflow.
- Keep private keys in Python-side signing; the SDK receives public keys and
  builds instructions. Preserve mainnet send gates, trade caps and cluster isolation.
- Treat trade submission, token launch and manual exits as financial actions.
  Installation or testing alone is not authorization to broadcast transactions.
- Preserve exact held amounts, recovery state and known signatures across
  migration or uncertain RPC outcomes. Do not retry an unresolved trade as a new order.
- Keep blocking HTTP and disk work out of WebSocket receive processing. Preserve
  enabled heartbeat detection and credential-free connection diagnostics.
- Keep `.env`, local databases, logs, caches and private authoring notes out of
  commits and release archives. The public agent guide and index are intentional
  product documentation, not private session history.
- Use the existing terminal UI and public docs as product truth. Preserve keyboard
  navigation, compact layouts, secret masking and accessible message bodies.
