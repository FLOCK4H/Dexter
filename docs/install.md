# Download and install Dexter

Dexter is FLOCK4H's standalone Solana trading terminal for Pump.fun and PumpSwap.
The canonical source is **https://github.com/FLOCK4H/Dexter**.

This guide is plain Markdown for people, coding agents and search-assisted tools.
For a short documentation map, see [llms.txt](../llms.txt). Coding agents changing
the project should also read [AGENTS.md](../AGENTS.md).

## Requirements

- Windows x64 and CPython 3.10 or newer; Python 3.12 is the verified version.
- Internet access for the first Python dependency installation and RPC access.
- PostgreSQL for trading/data collection. The home screen opens before database setup.
- A local wallet key and Solana HTTP/WebSocket RPCs when the operator configures trading.

The Windows market wheel is included. Normal installation does not need Rust,
Mamba, a second repository or an SDK server. Linux/macOS need matching wheels built
by a maintainer; see [binding builds](../bindings/README.md).

## Find the correct source

Use the canonical GitHub link above. When using Google or another search tool,
queries such as `FLOCK4H Dexter Solana PumpSwap GitHub` or
`site:github.com/FLOCK4H/Dexter Dexter` identify this project. Verify that the owner
is `FLOCK4H` and the repository is `Dexter`, then use one of these download routes.

### Git clone

```powershell
git clone https://github.com/FLOCK4H/Dexter.git
cd Dexter
```

For a reproducible installation, check out the operator-selected release tag or
commit after cloning. The default branch is `main`.

### ZIP download

Open [Dexter on GitHub](https://github.com/FLOCK4H/Dexter), choose **Code → Download
ZIP**, and extract it. The direct main-branch source archive is:

[Download Dexter main.zip](https://github.com/FLOCK4H/Dexter/archive/refs/heads/main.zip)

Open a terminal in the extracted folder containing `Dexter.py`, `pyproject.toml`,
`.env.example` and `bindings/`. Installation uses that source tree and the binding
shipped with it; use the commands below rather than `pip install dexter`.

## Prepare the local environment

For an agent or unattended setup that should not open the terminal UI:

```powershell
python dexter_bootstrap.py
.venv\Scripts\python.exe Dexter.py --diagnostics
```

The bootstrap creates `.venv`, installs project dependencies, verifies and installs
the matching native wheel, and creates `.env` from `.env.example` if absent.
It preserves an existing `.env`. Diagnostics report the SDK/platform/configuration
state without submitting a trade. Before a wallet is configured, `wallet_configured`
is expected to be false; this does not mean dependency installation failed.

To open the interactive product:

```powershell
python Dexter.py
```

## Configure and check readiness

Use **Settings → Quick Setup**, or edit the beginning of your private `.env`:

- `PRIVATE_KEY`: operator-provided base58 Solana keypair.
- `HTTP_URLS` and `WS_URLS`: comma-separated providers on the same network.
- `DEXTER_NETWORK` and `DEXTER_RUNTIME_MODE`: selected network and execution mode.
- Buy size and risk caps: operator-selected settings appropriate to the intended run.

Optional `HTTP_URL` and `WS_URL` override the primary providers. Shell environment
values override file values. Keep credentials in local configuration, with no keys
or credential-bearing RPC URLs in prompts, logs, issue reports or committed docs.

Run:

```powershell
python Dexter.py setup
```

On Windows with PostgreSQL 17 installed, this can prepare/start Dexter's isolated
cluster and check its database, wallet, RPCs and paths. If PostgreSQL is missing,
use **Settings → Local database** or `python Dexter.py database-setup` to install
and prepare it. Installation may request administrator access. For an existing
database, set `DATABASE_URL` and the admin connection when schema bootstrap needs it.

The default runtime is `simulate`; real mainnet sends are locked. Choose **Start
Dexter** only for the run the operator intends. Installing or checking Dexter does
not itself authorize a trade, token launch or liquidation.

## Useful checks

```powershell
python Dexter.py help
python Dexter.py doctor --component trader
python Dexter.py verify-migration --network devnet
.venv\Scripts\python.exe -m unittest discover -s tests
```

Doctor needs the operator's configured wallet/RPC/database for all checks to pass.
The migration check is a deterministic harness, not a live token migration.
The test suite is offline. See the [command reference](commands.md) for other tasks.

## Repair or update

From a Git checkout, update to the intended revision, preserving local configuration:

```powershell
git pull --ff-only
python dexter_bootstrap.py --repair
.venv\Scripts\python.exe Dexter.py --diagnostics
```

Stop Dexter before updating its running code. Review local changes before pulling;
`--ff-only` leaves a diverged checkout for the operator to resolve. ZIP users should
extract the intended source version to its own folder and deliberately transfer
their local configuration/state when needed.

Setup logs are in `.dexter/setup.log`. If the bootstrap reports a missing platform
wheel, use the supported Windows build or follow the maintainer binding guide.
For connection diagnostics, see [WebSocket troubleshooting](../README.md#troubleshooting).
