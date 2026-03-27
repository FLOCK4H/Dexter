import asyncio
import os

import asyncpg

from dexter_config import ENV_PATH, ensure_directories, load_config, load_env_file, log_startup_summary, update_env_file, validate_config
from dexter_data_store import PHASE2_SCHEMA_STATEMENTS
from dexter_local_postgres import (
    apply_managed_postgres_state_to_environment,
    ensure_local_postgres_running,
    load_managed_postgres_state,
    managed_state_to_env_updates,
    persist_managed_postgres_state_from_config,
)


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _connection_help(config, exc: Exception, *, admin: bool) -> str:
    host = config.database.admin_host if admin else config.database.host
    port = config.database.admin_port if admin else config.database.port
    role = config.database.admin_user if admin else config.database.user
    target = "PostgreSQL admin connection" if admin else "Dexter app database"
    message = (
        f"Unable to reach the {target} at {host}:{port} as {role}: {exc}. "
        "Make sure PostgreSQL is installed, the local PostgreSQL server is running, and the configured password is correct."
    )
    if os.name == "nt":
        message = (
            f"{message} On Windows, the easiest fix is `dexter database-setup`, "
            "which repairs Dexter's local postgres admin password, app role, database, and tables for you."
        )
    else:
        message = f"{message} On Linux, run `./install_postgre.sh` if PostgreSQL is not installed yet."
    return message


async def initialize_db(mode_override: str | None = None, network_override: str | None = None):
    load_env_file()
    if apply_managed_postgres_state_to_environment():
        managed_state = load_managed_postgres_state()
        if managed_state:
            update_env_file(managed_state_to_env_updates(managed_state), path=ENV_PATH)
    config = load_config(mode_override, network_override=network_override)
    ensure_local_postgres_running(config)
    errors, warnings = validate_config(config, "database")
    if errors:
        raise RuntimeError("; ".join(errors))
    for warning in warnings:
        print(f"WARN: {warning}")

    ensure_directories(config)
    log_startup_summary(__import__("logging").getLogger("dexter.database"), config, "database")

    conn = None
    try:
        conn = await asyncpg.connect(config.database.dsn)
        print(f"Database '{config.database.name}' is already reachable; continuing with schema bootstrap.")
    except Exception:
        conn = None

    admin_conn = None
    admin_error: Exception | None = None
    if config.database.admin_dsn:
        try:
            admin_conn = await asyncpg.connect(config.database.admin_dsn)
        except Exception as exc:
            admin_error = exc
            if conn is None:
                raise RuntimeError(_connection_help(config, exc, admin=True)) from exc

    if admin_conn is not None:
        try:
            await admin_conn.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;")
            await admin_conn.execute("COMMIT")

            user_exists = await admin_conn.fetchval(
                "SELECT 1 FROM pg_roles WHERE rolname = $1;",
                config.database.user,
            )
            if not user_exists:
                await admin_conn.execute(
                    f"CREATE USER {_quote_ident(config.database.user)} "
                    f"WITH PASSWORD {_quote_literal(config.database.password)};"
                )
                print(f"User '{config.database.user}' created.")
            else:
                await admin_conn.execute(
                    f"ALTER USER {_quote_ident(config.database.user)} "
                    f"WITH PASSWORD {_quote_literal(config.database.password)};"
                )
                print(f"User '{config.database.user}' password refreshed.")

            db_exists = await admin_conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1;",
                config.database.name,
            )
            if not db_exists:
                await admin_conn.execute("COMMIT")
                await admin_conn.execute(
                    f"CREATE DATABASE {_quote_ident(config.database.name)} "
                    f"OWNER {_quote_ident(config.database.user)};"
                )
                print(f"Database '{config.database.name}' created.")
        finally:
            await admin_conn.close()
    elif admin_error is not None and conn is not None:
        print(f"WARN: {_connection_help(config, admin_error, admin=True)}")

    if conn is None:
        try:
            conn = await asyncpg.connect(config.database.dsn)
        except Exception as exc:
            raise RuntimeError(_connection_help(config, exc, admin=False)) from exc

    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mints (
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
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stagnant_mints (
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
            """
        )

        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mints_mint_id ON mints(mint_id);
            CREATE INDEX IF NOT EXISTS idx_stagnant_mints_mint_id ON stagnant_mints(mint_id);
            CREATE INDEX IF NOT EXISTS idx_mints_timestamp ON mints(timestamp);
            CREATE INDEX IF NOT EXISTS idx_stagnant_mints_timestamp ON stagnant_mints(timestamp);
            """
        )

        if config.phase2.enabled:
            for statement in PHASE2_SCHEMA_STATEMENTS:
                await conn.execute(statement)
    finally:
        await conn.close()

    persisted_state = persist_managed_postgres_state_from_config(config)
    if persisted_state is not None:
        managed_state = load_managed_postgres_state()
        if managed_state:
            update_env_file(managed_state_to_env_updates(managed_state), path=ENV_PATH)
    print("PostgreSQL database, tables, and indexes initialized successfully.")


def run(mode_override: str | None = None, network_override: str | None = None):
    asyncio.run(initialize_db(mode_override=mode_override, network_override=network_override))
    return 0


if __name__ == "__main__":
    run()
