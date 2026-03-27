from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import secrets
import shutil
import string
import subprocess
from urllib.parse import quote

from dexter_config import ENV_PATH, PROJECT_ROOT, current_utc_timestamp, read_env_values, update_env_file
from dexter_local_postgres import (
    BIN_DIR_ENV,
    DATA_DIR_ENV,
    LOCAL_POSTGRES_DEFAULT_MAJOR,
    LOCAL_POSTGRES_DEFAULT_PORT,
    LOG_FILE_ENV,
    MANAGED_FLAG_ENV,
    PORT_ENV,
    default_local_postgres_data_dir,
    default_local_postgres_log_file,
    discover_postgres_bin,
    ensure_local_postgres_running,
    initialize_local_cluster,
    load_managed_postgres_state,
    start_local_postgres,
    write_managed_postgres_state,
)

DEFAULT_DB_HOST = "127.0.0.1"
DEFAULT_DB_NAME = "dexter_db"
DEFAULT_DB_USER = "dexter_user"
DEFAULT_ADMIN_DB = "postgres"
DEFAULT_ADMIN_USER = "postgres"
PLACEHOLDER_VALUES = {"", "replace-me"}


def _log(message: str) -> None:
    print(f"[{current_utc_timestamp()}] {message}")


def _quote_env_dsn(user: str, password: str, host: str, port: int, database: str) -> str:
    auth = quote(user, safe="")
    if password:
        auth = f"{auth}:{quote(password, safe='')}"
    return f"postgres://{auth}@{host}:{port}/{database}"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _is_placeholder(value: str | None) -> bool:
    return not value or value.strip() in PLACEHOLDER_VALUES


def _generate_password(length: int = 24) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _ensure_env_file() -> None:
    if ENV_PATH.exists():
        return
    example_path = PROJECT_ROOT / ".env.example"
    if example_path.exists():
        shutil.copyfile(example_path, ENV_PATH)
        _log(f"Created {ENV_PATH} from {example_path.name}.")
        return
    ENV_PATH.write_text("", encoding="utf-8")
    _log(f"Created empty env file at {ENV_PATH}.")


def _run_process(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        text=True,
        capture_output=True,
    )


def _install_postgres_with_winget(major_version: str, *, dry_run: bool) -> None:
    winget = shutil.which("winget")
    if not winget:
        raise RuntimeError("WinGet is not installed. Install App Installer from Microsoft and rerun `dexter database-setup`.")

    package_id = f"PostgreSQL.PostgreSQL.{major_version}"
    command = [
        winget,
        "install",
        "-e",
        "--id",
        package_id,
        "--accept-package-agreements",
        "--accept-source-agreements",
        "--silent",
    ]
    _log(f"Installing PostgreSQL {major_version} binaries with WinGet.")
    if dry_run:
        print(" ".join(command))
        return

    result = _run_process(command)
    if result.returncode != 0:
        details = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(
            f"WinGet failed to install {package_id}. {details or 'No installer output was captured.'}"
        )


def _resolve_settings(args: argparse.Namespace) -> dict[str, str]:
    env_values = read_env_values(ENV_PATH)
    major_version = str(args.major_version or LOCAL_POSTGRES_DEFAULT_MAJOR).strip() or LOCAL_POSTGRES_DEFAULT_MAJOR

    cluster_dir = Path(
        str(args.cluster_dir or env_values.get(DATA_DIR_ENV) or default_local_postgres_data_dir(PROJECT_ROOT, major_version=major_version))
    ).expanduser()
    existing_state = load_managed_postgres_state(cluster_dir) or {}

    db_host = str(existing_state.get("db_host") or env_values.get("DB_HOST") or DEFAULT_DB_HOST).strip() or DEFAULT_DB_HOST
    port = int(
        args.db_port
        or existing_state.get("db_port")
        or existing_state.get("port")
        or env_values.get(PORT_ENV)
        or env_values.get("DB_PORT")
        or LOCAL_POSTGRES_DEFAULT_PORT
    )
    db_name = str(args.db_name or existing_state.get("db_name") or env_values.get("DB_NAME") or DEFAULT_DB_NAME).strip() or DEFAULT_DB_NAME
    db_user = str(args.db_user or existing_state.get("db_user") or env_values.get("DB_USER") or DEFAULT_DB_USER).strip() or DEFAULT_DB_USER
    db_password = str(args.db_password or existing_state.get("db_password") or env_values.get("DB_PASSWORD") or "").strip()
    if _is_placeholder(db_password):
        db_password = _generate_password()

    admin_password = str(args.admin_password or existing_state.get("admin_password") or env_values.get("POSTGRES_ADMIN_PASSWORD") or "").strip()
    if _is_placeholder(admin_password):
        admin_password = _generate_password()

    log_file = Path(
        str(args.log_file or existing_state.get("log_file") or env_values.get(LOG_FILE_ENV) or default_local_postgres_log_file(PROJECT_ROOT, major_version=major_version))
    ).expanduser()

    return {
        "db_host": db_host,
        "db_port": str(port),
        "db_name": db_name,
        "db_user": db_user,
        "db_password": db_password,
        "admin_user": DEFAULT_ADMIN_USER,
        "admin_password": admin_password,
        "admin_db": DEFAULT_ADMIN_DB,
        "major_version": major_version,
        "cluster_dir": str(cluster_dir),
        "log_file": str(log_file),
    }


def _write_managed_env(settings: dict[str, str], *, bin_dir: Path, pg_dump_path: Path) -> None:
    db_host = settings["db_host"]
    db_port = int(settings["db_port"])
    db_name = settings["db_name"]
    db_user = settings["db_user"]
    db_password = settings["db_password"]
    admin_user = settings["admin_user"]
    admin_password = settings["admin_password"]
    admin_db = settings["admin_db"]

    update_env_file(
        {
            MANAGED_FLAG_ENV: "true",
            BIN_DIR_ENV: str(bin_dir),
            DATA_DIR_ENV: settings["cluster_dir"],
            LOG_FILE_ENV: settings["log_file"],
            PORT_ENV: str(db_port),
            "DB_HOST": db_host,
            "DB_PORT": str(db_port),
            "DB_NAME": db_name,
            "DB_USER": db_user,
            "DB_PASSWORD": db_password,
            "DATABASE_URL": _quote_env_dsn(db_user, db_password, db_host, db_port, db_name),
            "POSTGRES_ADMIN_HOST": db_host,
            "POSTGRES_ADMIN_PORT": str(db_port),
            "POSTGRES_ADMIN_DB": admin_db,
            "POSTGRES_ADMIN_USER": admin_user,
            "POSTGRES_ADMIN_PASSWORD": admin_password,
            "POSTGRES_ADMIN_DSN": _quote_env_dsn(admin_user, admin_password, db_host, db_port, admin_db),
            "DEXTER_PG_DUMP_PATH": str(pg_dump_path),
            "DEXTER_BACKUP_ENABLED": "true",
        },
        path=ENV_PATH,
    )


async def _set_role_password(dsn: str, role_name: str, password: str) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f"ALTER USER {_quote_ident(role_name)} WITH PASSWORD {_quote_literal(password)};"
        )
    finally:
        await conn.close()


def _existing_admin_dsn(state: dict[str, str]) -> str:
    host = str(state.get("db_host") or DEFAULT_DB_HOST)
    port = int(state.get("db_port") or state.get("port") or LOCAL_POSTGRES_DEFAULT_PORT)
    user = str(state.get("admin_user") or DEFAULT_ADMIN_USER)
    password = str(state.get("admin_password") or "")
    database = str(state.get("admin_db") or DEFAULT_ADMIN_DB)
    return _quote_env_dsn(user, password, host, port, database)


def _maybe_rotate_admin_password(existing_state: dict[str, str] | None, settings: dict[str, str]) -> None:
    if not existing_state:
        return

    current_password = str(existing_state.get("admin_password") or "").strip()
    desired_password = settings["admin_password"]
    if not current_password or current_password == desired_password:
        return

    _log("Refreshing Dexter local postgres superuser password.")
    asyncio.run(_set_role_password(_existing_admin_dsn(existing_state), settings["admin_user"], desired_password))


def run_windows_setup(args: argparse.Namespace) -> int:
    if os.name != "nt":
        raise RuntimeError(
            "Dexter's automated PostgreSQL bootstrap currently supports Windows only. "
            "On Linux, use `./install_postgre.sh`."
        )

    _ensure_env_file()
    settings = _resolve_settings(args)
    cluster_dir = Path(settings["cluster_dir"])
    log_file = Path(settings["log_file"])
    existing_state = load_managed_postgres_state(cluster_dir)

    postgres_bin: Path | None = None
    if existing_state:
        state_bin_dir = Path(str(existing_state.get("bin_dir") or ""))
        if state_bin_dir and (state_bin_dir / "psql.exe").exists():
            postgres_bin = state_bin_dir
    if postgres_bin is None:
        postgres_bin = discover_postgres_bin(settings["major_version"])

    if postgres_bin is None and not args.skip_install:
        _install_postgres_with_winget(settings["major_version"], dry_run=bool(args.dry_run))
        if args.dry_run:
            return 0
        postgres_bin = discover_postgres_bin(settings["major_version"])

    if postgres_bin is None:
        raise RuntimeError(
            "PostgreSQL tools were not found. Install PostgreSQL first or rerun `dexter database-setup` without `--skip-install`."
        )

    psql_path = postgres_bin / "psql.exe"
    pg_dump_path = postgres_bin / "pg_dump.exe"
    if not psql_path.exists():
        raise RuntimeError(f"psql.exe was not found in {postgres_bin}.")
    if not pg_dump_path.exists():
        raise RuntimeError(f"pg_dump.exe was not found in {postgres_bin}.")

    _log(f"Using PostgreSQL tools from {postgres_bin}.")
    _log(f"Dexter will manage its own local PostgreSQL cluster in {cluster_dir}.")

    if args.dry_run:
        _log(f"Would initialize or reuse a private local PostgreSQL cluster on {settings['db_host']}:{settings['db_port']}.")
        _log(f"Would create or refresh Dexter's local postgres admin password, app user, database, tables, and schema metadata.")
        _log(f"Would write Dexter database settings to {ENV_PATH}.")
        _log("Dry run complete.")
        return 0

    initialize_local_cluster(
        bin_dir=postgres_bin,
        data_dir=cluster_dir,
        log_file=log_file,
        port=int(settings["db_port"]),
        admin_user=settings["admin_user"],
        admin_password=settings["admin_password"],
    )
    start_local_postgres(
        bin_dir=postgres_bin,
        data_dir=cluster_dir,
        log_file=log_file,
        port=int(settings["db_port"]),
        timeout_seconds=90.0,
    )
    _maybe_rotate_admin_password(existing_state, settings)
    write_managed_postgres_state(
        bin_dir=postgres_bin,
        data_dir=cluster_dir,
        log_file=log_file,
        port=int(settings["db_port"]),
        db_host=settings["db_host"],
        db_port=int(settings["db_port"]),
        db_name=settings["db_name"],
        db_user=settings["db_user"],
        db_password=settings["db_password"],
        admin_db=settings["admin_db"],
        admin_user=settings["admin_user"],
        admin_password=settings["admin_password"],
        pg_dump_path=pg_dump_path,
    )
    _write_managed_env(settings, bin_dir=postgres_bin, pg_dump_path=pg_dump_path)
    ensure_local_postgres_running(timeout_seconds=90.0)

    from database import run as run_database_init

    _log(f"Running Dexter schema bootstrap with env file {ENV_PATH}.")
    run_database_init(network_override=getattr(args, "network", None))
    _log("Windows PostgreSQL setup completed successfully.")
    _log(f"Dexter now owns the local postgres admin password, app role, database, tables, and schema. Env file: {ENV_PATH}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Install and configure a Dexter-managed local PostgreSQL instance on Windows.",
    )
    parser.add_argument("--network", choices=["devnet", "mainnet"], help="Optional Dexter network override for config loading.")
    parser.add_argument("--major-version", default=LOCAL_POSTGRES_DEFAULT_MAJOR, help="WinGet PostgreSQL major version to install when PostgreSQL binaries are missing.")
    parser.add_argument("--admin-password", help="Password for Dexter's local postgres superuser. Defaults to the existing managed password or a generated password.")
    parser.add_argument("--db-user", help="Dexter application database user. Defaults to the managed value, DB_USER, or dexter_user.")
    parser.add_argument("--db-password", help="Dexter application database password. Defaults to the managed value, DB_PASSWORD, or a generated value.")
    parser.add_argument("--db-name", help="Dexter application database name. Defaults to the managed value, DB_NAME, or dexter_db.")
    parser.add_argument("--db-port", type=int, help=f"Local PostgreSQL port. Defaults to the managed value or {LOCAL_POSTGRES_DEFAULT_PORT}.")
    parser.add_argument("--cluster-dir", help="Path for Dexter's local PostgreSQL data directory. Defaults to .dexter/postgres/<version>/data.")
    parser.add_argument("--log-file", help="Path for Dexter's local PostgreSQL log file.")
    parser.add_argument("--skip-install", action="store_true", help="Do not run WinGet; assume PostgreSQL binaries are already installed locally.")
    parser.add_argument("--dry-run", action="store_true", help="Print the intended install/setup actions without changing the machine.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(run_windows_setup(args))
    except Exception as exc:
        print(f"[{current_utc_timestamp()}] FAIL  Database setup: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
