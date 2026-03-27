from __future__ import annotations

import argparse
import ctypes
import os
from pathlib import Path
import secrets
import shutil
import string
import subprocess
import sys
import time
from urllib.parse import quote

from dexter_config import ENV_PATH, PROJECT_ROOT, current_utc_timestamp, read_env_values, update_env_file

DEFAULT_DB_HOST = "127.0.0.1"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "dexter_db"
DEFAULT_DB_USER = "dexter_user"
DEFAULT_ADMIN_DB = "postgres"
DEFAULT_ADMIN_USER = "postgres"
DEFAULT_ADMIN_PASSWORD = "postgres"
DEFAULT_POSTGRES_MAJOR = "17"
PLACEHOLDER_VALUES = {"", "replace-me"}


def _log(message: str) -> None:
    print(f"[{current_utc_timestamp()}] {message}")


def _quote_env_dsn(user: str, password: str, host: str, port: int, database: str) -> str:
    auth = quote(user, safe="")
    if password:
        auth = f"{auth}:{quote(password, safe='')}"
    return f"postgres://{auth}@{host}:{port}/{database}"


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


def _version_key(path: Path) -> tuple[int, ...]:
    parts: list[int] = []
    for token in path.name.replace("-", ".").split("."):
        if token.isdigit():
            parts.append(int(token))
    return tuple(parts) or (0,)


def _discover_postgres_bin(preferred_major: str | None) -> Path | None:
    candidates: list[Path] = []
    seen: set[str] = set()

    psql_on_path = shutil.which("psql")
    if psql_on_path:
        bin_dir = Path(psql_on_path).resolve().parent
        seen.add(str(bin_dir).lower())
        candidates.append(bin_dir)

    roots = {
        os.environ.get("ProgramFiles"),
        os.environ.get("ProgramW6432"),
        os.environ.get("ProgramFiles(x86)"),
    }
    for root in roots:
        if not root:
            continue
        postgres_root = Path(root) / "PostgreSQL"
        if not postgres_root.exists():
            continue
        for child in postgres_root.iterdir():
            bin_dir = child / "bin"
            if not (bin_dir / "psql.exe").exists():
                continue
            key = str(bin_dir).lower()
            if key in seen:
                continue
            seen.add(key)
            candidates.append(bin_dir)

    if not candidates:
        return None

    preferred: list[Path] = []
    fallback: list[Path] = []
    for candidate in candidates:
        version_name = candidate.parent.name
        if preferred_major and (version_name.startswith(f"{preferred_major}.") or version_name == preferred_major):
            preferred.append(candidate)
        else:
            fallback.append(candidate)

    pool = preferred or fallback
    pool.sort(key=lambda item: _version_key(item.parent), reverse=True)
    return pool[0]


def _run_process(command: list[str], *, env: dict[str, str] | None = None, check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        text=True,
        capture_output=True,
        env=env,
        check=check,
    )


def _is_windows_admin() -> bool:
    if os.name != "nt":
        return False
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def _powershell_quote(value: str) -> str:
    return value.replace("'", "''")


def _powershell(command: str) -> subprocess.CompletedProcess[str]:
    return _run_process(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            command,
        ]
    )


def _rebuild_cli_args(args: argparse.Namespace) -> list[str]:
    rebuilt: list[str] = []
    for key in ("network", "major_version", "admin_password", "db_user", "db_password", "db_name", "db_host", "db_port"):
        value = getattr(args, key, None)
        if value in (None, ""):
            continue
        rebuilt.extend([f"--{key.replace('_', '-')}", str(value)])
    for key in ("skip_install", "dry_run"):
        if getattr(args, key, False):
            rebuilt.append(f"--{key.replace('_', '-')}")
    return rebuilt


def _rerun_elevated(args: argparse.Namespace) -> int:
    script_path = str(Path(__file__).resolve())
    python_path = sys.executable or "python"
    command_args = [script_path, *_rebuild_cli_args(args)]
    rendered_args = ", ".join(f"'{_powershell_quote(item)}'" for item in command_args)
    ps_command = (
        f"$p = Start-Process -FilePath '{_powershell_quote(python_path)}' "
        f"-ArgumentList @({rendered_args}) "
        f"-WorkingDirectory '{_powershell_quote(str(PROJECT_ROOT))}' "
        f"-Verb RunAs -Wait -PassThru; "
        "exit $p.ExitCode"
    )
    _log("Administrator approval is required. Windows will open an elevated prompt to finish PostgreSQL setup.")
    result = _powershell(ps_command)
    if result.returncode != 0:
        details = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"Unable to start an elevated PostgreSQL setup session. {details or 'The UAC prompt may have been cancelled.'}")
    return 0


def _postgres_service_names() -> list[str]:
    result = _powershell(
        "Get-Service | "
        "Where-Object { $_.Name -like 'postgresql*' -or $_.DisplayName -like 'PostgreSQL*' } | "
        "Select-Object -ExpandProperty Name"
    )
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _start_postgres_services() -> None:
    for name in _postgres_service_names():
        _powershell(
            f"try {{ Set-Service -Name '{name}' -StartupType Automatic -ErrorAction SilentlyContinue; "
            f"Start-Service -Name '{name}' -ErrorAction Stop }} catch {{ }}"
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
    _log(f"Installing PostgreSQL {major_version} with WinGet.")
    if dry_run:
        print(" ".join(command))
        return

    result = _run_process(command)
    if result.returncode != 0:
        details = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(
            f"WinGet failed to install {package_id}. {details or 'No installer output was captured.'}"
        )


def _wait_for_postgres(psql_path: Path, host: str, port: int, user: str, password: str, *, timeout_seconds: float = 90.0) -> None:
    deadline = time.time() + timeout_seconds
    env = os.environ.copy()
    env["PGPASSWORD"] = password
    command = [
        str(psql_path),
        "-h",
        host,
        "-p",
        str(port),
        "-U",
        user,
        "-d",
        DEFAULT_ADMIN_DB,
        "-tAc",
        "SELECT 1;",
    ]

    last_error = ""
    while time.time() < deadline:
        result = _run_process(command, env=env)
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        if result.returncode == 0 and stdout == "1":
            return
        if "password authentication failed" in stderr.lower():
            raise RuntimeError(
                "PostgreSQL is installed, but the postgres password did not match. "
                "Rerun `dexter database-setup --admin-password <your-local-postgres-password>`."
            )
        last_error = stderr or stdout or last_error
        time.sleep(2)

    raise RuntimeError(
        f"PostgreSQL did not become ready on {host}:{port}. "
        f"Last check result: {last_error or 'connection still unavailable'}"
    )


def _resolve_db_settings(args: argparse.Namespace) -> dict[str, str]:
    env_values = read_env_values(ENV_PATH)

    admin_password = str(args.admin_password or env_values.get("POSTGRES_ADMIN_PASSWORD") or "").strip()
    if _is_placeholder(admin_password):
        admin_password = DEFAULT_ADMIN_PASSWORD

    db_password = str(args.db_password or env_values.get("DB_PASSWORD") or "").strip()
    if _is_placeholder(db_password):
        db_password = _generate_password()

    db_host = str(args.db_host or env_values.get("DB_HOST") or DEFAULT_DB_HOST).strip() or DEFAULT_DB_HOST
    db_port = int(args.db_port or env_values.get("DB_PORT") or DEFAULT_DB_PORT)
    db_name = str(args.db_name or env_values.get("DB_NAME") or DEFAULT_DB_NAME).strip() or DEFAULT_DB_NAME
    db_user = str(args.db_user or env_values.get("DB_USER") or DEFAULT_DB_USER).strip() or DEFAULT_DB_USER

    return {
        "db_host": db_host,
        "db_port": str(db_port),
        "db_name": db_name,
        "db_user": db_user,
        "db_password": db_password,
        "admin_user": DEFAULT_ADMIN_USER,
        "admin_password": admin_password,
        "admin_db": DEFAULT_ADMIN_DB,
    }


def _write_db_env(settings: dict[str, str], pg_dump_path: Path) -> None:
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


def run_windows_setup(args: argparse.Namespace) -> int:
    if os.name != "nt":
        raise RuntimeError(
            "Dexter's automated PostgreSQL bootstrap currently supports Windows only. "
            "On Linux, use `./install_postgre.sh`."
        )
    if not args.dry_run and not _is_windows_admin():
        return _rerun_elevated(args)

    _ensure_env_file()
    settings = _resolve_db_settings(args)

    postgres_bin = _discover_postgres_bin(args.major_version)
    if postgres_bin is None and not args.skip_install:
        _install_postgres_with_winget(args.major_version, dry_run=bool(args.dry_run))
        if args.dry_run:
            return 0
        postgres_bin = _discover_postgres_bin(args.major_version)

    if postgres_bin is None:
        raise RuntimeError(
            "PostgreSQL tools were not found after installation. "
            "Install PostgreSQL with WinGet or rerun with `--major-version` set to an installed version."
        )

    psql_path = postgres_bin / "psql.exe"
    pg_dump_path = postgres_bin / "pg_dump.exe"
    if not psql_path.exists():
        raise RuntimeError(f"psql.exe was not found in {postgres_bin}.")
    if not pg_dump_path.exists():
        raise RuntimeError(f"pg_dump.exe was not found in {postgres_bin}.")

    _log(f"Using PostgreSQL tools from {postgres_bin}.")
    if args.dry_run:
        _log(f"Would write Dexter database settings to {ENV_PATH}.")
        _log("Dry run complete.")
        return 0

    _start_postgres_services()
    _wait_for_postgres(
        psql_path,
        settings["db_host"],
        int(settings["db_port"]),
        settings["admin_user"],
        settings["admin_password"],
    )
    _write_db_env(settings, pg_dump_path)

    from database import run as run_database_init

    _log(f"Running Dexter schema bootstrap with env file {ENV_PATH}.")
    run_database_init(network_override=getattr(args, "network", None))
    _log("Windows PostgreSQL setup completed successfully.")
    _log(f"Dexter wrote database settings to {ENV_PATH}.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Install and configure a local PostgreSQL instance for Dexter on Windows.",
    )
    parser.add_argument("--network", choices=["devnet", "mainnet"], help="Optional Dexter network override for config loading.")
    parser.add_argument("--major-version", default=DEFAULT_POSTGRES_MAJOR, help="WinGet PostgreSQL major version to install when PostgreSQL is missing.")
    parser.add_argument("--admin-password", help="Local postgres superuser password. Defaults to `postgres` for WinGet installs.")
    parser.add_argument("--db-user", help="Dexter application database user. Defaults to DB_USER or dexter_user.")
    parser.add_argument("--db-password", help="Dexter application database password. Defaults to DB_PASSWORD or a generated value.")
    parser.add_argument("--db-name", help="Dexter application database name. Defaults to DB_NAME or dexter_db.")
    parser.add_argument("--db-host", help="Local PostgreSQL host. Defaults to DB_HOST or 127.0.0.1.")
    parser.add_argument("--db-port", type=int, help="Local PostgreSQL port. Defaults to DB_PORT or 5432.")
    parser.add_argument("--skip-install", action="store_true", help="Do not run WinGet; assume PostgreSQL is already installed locally.")
    parser.add_argument("--dry-run", action="store_true", help="Print the intended install/setup actions without changing the machine.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if os.name != "nt":
        print(
            "Dexter's Windows PostgreSQL bootstrap is only supported on Windows. "
            "On Linux, use `./install_postgre.sh` instead."
        )
        return 1
    try:
        return int(run_windows_setup(args))
    except Exception as exc:
        print(f"[{current_utc_timestamp()}] FAIL  Database setup: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
