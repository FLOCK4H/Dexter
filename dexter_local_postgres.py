from __future__ import annotations

import json
import locale
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
from urllib.parse import quote

from dexter_config import AppConfig, PROJECT_ROOT

LOCAL_POSTGRES_DEFAULT_MAJOR = "17"
LOCAL_POSTGRES_DEFAULT_PORT = 55432
MANAGED_FLAG_ENV = "DEXTER_LOCAL_POSTGRES_MANAGED"
BIN_DIR_ENV = "DEXTER_LOCAL_POSTGRES_BIN_DIR"
DATA_DIR_ENV = "DEXTER_LOCAL_POSTGRES_DATA_DIR"
LOG_FILE_ENV = "DEXTER_LOCAL_POSTGRES_LOG_FILE"
PORT_ENV = "DEXTER_LOCAL_POSTGRES_PORT"
STATE_FILE_NAME = "dexter-managed.json"


def _env_flag(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def is_managed_local_postgres() -> bool:
    return _env_flag(MANAGED_FLAG_ENV, False)


def default_local_postgres_root(project_root: Path = PROJECT_ROOT) -> Path:
    return project_root / ".dexter" / "postgres"


def default_local_postgres_data_dir(
    project_root: Path = PROJECT_ROOT,
    *,
    major_version: str = LOCAL_POSTGRES_DEFAULT_MAJOR,
) -> Path:
    return default_local_postgres_root(project_root) / major_version / "data"


def default_local_postgres_log_file(
    project_root: Path = PROJECT_ROOT,
    *,
    major_version: str = LOCAL_POSTGRES_DEFAULT_MAJOR,
) -> Path:
    return default_local_postgres_root(project_root) / major_version / "postgres.log"


def configured_local_postgres_port() -> int:
    raw = (os.environ.get(PORT_ENV) or "").strip()
    if not raw:
        return LOCAL_POSTGRES_DEFAULT_PORT
    try:
        return int(raw)
    except ValueError:
        return LOCAL_POSTGRES_DEFAULT_PORT


def configured_local_postgres_bin_dir() -> Path | None:
    raw = (os.environ.get(BIN_DIR_ENV) or "").strip()
    return Path(raw) if raw else None


def configured_local_postgres_data_dir() -> Path | None:
    raw = (os.environ.get(DATA_DIR_ENV) or "").strip()
    return Path(raw) if raw else None


def configured_local_postgres_log_file() -> Path | None:
    raw = (os.environ.get(LOG_FILE_ENV) or "").strip()
    return Path(raw) if raw else None


def _build_postgres_dsn(user: str, password: str, host: str, port: int, database: str) -> str:
    auth = quote(user, safe="")
    if password:
        auth = f"{auth}:{quote(password, safe='')}"
    return f"postgres://{auth}@{host}:{int(port)}/{database}"


def managed_state_file(data_dir: Path) -> Path:
    return data_dir.parent / STATE_FILE_NAME


def _version_key(path: Path) -> tuple[int, ...]:
    parts: list[int] = []
    for token in path.name.replace("-", ".").split("."):
        if token.isdigit():
            parts.append(int(token))
    return tuple(parts) or (0,)


def _discover_managed_state_file(project_root: Path = PROJECT_ROOT) -> Path | None:
    root = default_local_postgres_root(project_root)
    if not root.exists():
        return None

    candidates = [child / STATE_FILE_NAME for child in root.iterdir() if (child / STATE_FILE_NAME).exists()]
    if not candidates:
        return None

    candidates.sort(key=lambda item: _version_key(item.parent), reverse=True)
    return candidates[0]


def load_managed_postgres_state(data_dir: Path | None = None) -> dict[str, str] | None:
    state_path: Path | None = None
    if data_dir is not None:
        candidate = managed_state_file(data_dir)
        if candidate.exists():
            state_path = candidate
    else:
        configured_data_dir = configured_local_postgres_data_dir()
        if configured_data_dir is not None:
            candidate = managed_state_file(configured_data_dir)
            if candidate.exists():
                state_path = candidate
        if state_path is None:
            state_path = _discover_managed_state_file()

    if state_path is None or not state_path.exists():
        return None

    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None

    state: dict[str, str] = {}
    for key, value in payload.items():
        if value is None:
            continue
        state[str(key)] = str(value)
    return state or None


def managed_state_to_env_updates(state: dict[str, str]) -> dict[str, str]:
    db_host = state.get("db_host", "127.0.0.1")
    db_port = state.get("db_port") or state.get("port") or str(LOCAL_POSTGRES_DEFAULT_PORT)
    db_name = state.get("db_name", "dexter_db")
    db_user = state.get("db_user", "dexter_user")
    db_password = state.get("db_password", "")
    admin_db = state.get("admin_db", "postgres")
    admin_user = state.get("admin_user", "postgres")
    admin_password = state.get("admin_password", "")
    bin_dir = state.get("bin_dir", "")
    data_dir = state.get("data_dir", "")
    log_file = state.get("log_file", "")
    pg_dump_path = state.get("pg_dump_path", str(Path(bin_dir) / "pg_dump.exe") if bin_dir else "pg_dump")

    return {
        MANAGED_FLAG_ENV: "true",
        BIN_DIR_ENV: bin_dir,
        DATA_DIR_ENV: data_dir,
        LOG_FILE_ENV: log_file,
        PORT_ENV: db_port,
        "DB_HOST": db_host,
        "DB_PORT": db_port,
        "DB_NAME": db_name,
        "DB_USER": db_user,
        "DB_PASSWORD": db_password,
        "DATABASE_URL": _build_postgres_dsn(db_user, db_password, db_host, int(db_port), db_name),
        "POSTGRES_ADMIN_HOST": db_host,
        "POSTGRES_ADMIN_PORT": db_port,
        "POSTGRES_ADMIN_DB": admin_db,
        "POSTGRES_ADMIN_USER": admin_user,
        "POSTGRES_ADMIN_PASSWORD": admin_password,
        "POSTGRES_ADMIN_DSN": _build_postgres_dsn(admin_user, admin_password, db_host, int(db_port), admin_db),
        "DEXTER_PG_DUMP_PATH": pg_dump_path,
        "DEXTER_BACKUP_ENABLED": "true",
    }


def apply_managed_postgres_state_to_environment() -> bool:
    if not is_managed_local_postgres():
        return False

    state = load_managed_postgres_state()
    if not state:
        return False

    for key, value in managed_state_to_env_updates(state).items():
        os.environ[key] = value
    return True


def write_managed_postgres_state(
    *,
    bin_dir: Path,
    data_dir: Path,
    log_file: Path,
    port: int,
    db_host: str,
    db_port: int,
    db_name: str,
    db_user: str,
    db_password: str,
    admin_db: str,
    admin_user: str,
    admin_password: str,
    pg_dump_path: Path | str,
) -> Path:
    state_path = managed_state_file(data_dir)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "managed": True,
        "bin_dir": str(bin_dir),
        "data_dir": str(data_dir),
        "log_file": str(log_file),
        "port": int(port),
        "db_host": db_host,
        "db_port": int(db_port),
        "db_name": db_name,
        "db_user": db_user,
        "db_password": db_password,
        "admin_db": admin_db,
        "admin_user": admin_user,
        "admin_password": admin_password,
        "pg_dump_path": str(pg_dump_path),
    }
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return state_path


def persist_managed_postgres_state_from_config(config: AppConfig) -> Path | None:
    if not is_managed_local_postgres():
        return None

    bin_dir = configured_local_postgres_bin_dir()
    data_dir = configured_local_postgres_data_dir()
    log_file = configured_local_postgres_log_file()
    if bin_dir is None or data_dir is None or log_file is None:
        return None

    return write_managed_postgres_state(
        bin_dir=bin_dir,
        data_dir=data_dir,
        log_file=log_file,
        port=configured_local_postgres_port(),
        db_host=config.database.host or "127.0.0.1",
        db_port=config.database.port or configured_local_postgres_port(),
        db_name=config.database.name or "dexter_db",
        db_user=config.database.user or "dexter_user",
        db_password=config.database.password,
        admin_db=config.database.admin_name or "postgres",
        admin_user=config.database.admin_user or "postgres",
        admin_password=config.database.admin_password,
        pg_dump_path=config.backup.pg_dump_path,
    )


def discover_postgres_bin(preferred_major: str | None = None) -> Path | None:
    configured = configured_local_postgres_bin_dir()
    if configured and (configured / "psql.exe").exists():
        return configured

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


def _run_process(command: list[str], *, cwd: Path | None = None, check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd or PROJECT_ROOT),
        text=True,
        capture_output=True,
        check=check,
    )


def _start_pg_ctl(
    *,
    pg_ctl: Path,
    data_dir: Path,
    log_file: Path,
    host: str,
    port: int,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(pg_ctl),
        "-D",
        str(data_dir),
        "-l",
        str(log_file),
        "-w",
        "-t",
        str(max(5, int(timeout_seconds))),
        "-o",
        f"-h {host} -p {int(port)}",
        "start",
    ]
    with tempfile.TemporaryFile(mode="w+b") as stream:
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            stdout=stream,
            stderr=stream,
        )
        stream.seek(0)
        rendered = stream.read().decode(locale.getpreferredencoding(False), errors="replace")
    return subprocess.CompletedProcess(
        args=command,
        returncode=completed.returncode,
        stdout=rendered,
        stderr="",
    )


def _postgres_ctl_paths(bin_dir: Path) -> tuple[Path, Path, Path]:
    initdb = bin_dir / "initdb.exe"
    pg_ctl = bin_dir / "pg_ctl.exe"
    pg_isready = bin_dir / "pg_isready.exe"
    return initdb, pg_ctl, pg_isready


def _wait_for_port(host: str, port: int, *, timeout_seconds: float = 30.0) -> None:
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(2.0)
            try:
                sock.connect((host, port))
                return
            except OSError as exc:
                last_error = str(exc)
        time.sleep(1)
    raise RuntimeError(f"PostgreSQL did not start listening on {host}:{port}. Last socket error: {last_error or 'timeout'}")


def _wait_for_postmaster_release(data_dir: Path, host: str, port: int, *, timeout_seconds: float = 15.0) -> None:
    deadline = time.time() + timeout_seconds
    pid_file = data_dir / "postmaster.pid"
    while time.time() < deadline:
        if not pid_file.exists() and not _server_is_ready(host, port):
            return
        time.sleep(1)


def _server_is_ready(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        try:
            sock.connect((host, port))
            return True
        except OSError:
            return False


def initialize_local_cluster(
    *,
    bin_dir: Path,
    data_dir: Path,
    log_file: Path,
    port: int,
    admin_user: str,
    admin_password: str,
) -> None:
    if (data_dir / "PG_VERSION").exists():
        return

    initdb, _pg_ctl, _pg_isready = _postgres_ctl_paths(bin_dir)
    if not initdb.exists():
        raise RuntimeError(f"initdb.exe was not found in {bin_dir}.")

    data_dir.parent.mkdir(parents=True, exist_ok=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
        handle.write(admin_password)
        handle.write("\n")
        pwfile = Path(handle.name)

    try:
        result = _run_process(
            [
                str(initdb),
                "-D",
                str(data_dir),
                "-U",
                admin_user,
                "-A",
                "scram-sha-256",
                "--pwfile",
                str(pwfile),
                "--encoding",
                "UTF8",
            ]
        )
    finally:
        try:
            pwfile.unlink()
        except OSError:
            pass

    if result.returncode != 0:
        details = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"initdb failed for {data_dir}. {details or 'No output was captured.'}")

    auto_conf = data_dir / "postgresql.auto.conf"
    auto_conf.parent.mkdir(parents=True, exist_ok=True)
    with auto_conf.open("a", encoding="utf-8") as handle:
        handle.write("listen_addresses = '127.0.0.1'\n")
        handle.write(f"port = {int(port)}\n")


def start_local_postgres(
    *,
    bin_dir: Path,
    data_dir: Path,
    log_file: Path,
    port: int,
    host: str = "127.0.0.1",
    timeout_seconds: float = 60.0,
) -> bool:
    if not (data_dir / "PG_VERSION").exists():
        raise RuntimeError(
            f"Dexter's managed local PostgreSQL data directory was not found at {data_dir}. "
            "Rerun `dexter database-setup`."
        )

    if _server_is_ready(host, port):
        return True

    _initdb, pg_ctl, _pg_isready = _postgres_ctl_paths(bin_dir)
    if not pg_ctl.exists():
        raise RuntimeError(f"pg_ctl.exe was not found in {bin_dir}.")

    _wait_for_postmaster_release(data_dir, host, port)

    active_log_file = log_file
    result = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="")
    for _attempt in range(5):
        result = _start_pg_ctl(
            pg_ctl=pg_ctl,
            data_dir=data_dir,
            log_file=active_log_file,
            host=host,
            port=port,
            timeout_seconds=timeout_seconds,
        )
        details = (result.stderr or result.stdout or "").strip()
        lowered = details.lower()
        if result.returncode == 0:
            break
        if "could not open log file" in lowered and "permission denied" in lowered:
            active_log_file = log_file.parent / f"postgres-start-{int(time.time())}.log"
            continue
        if "being used by another process" in lowered or "uzywany przez inny proces" in lowered:
            time.sleep(2)
            continue
        break

    if result.returncode != 0 and _server_is_ready(host, port):
        return True

    if result.returncode != 0:
        details = (result.stderr or result.stdout or "").strip()
        lowered = details.lower()
        if "another server may be running" in lowered or "inny serwer" in lowered:
            _wait_for_port(host, port, timeout_seconds=10.0)
            return True
        raise RuntimeError(
            f"Dexter could not start its local PostgreSQL server from {data_dir}. "
            f"{details or 'No pg_ctl output was captured.'}"
        )

    if active_log_file != log_file:
        os.environ[LOG_FILE_ENV] = str(active_log_file)
    _wait_for_port(host, port, timeout_seconds=timeout_seconds)
    return True


def ensure_local_postgres_running(
    config: AppConfig | None = None,
    *,
    timeout_seconds: float = 60.0,
) -> bool:
    if not is_managed_local_postgres():
        return False

    host = "127.0.0.1"
    port = configured_local_postgres_port()
    data_dir = configured_local_postgres_data_dir()
    log_file = configured_local_postgres_log_file()
    bin_dir = configured_local_postgres_bin_dir()

    if data_dir is None or log_file is None or bin_dir is None:
        raise RuntimeError(
            "Dexter's managed local PostgreSQL is enabled, but one of its required paths is missing. "
            "Rerun `dexter database-setup`."
        )

    return start_local_postgres(
        bin_dir=bin_dir,
        data_dir=data_dir,
        log_file=log_file,
        port=port,
        host=host,
        timeout_seconds=timeout_seconds,
    )
