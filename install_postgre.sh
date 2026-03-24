#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-$PROJECT_ROOT/.env}"
PG_PASSWORD_AUTH_METHOD="${PG_PASSWORD_AUTH_METHOD:-scram-sha-256}"
DEFAULT_DB_HOST="127.0.0.1"
DEFAULT_DB_PORT="5432"
DEFAULT_DB_NAME="dexter_db"
DEFAULT_DB_USER="dexter_user"
DEFAULT_ADMIN_DB="postgres"
DEFAULT_ADMIN_USER="postgres"
DEFAULT_PASSWORD="admin123"
HOST_PYTHON_BIN="${HOST_PYTHON_BIN:-}"
HBA_FILE=""
HBA_BACKUP=""
HBA_RESTORE_ON_EXIT=0

log() {
    printf '[install_postgre] %s\n' "$*"
}

fail() {
    printf '[install_postgre] ERROR: %s\n' "$*" >&2
    exit 1
}

cleanup() {
    local status=$?

    if [ "$status" -ne 0 ] && [ "$HBA_RESTORE_ON_EXIT" -eq 1 ] && [ -n "$HBA_FILE" ] && [ -f "$HBA_BACKUP" ]; then
        log "Restoring original pg_hba.conf after failure."
        sudo cp "$HBA_BACKUP" "$HBA_FILE" || true
        restart_postgres_service || true
    fi

    if [ -n "$HBA_BACKUP" ] && [ -f "$HBA_BACKUP" ]; then
        rm -f "$HBA_BACKUP"
    fi
}

trap cleanup EXIT

resolve_host_python() {
    if [ -n "$HOST_PYTHON_BIN" ]; then
        printf '%s' "$HOST_PYTHON_BIN"
        return 0
    fi

    if command -v python3 >/dev/null 2>&1; then
        command -v python3
        return 0
    fi

    if [ -x "$PROJECT_ROOT/env/bin/python" ]; then
        printf '%s' "$PROJECT_ROOT/env/bin/python"
        return 0
    fi

    if command -v python >/dev/null 2>&1; then
        command -v python
        return 0
    fi

    fail "Python was not found. Set HOST_PYTHON_BIN or install Python 3."
}

restart_postgres_service() {
    if command -v pg_lsclusters >/dev/null 2>&1 && command -v pg_ctlcluster >/dev/null 2>&1; then
        local clusters_found=0
        while read -r version cluster _rest; do
            [ -n "${version:-}" ] || continue
            clusters_found=1
            sudo pg_ctlcluster "$version" "$cluster" restart
        done < <(sudo pg_lsclusters --no-header 2>/dev/null || true)
        if [ "$clusters_found" -eq 1 ]; then
            return 0
        fi
    fi

    if command -v systemctl >/dev/null 2>&1; then
        sudo systemctl restart postgresql && return 0
    fi

    sudo service postgresql restart
}

ensure_postgres_running() {
    if command -v systemctl >/dev/null 2>&1; then
        sudo systemctl enable --now postgresql >/dev/null 2>&1 || true
    fi

    if command -v service >/dev/null 2>&1; then
        sudo service postgresql start >/dev/null 2>&1 || true
    fi

    if command -v pg_lsclusters >/dev/null 2>&1 && command -v pg_ctlcluster >/dev/null 2>&1; then
        while read -r version cluster _port status _rest; do
            [ -n "${version:-}" ] || continue
            if [ "${status:-}" != "online" ]; then
                sudo pg_ctlcluster "$version" "$cluster" start
            fi
        done < <(sudo pg_lsclusters --no-header 2>/dev/null || true)
    fi
}

rewrite_hba_auth() {
    local file="$1"
    local method="$2"
    local temp_file

    temp_file="$(mktemp)"
    sudo cp "$file" "$temp_file"

    "$HOST_PYTHON_BIN" - "$temp_file" "$method" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
method = sys.argv[2]
lines = path.read_text().splitlines()
rewritten = []

for line in lines:
    stripped = line.lstrip()
    if not stripped or stripped.startswith("#"):
        rewritten.append(line)
        continue

    body, marker, comment = line.partition("#")
    columns = body.split()
    if not columns:
        rewritten.append(line)
        continue

    record_type = columns[0].lower()
    if record_type not in {"local", "host", "hostssl", "hostnossl", "hostgssenc", "hostnogssenc"}:
        rewritten.append(line)
        continue

    auth_index = 3 if record_type == "local" else 4
    if len(columns) <= auth_index:
        rewritten.append(line)
        continue

    columns[auth_index] = method
    new_line = "\t".join(columns)
    if marker:
        new_line = f"{new_line}  #{comment}"
    rewritten.append(new_line)

path.write_text("\n".join(rewritten) + "\n")
PY

    sudo cp "$temp_file" "$file"
    rm -f "$temp_file"
}

escape_env_value() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

upsert_env() {
    local key="$1"
    local value="$2"
    local escaped_value
    local escaped_replacement

    mkdir -p "$(dirname "$ENV_FILE")"
    touch "$ENV_FILE"
    escaped_value="$(escape_env_value "$value")"
    escaped_replacement="$(printf '%s' "$escaped_value" | sed 's/[&|]/\\&/g')"

    if grep -qE "^${key}=" "$ENV_FILE"; then
        sed -i "s|^${key}=.*|${key}=\"${escaped_replacement}\"|" "$ENV_FILE"
    else
        printf '%s="%s"\n' "$key" "$escaped_value" >>"$ENV_FILE"
    fi
}

urlencode() {
    "$HOST_PYTHON_BIN" -c 'import sys; from urllib.parse import quote; print(quote(sys.argv[1], safe=""))' "$1"
}

set_role_passwords() {
    local encryption_sql=""

    case "$PG_PASSWORD_AUTH_METHOD" in
        scram-sha-256)
            encryption_sql="SET password_encryption = 'scram-sha-256';"
            ;;
        md5)
            encryption_sql="SET password_encryption = 'md5';"
            ;;
        password)
            encryption_sql=""
            ;;
        *)
            fail "Unsupported PG_PASSWORD_AUTH_METHOD: $PG_PASSWORD_AUTH_METHOD"
            ;;
    esac

    sudo -u postgres psql -v ON_ERROR_STOP=1 \
        --set=admin_user="$POSTGRES_ADMIN_USER" \
        --set=admin_password="$POSTGRES_ADMIN_PASSWORD" \
        --set=app_user="$DB_USER" \
        --set=app_password="$DB_PASSWORD" \
        postgres <<SQL
${encryption_sql}
SELECT format('ALTER USER %I WITH PASSWORD %L', :'admin_user', :'admin_password');
\gexec
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'app_user')
        THEN format('ALTER USER %I WITH PASSWORD %L', :'app_user', :'app_password')
    ELSE format('CREATE USER %I WITH PASSWORD %L', :'app_user', :'app_password')
END;
\gexec
SQL
}

detect_pg_hba() {
    local hba

    hba="$(sudo -u postgres psql -d postgres -tAc 'SHOW hba_file;' | tr -d '[:space:]')"
    [ -n "$hba" ] || fail "Unable to locate pg_hba.conf."
    [ -f "$hba" ] || fail "pg_hba.conf not found at $hba."
    printf '%s' "$hba"
}

run_database_bootstrap() {
    local python_bin="${PYTHON_BIN:-}"
    local -a env_args

    if [ -z "$python_bin" ]; then
        if [ -x "$PROJECT_ROOT/env/bin/python" ]; then
            python_bin="$PROJECT_ROOT/env/bin/python"
        else
            python_bin="$HOST_PYTHON_BIN"
        fi
    fi

    env_args=(
        "DB_HOST=$DB_HOST"
        "DB_PORT=$DB_PORT"
        "DB_NAME=$DB_NAME"
        "DB_USER=$DB_USER"
        "DB_PASSWORD=$DB_PASSWORD"
        "DATABASE_URL=$DATABASE_URL"
        "POSTGRES_ADMIN_HOST=$POSTGRES_ADMIN_HOST"
        "POSTGRES_ADMIN_PORT=$POSTGRES_ADMIN_PORT"
        "POSTGRES_ADMIN_DB=$POSTGRES_ADMIN_DB"
        "POSTGRES_ADMIN_USER=$POSTGRES_ADMIN_USER"
        "POSTGRES_ADMIN_PASSWORD=$POSTGRES_ADMIN_PASSWORD"
        "POSTGRES_ADMIN_DSN=$POSTGRES_ADMIN_DSN"
    )

    log "Running database.py with $python_bin"
    (
        cd "$PROJECT_ROOT"
        env "${env_args[@]}" "$python_bin" "$PROJECT_ROOT/database.py"
    )
}

[ -f "$PROJECT_ROOT/database.py" ] || fail "database.py was not found in $PROJECT_ROOT."
command -v sudo >/dev/null 2>&1 || fail "sudo is required."
[ "$PG_PASSWORD_AUTH_METHOD" = "scram-sha-256" ] || [ "$PG_PASSWORD_AUTH_METHOD" = "md5" ] || [ "$PG_PASSWORD_AUTH_METHOD" = "password" ] || fail "PG_PASSWORD_AUTH_METHOD must be one of: scram-sha-256, md5, password."
HOST_PYTHON_BIN="$(resolve_host_python)"

if [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
fi

DB_HOST="${DB_HOST:-$DEFAULT_DB_HOST}"
DB_PORT="${DB_PORT:-$DEFAULT_DB_PORT}"
DB_NAME="${DB_NAME:-$DEFAULT_DB_NAME}"
DB_USER="${DB_USER:-$DEFAULT_DB_USER}"
DB_PASSWORD="${DB_PASSWORD:-$DEFAULT_PASSWORD}"
POSTGRES_ADMIN_HOST="${POSTGRES_ADMIN_HOST:-$DB_HOST}"
POSTGRES_ADMIN_PORT="${POSTGRES_ADMIN_PORT:-$DB_PORT}"
POSTGRES_ADMIN_DB="${POSTGRES_ADMIN_DB:-$DEFAULT_ADMIN_DB}"
POSTGRES_ADMIN_USER="${POSTGRES_ADMIN_USER:-$DEFAULT_ADMIN_USER}"
POSTGRES_ADMIN_PASSWORD="${POSTGRES_ADMIN_PASSWORD:-$DB_PASSWORD}"

DATABASE_URL="postgres://$(urlencode "$DB_USER"):$(urlencode "$DB_PASSWORD")@${DB_HOST}:${DB_PORT}/${DB_NAME}"
POSTGRES_ADMIN_DSN="postgres://$(urlencode "$POSTGRES_ADMIN_USER"):$(urlencode "$POSTGRES_ADMIN_PASSWORD")@${POSTGRES_ADMIN_HOST}:${POSTGRES_ADMIN_PORT}/${POSTGRES_ADMIN_DB}"

log "Installing PostgreSQL packages."
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql

ensure_postgres_running

HBA_FILE="$(detect_pg_hba)"
HBA_BACKUP="$(mktemp)"
sudo cp "$HBA_FILE" "$HBA_BACKUP"
HBA_RESTORE_ON_EXIT=1

log "Switching pg_hba.conf auth methods to trust."
rewrite_hba_auth "$HBA_FILE" "trust"
restart_postgres_service

log "Setting PostgreSQL role passwords."
set_role_passwords

log "Switching pg_hba.conf auth methods to $PG_PASSWORD_AUTH_METHOD."
rewrite_hba_auth "$HBA_FILE" "$PG_PASSWORD_AUTH_METHOD"
restart_postgres_service
HBA_RESTORE_ON_EXIT=0

log "Writing database settings to $ENV_FILE."
upsert_env "DB_HOST" "$DB_HOST"
upsert_env "DB_PORT" "$DB_PORT"
upsert_env "DB_NAME" "$DB_NAME"
upsert_env "DB_USER" "$DB_USER"
upsert_env "DB_PASSWORD" "$DB_PASSWORD"
upsert_env "DATABASE_URL" "$DATABASE_URL"
upsert_env "POSTGRES_ADMIN_HOST" "$POSTGRES_ADMIN_HOST"
upsert_env "POSTGRES_ADMIN_PORT" "$POSTGRES_ADMIN_PORT"
upsert_env "POSTGRES_ADMIN_DB" "$POSTGRES_ADMIN_DB"
upsert_env "POSTGRES_ADMIN_USER" "$POSTGRES_ADMIN_USER"
upsert_env "POSTGRES_ADMIN_PASSWORD" "$POSTGRES_ADMIN_PASSWORD"
upsert_env "POSTGRES_ADMIN_DSN" "$POSTGRES_ADMIN_DSN"

run_database_bootstrap

log "PostgreSQL installation and Dexter database bootstrap completed."
