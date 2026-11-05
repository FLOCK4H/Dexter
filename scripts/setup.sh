#!/usr/bin/env sh
set -eu
dexter_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$dexter_root"
python3 dexter_bootstrap.py "$@"
.venv/bin/python Dexter.py setup
