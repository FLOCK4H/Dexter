"""Maintainers: build the pinned upstream source, then package a Dexter wheel.

Normal launch never executes this file, fetches Rust source or reads Mamba config.
Requires Git, Rust and maturin in the current Python environment.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', default=None, help='Dexter binding version to package.')
    args = parser.parse_args(argv)
    source = json.loads((ROOT/'bindings'/'source.json').read_text())
    patch = ROOT/'bindings'/source['patch']
    if hashlib.sha256(patch.read_bytes()).hexdigest() != source['patch_sha256']:
        raise RuntimeError('Source patch checksum mismatch; update the provenance record first.')
    version = args.version or json.loads((ROOT/'bindings'/'manifest.json').read_text())['version']
    build_root = ROOT/'.dexter'/'binding-build'
    build_root.mkdir(parents=True, exist_ok=True)
    # Retain build evidence on error. No existing checkout or runtime environment is changed.
    checkout = Path(tempfile.mkdtemp(prefix='source-', dir=build_root))

    def run(*command):
        subprocess.run(list(map(str,command)),cwd=checkout,check=True)

    run('git','init','--quiet')
    run('git','remote','add','origin',source['repository'])
    run('git','fetch','--depth','1','origin',source['commit'])
    run('git','checkout','--detach','FETCH_HEAD')
    run('git','apply','--check',patch)
    run('git','apply',patch)
    run(sys.executable,'scripts/prepare_python_build.py')
    wheels=checkout/'target'/'wheels'
    run(sys.executable,'-m','maturin','build','--profile','dev','--locked',
        '--interpreter',sys.executable,'--out',wheels)
    candidates=list(wheels.glob('mamba_sdk-*.whl'))
    if len(candidates)!=1:
        raise RuntimeError('Expected one built SDK wheel.')
    run(sys.executable,ROOT/'scripts'/'update_bindings.py','--from-wheel',candidates[0],
        '--version',version,'--source-commit',source['commit'])
    print('Binding packaged. Run setup with --repair, then the tests before releasing.')
    print('Build evidence: '+str(checkout))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
