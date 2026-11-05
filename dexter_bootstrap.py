"""Standard-library-only launcher for a standalone Dexter checkout."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import sysconfig
import venv

ROOT = Path(__file__).resolve().parent


def runtime_python(root=ROOT):
    return root/'.venv'/('Scripts/python.exe' if os.name=='nt' else 'bin/python')


def select_binding(root=ROOT, platform=None):
    manifest = json.loads((root/'bindings'/'manifest.json').read_text(encoding='utf-8'))
    platform = (platform or sysconfig.get_platform()).replace('-', '_').replace('.', '_')
    matches = [item for item in manifest['wheels'] if item['filename'].endswith('-'+platform+'.whl')]
    if len(matches) != 1:
        raise RuntimeError(f'No packaged market binding for {platform}. See bindings/README.md for supported builds.')
    record = matches[0]
    wheel = (root/'bindings'/'wheels'/record['filename']).resolve()
    if not wheel.is_relative_to((root/'bindings'/'wheels').resolve()):
        raise RuntimeError('Invalid binding manifest path')
    if hashlib.sha256(wheel.read_bytes()).hexdigest() != record['sha256']:
        raise RuntimeError('Market binding checksum mismatch. Restore the binding from the Dexter release.')
    return wheel


def ensure_env(root=ROOT):
    target = root/'.env'
    if not target.exists():
        # Exclusive creation preserves an environment another launch just wrote.
        try:
            with target.open('x',encoding='utf-8') as handle:
                handle.write((root/'.env.example').read_text(encoding='utf-8'))
        except FileExistsError:
            pass
    return target


def environment_stamp(root=ROOT):
    digest = hashlib.sha256()
    digest.update(sys.implementation.cache_tag.encode())
    for name in ('pyproject.toml','bindings/manifest.json'):
        digest.update((root/name).read_bytes())
    return digest.hexdigest()


def install_runtime(root=ROOT, *, force=False):
    if sys.version_info < (3,10):
        raise RuntimeError('Dexter requires Python 3.10 or newer.')
    python = runtime_python(root)
    stamp = environment_stamp(root)
    state = root/'.venv'/'dexter-runtime.json'
    wheel = select_binding(root)
    if not force and python.exists() and state.exists():
        try:
            if json.loads(state.read_text()).get('stamp') == stamp:
                probe = subprocess.run([str(python), '-c', 'import dexter_sdk; import curses; import solana'],
                    cwd=root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if probe.returncode == 0:
                    return python
        except (ValueError,OSError):
            pass
    log_dir = root/'.dexter'
    log_dir.mkdir(exist_ok=True)
    print('Dexter | Preparing your Python environment...',flush=True)
    if not python.exists():
        venv.EnvBuilder(with_pip=True).create(root/'.venv')
    with (log_dir/'setup.log').open('a',encoding='utf-8') as log:
        for label,command in (
            ('Installing app dependencies',[str(python),'-m','pip','install','--disable-pip-version-check','-e',str(root)]),
            ('Installing market bindings',[str(python),'-m','pip','install','--disable-pip-version-check','--force-reinstall',str(wheel)]),
        ):
            print('Dexter | '+label+'...',flush=True)
            result = subprocess.run(command,cwd=root,stdout=log,stderr=subprocess.STDOUT)
            if result.returncode:
                raise RuntimeError(f'{label} failed. Details: {log_dir / "setup.log"}')
    state.write_text(json.dumps({'stamp':stamp,'python':str(python)},indent=2)+'\n')
    ensure_env(root)
    print('Dexter | Ready.',flush=True)
    return python


def ensure_runtime():
    ensure_env()
    if os.getenv('DEXTER_SKIP_BOOTSTRAP') == '1':
        return
    auto_setup = os.getenv('DEXTER_AUTO_SETUP')
    if auto_setup is None:
        for line in (ROOT/'.env').read_text(encoding='utf-8').splitlines():
            key, separator, value = line.partition('=')
            if separator and key.strip() == 'DEXTER_AUTO_SETUP':
                auto_setup = value.split('#',1)[0].strip().strip('\"\'')
    if auto_setup is not None and auto_setup.strip().lower() in {'false','0','no','off'}:
        return
    try:
        python = install_runtime()
    except (OSError,RuntimeError,ValueError) as exc:
        print('Dexter setup: '+str(exc),file=sys.stderr)
        raise SystemExit(1) from None
    if Path(sys.executable).resolve() != python.resolve():
        try:
            result = subprocess.run([str(python),str(ROOT/'Dexter.py'),*sys.argv[1:]],cwd=ROOT)
            raise SystemExit(result.returncode)
        except KeyboardInterrupt:
            raise SystemExit(130) from None


def launch():
    ensure_runtime()
    from Dexter import main
    return main()


if __name__ == '__main__':
    install_runtime(force='--repair' in sys.argv)
