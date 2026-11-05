"""Readiness and explicit local setup for Dexter's launch screen."""
from __future__ import annotations

import importlib.metadata
import json
import os
from pathlib import Path
import socket


def diagnostics():
    from dexter_config import load_config, resolve_trade_execution_mode
    from dexter_bootstrap import ROOT, select_binding
    import dexter_sdk
    config = load_config()
    wheel = select_binding()
    return {
        'product':'Dexter','standalone':True,'project_root':str(ROOT),
        'sdk_version':importlib.metadata.version('dexter-market-sdk'),
        'sdk_wheel':wheel.name,'sdk_markets':len(dexter_sdk.markets()),
        'environment_file':str(ROOT/'.env'),'wallet_configured':bool(config.rpc.private_key),
        'network':config.runtime.network,'mode':config.runtime.mode,
        'execution':resolve_trade_execution_mode(config),'requires_mamba_checkout':False,
    }


def prepare_database():
    from dexter_config import load_config, read_env_values
    from dexter_local_postgres import ensure_local_postgres_running
    values = read_env_values()
    managed = values.get('DEXTER_LOCAL_POSTGRES_MANAGED','').lower() in {'true','1','yes'}
    if managed:
        ensure_local_postgres_running(load_config())
        return
    if values.get('DATABASE_URL') or (values.get('DB_PASSWORD') and 'replace-me' not in values['DB_PASSWORD']):
        return  # An explicitly configured external/local database belongs to its owner.
    if os.name != 'nt':
        raise RuntimeError('Configure DATABASE_URL for your PostgreSQL instance, then choose Start Dexter.')
    import database_setup
    port = 55432
    while port < 55450:
        with socket.socket() as probe:
            if probe.connect_ex(('127.0.0.1',port)) != 0:
                break
        port += 1
    args = ['--skip-install','--db-port',str(port)]
    result = database_setup.main(args)
    if result:
        raise RuntimeError('Database setup needs PostgreSQL 17. Use Settings > Local database to install or repair it.')
    load_config.cache_clear()


def prepare_launch():
    from dexter_config import load_env_file
    load_env_file()
    if os.getenv('DEXTER_AUTO_DATABASE','true').strip().lower() in {'true','1','yes'}:
        prepare_database()


def run_setup():
    from dexter_config import load_env_file
    load_env_file()
    try:
        prepare_launch()
    except (OSError, RuntimeError) as exc:
        print('Setup needs attention: '+str(exc))
        return 1
    result = diagnostics()
    print('Checking Dexter: '+result['mode']+' on '+result['network']+'.')
    import argparse
    import asyncio
    from dexter_cli import run_doctor
    return asyncio.run(run_doctor(argparse.Namespace(mode=result['mode'],network=result['network'],component='trader')))


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description='Prepare or inspect this standalone Dexter checkout.')
    parser.add_argument('--diagnostics',action='store_true')
    args = parser.parse_args(argv)
    if args.diagnostics:
        print(json.dumps(diagnostics(),indent=2))
        return 0
    return run_setup()


if __name__=='__main__':
    raise SystemExit(main())
