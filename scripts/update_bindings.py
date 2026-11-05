"""Package a tested native SDK wheel as a standalone Dexter binding.

Maintainer workflow only. Normal users run python Dexter.py.
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
from pathlib import Path
import re
import zipfile


def package_wheel(source: Path, destination: Path, version: str):
    if not re.fullmatch(r'\d+(?:\.\d+)*(?:(?:a|b|rc)\d+)?', version):
        raise ValueError('Use a numeric Python package version, for example 0.1.0')
    stem = source.name.removesuffix('.whl').split('-')
    if len(stem) < 5:
        raise ValueError('Expected a Python wheel')
    tags = '-'.join(stem[-3:])
    distribution = f'dexter_market_sdk-{version}'
    target = destination / f'{distribution}-{tags}.whl'
    info = distribution + '.dist-info'
    entries = {}
    with zipfile.ZipFile(source) as archive:
        for original in archive.namelist():
            if original.endswith('/') or original.endswith('.dist-info/RECORD'):
                continue
            name = original
            if name.startswith(('mamba_sdk/', 'dexter_sdk/')):
                name = 'dexter_sdk/' + name.split('/', 1)[1]
            elif '.dist-info/' in name:
                name = info + '/' + name.split('.dist-info/', 1)[1]
            payload = archive.read(original)
            if name.endswith('.dist-info/METADATA'):
                metadata = payload.decode('utf-8')
                metadata = re.sub(r'^Name:.*$', 'Name: dexter-market-sdk', metadata, flags=re.M)
                metadata = re.sub(r'^Version:.*$', 'Version: '+version, metadata, flags=re.M)
                metadata = re.sub(r'^Summary:.*$', 'Summary: Dexter native market bindings', metadata, flags=re.M)
                payload = metadata.encode('utf-8')
            if name == 'dexter_sdk/__init__.py':
                payload = payload.replace(b"Mamba's Rust market modules", b"Dexter's native market bindings")
            entries[name] = payload
    if 'dexter_sdk/__init__.py' not in entries or not any('/_native.' in name for name in entries):
        raise ValueError('The wheel does not contain the expected market binding')
    record = io.StringIO(newline='')
    writer = csv.writer(record, lineterminator='\n')
    for name, payload in entries.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b'=').decode()
        writer.writerow([name, 'sha256='+digest, len(payload)])
    writer.writerow([info+'/RECORD', '', ''])
    entries[info+'/RECORD'] = record.getvalue().encode()
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in entries.items():
            archive.writestr(name, payload)
    return target


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--from-wheel', required=True, type=Path)
    parser.add_argument('--version', required=True)
    parser.add_argument('--source-commit', default='')
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    wheel = package_wheel(args.from_wheel, root/'bindings'/'wheels', args.version)
    manifest_path = root/'bindings'/'manifest.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {'schema':1,'wheels':[]}
    manifest.update(package='dexter-market-sdk', module='dexter_sdk', version=args.version)
    manifest['wheels'] = [item for item in manifest['wheels'] if item['filename'].split('-')[-1] != wheel.name.split('-')[-1]]
    manifest['wheels'].append({'filename':wheel.name,'sha256':hashlib.sha256(wheel.read_bytes()).hexdigest(),
                              'source_commit':args.source_commit})
    manifest_path.write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    print('Packaged '+wheel.name)


if __name__ == '__main__':
    main()
