from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest import TestCase, mock

import dexter_bootstrap as bootstrap
import dexter_config


class StandaloneLaunchTests(TestCase):
    def test_other_working_directory_cannot_select_another_projects_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            other=Path(directory)
            (other/'.env.example').touch()
            (other/'pyproject.toml').write_text('[project]\nname="other"\n')
            with mock.patch.dict('os.environ',{},clear=True), mock.patch('pathlib.Path.cwd',return_value=other):
                self.assertEqual(dexter_config._resolve_project_root(),dexter_config.CODE_ROOT)
                self.assertEqual(dexter_config._project_path('./dev/logs'),dexter_config.PROJECT_ROOT/'dev/logs')

    def binding_root(self, root):
        wheels=root/'bindings'/'wheels'
        wheels.mkdir(parents=True)
        wheel=wheels/'dexter_market_sdk-0.1.0-cp310-abi3-win_amd64.whl'
        wheel.write_bytes(b'test binding')
        (root/'bindings'/'manifest.json').write_text(json.dumps({'wheels':[
            {'filename':wheel.name,'sha256':hashlib.sha256(wheel.read_bytes()).hexdigest()}]}))
        return wheel

    def test_rejects_modified_binding_before_install(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            wheel=self.binding_root(root)
            self.assertEqual(bootstrap.select_binding(root,'win-amd64'),wheel)
            wheel.write_bytes(b'changed')
            with self.assertRaisesRegex(RuntimeError,'checksum'):
                bootstrap.select_binding(root,'win-amd64')

    def test_unsupported_platform_has_recovery_instruction(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            self.binding_root(root)
            with self.assertRaisesRegex(RuntimeError,'bindings/README.md'):
                bootstrap.select_binding(root,'linux-x86_64')

    def test_new_environment_comes_from_own_example_existing_secrets_survive(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'.env.example').write_text('PRIVATE_KEY=\nDEXTER_START_ACTION=menu\n')
            target=bootstrap.ensure_env(root)
            self.assertEqual(target.read_text(),(root/'.env.example').read_text())
            target.write_text('PRIVATE_KEY=existing-private-value\n')
            bootstrap.ensure_env(root)
            self.assertEqual(target.read_text(),'PRIVATE_KEY=existing-private-value\n')

    def test_mainnet_rpc_pool_and_private_key_are_local(self):
        with mock.patch.dict('os.environ',{'PRIVATE_KEY':'own-key',
                'HTTP_URLS':'https://one.example,https://two.example',
                'WS_URLS':'wss://one.example,wss://two.example',
                'MAMBA_PRIVATE_KEY':'must-not-use'},clear=True):
            with mock.patch.object(dexter_config,'load_dotenv'):
                dexter_config.load_config.cache_clear()
                config=dexter_config.load_config('simulate','mainnet')
                self.assertEqual(config.rpc.private_key,'own-key')
                self.assertEqual(config.rpc.http_url,'https://one.example')
                self.assertEqual(config.rpc.ws_url,'wss://one.example')
        dexter_config.load_config.cache_clear()

    def test_setup_menu_dispatches_the_setup_handler(self):
        import dexter_cli
        with mock.patch('dexter_setup.run_setup',return_value=0) as setup:
            self.assertEqual(dexter_cli.legacy_main(['setup']),0)
        setup.assert_called_once_with()

    def test_standalone_backup_location_is_valid_but_source_tree_is_not(self):
        from dataclasses import replace
        with mock.patch.dict('os.environ',{},clear=True), mock.patch.object(dexter_config,'load_dotenv'):
            dexter_config.load_config.cache_clear()
            config=dexter_config.load_config('read_only','mainnet')
            safe=replace(config,backup=replace(config.backup,directory=config.project_root/'.dexter'/'backups'))
            errors,_=dexter_config.validate_config(safe,'trader')
            self.assertFalse(any('DEXTER_BACKUP_DIR' in error for error in errors))
            unsafe=replace(config,backup=replace(config.backup,directory=config.project_root/'public-backups'))
            errors,_=dexter_config.validate_config(unsafe,'trader')
            self.assertTrue(any('DEXTER_BACKUP_DIR' in error for error in errors))
        dexter_config.load_config.cache_clear()

    def test_explicit_database_url_is_never_replaced_by_auto_setup(self):
        import dexter_setup
        with mock.patch('dexter_config.read_env_values',return_value={'DATABASE_URL':'postgresql://owner/db'}):
            with mock.patch('database_setup.main') as create:
                dexter_setup.prepare_database()
        create.assert_not_called()
