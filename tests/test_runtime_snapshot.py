from __future__ import annotations

from pathlib import Path
from unittest import TestCase, mock

import dexter_operator


def _windows_permission_error(winerror: int = 5) -> PermissionError:
    exc = PermissionError("Access is denied")
    exc.winerror = winerror
    return exc


class RuntimeSnapshotTests(TestCase):
    def setUp(self) -> None:
        dexter_operator._runtime_snapshot_warning_times.clear()

    def test_replace_with_retry_retries_transient_windows_lock(self) -> None:
        temp_path = Path("collector-runtime.json.tmp")
        target_path = Path("collector-runtime.json")

        with mock.patch.object(dexter_operator.os, "name", "nt"):
            with mock.patch("pathlib.Path.replace", side_effect=[_windows_permission_error(), None]) as replace_mock:
                with mock.patch("dexter_operator.time.sleep") as sleep_mock:
                    dexter_operator._replace_with_retry(temp_path, target_path)

        self.assertEqual(replace_mock.call_count, 2)
        sleep_mock.assert_called_once_with(0.05)

    def test_publish_runtime_snapshot_swallows_retryable_windows_lock(self) -> None:
        snapshot_path = Path("dev/state/collector-runtime.json")

        with mock.patch.object(dexter_operator.os, "name", "nt"):
            with mock.patch("dexter_operator._atomic_write_json", side_effect=_windows_permission_error()):
                with mock.patch.object(dexter_operator.logger, "warning") as warning_mock:
                    dexter_operator.publish_runtime_snapshot(snapshot_path, {"status": "streaming"})

        warning_mock.assert_called_once()

    def test_publish_runtime_snapshot_reraises_non_retryable_errors(self) -> None:
        snapshot_path = Path("dev/state/collector-runtime.json")

        with mock.patch.object(dexter_operator.os, "name", "nt"):
            with mock.patch("dexter_operator._atomic_write_json", side_effect=_windows_permission_error(87)):
                with self.assertRaises(PermissionError):
                    dexter_operator.publish_runtime_snapshot(snapshot_path, {"status": "streaming"})
