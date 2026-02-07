import asyncio
import unittest
from unittest.mock import patch

from awss.app import S3Browser


class _StubService:
    def sso_login_targets(self) -> list[str]:
        return []

    async def select_best_bucket_profiles(self, buckets):
        return buckets

    async def list_buckets_all(self):
        return [], []

    async def list_prefixes(self, *_args, **_kwargs):
        return []

    async def list_prefixes_and_objects(self, *_args, **_kwargs):
        return [], [], False

    async def get_object_head(self, *_args, **_kwargs):
        return b"", None, False

    async def get_object_range(self, *_args, **_kwargs):
        return b"", None, False


class TestTuiMount(unittest.IsolatedAsyncioTestCase):
    async def test_app_mounts_headless(self) -> None:
        app = S3Browser(profiles=["default"])
        app.service = _StubService()
        async with app.run_test() as pilot:
            await pilot.pause()

    async def test_double_escape_quits(self) -> None:
        app = S3Browser(profiles=["default"])
        app.service = _StubService()
        with patch.object(app, "exit") as exit_mock:
            async with app.run_test() as pilot:
                await pilot.press("escape")
                exit_mock.assert_not_called()
                await pilot.press("escape")
                exit_mock.assert_called_once()

    async def test_escape_quit_window_expires(self) -> None:
        app = S3Browser(profiles=["default"])
        app.service = _StubService()
        app._quit_escape_deadline = 0.0
        with patch.object(app, "exit") as exit_mock:
            async with app.run_test() as pilot:
                await pilot.press("escape")
                await asyncio.sleep(1.1)
                await pilot.press("escape")
                exit_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
