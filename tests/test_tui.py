import asyncio
import unittest
from unittest.mock import patch

from awss.app import S3Browser
from awss.s3 import BUCKET_ACCESS_GOOD, BucketInfo


class _StubService:
    profiles = [None]

    def sso_login_targets(self) -> list[str]:
        return []

    async def select_best_bucket_profiles(self, buckets, progress_callback=None):
        return buckets

    async def list_buckets_all(self, progress_callback=None):
        return [], []

    async def list_prefixes(self, *_args, **_kwargs):
        return []

    async def list_prefixes_and_objects(self, *_args, **_kwargs):
        return [], [], False

    async def get_object_head(self, *_args, **_kwargs):
        return b"", None, False

    async def get_object_range(self, *_args, **_kwargs):
        return b"", None, False


class _CachedStubService(_StubService):
    def __init__(self) -> None:
        self.list_calls = 0

    def load_bucket_cache(self, ignore_ttl: bool = False):
        return [
            BucketInfo(
                name="cached-bucket",
                profile=None,
                access=BUCKET_ACCESS_GOOD,
                is_empty=False,
            )
        ]

    async def list_buckets_all(self, progress_callback=None):
        self.list_calls += 1
        return [], []


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

    async def test_slash_focuses_path_input(self) -> None:
        app = S3Browser(profiles=["default"])
        app.service = _StubService()
        async with app.run_test() as pilot:
            app.set_focus(app.s3_tree)
            before = app.path_input.value
            await pilot.press("/")
            self.assertIs(app.focused, app.path_input)
            self.assertEqual(app.path_input.value, before)

    async def test_non_slash_key_does_not_focus_path_input(self) -> None:
        app = S3Browser(profiles=["default"])
        app.service = _StubService()
        async with app.run_test() as pilot:
            app.set_focus(app.s3_tree)
            await pilot.press("a")
            self.assertIs(app.focused, app.s3_tree)

    async def test_down_from_path_focuses_file_explorer(self) -> None:
        app = S3Browser(profiles=["default"])
        app.service = _StubService()
        async with app.run_test() as pilot:
            app.set_focus(app.path_input)
            await pilot.press("down")
            self.assertIs(app.focused, app.s3_table)

    async def test_preview_focus_toggles_preview_highlight(self) -> None:
        app = S3Browser(profiles=["default"])
        app.service = _StubService()
        async with app.run_test() as pilot:
            preview_container = app.query_one("#preview")
            self.assertFalse(preview_container.has_class("preview-focused"))
            app.set_focus(app.preview)
            await pilot.pause()
            self.assertTrue(preview_container.has_class("preview-focused"))
            app.set_focus(app.s3_tree)
            await pilot.pause()
            self.assertFalse(preview_container.has_class("preview-focused"))

    async def test_startup_uses_cached_buckets_without_live_listing(self) -> None:
        app = S3Browser(profiles=["default"])
        cached_service = _CachedStubService()
        app.service = cached_service
        async with app.run_test() as pilot:
            await pilot.pause()
            await asyncio.sleep(0.05)
            self.assertEqual(cached_service.list_calls, 0)


if __name__ == "__main__":
    unittest.main()
