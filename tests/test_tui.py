import unittest

from awss.app import S3Browser


class _StubService:
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


if __name__ == "__main__":
    unittest.main()
