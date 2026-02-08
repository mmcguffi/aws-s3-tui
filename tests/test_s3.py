import asyncio
import tempfile
import unittest
from pathlib import Path

from awss.s3 import (
    BUCKET_ACCESS_GOOD,
    BUCKET_ACCESS_NO_DOWNLOAD,
    BUCKET_ACCESS_NO_VIEW,
    BucketInfo,
    S3Service,
)


class TestS3Service(unittest.TestCase):
    class _StubService(S3Service):
        def __init__(self, profiles, cache_path, access_by_profile) -> None:
            super().__init__(profiles=profiles, cache_path=cache_path)
            self._access_by_profile = access_by_profile
            self.calls: list[tuple[str, str | None]] = []

        def _probe_profile_access_for_bucket(self, bucket, profile) -> str:
            self.calls.append((bucket, profile))
            return self._access_by_profile.get(
                (bucket, profile),
                BUCKET_ACCESS_NO_VIEW,
            )

    def test_normalize_profiles(self) -> None:
        service = S3Service(profiles=["default", "dev", "dev"])
        self.assertEqual(service.profiles, [None, "dev"])

    def test_select_best_bucket_profiles_picks_most_permissive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            service = self._StubService(
                profiles=[None, "dev", "prod"],
                cache_path=cache_path,
                access_by_profile={
                    ("bucket-a", None): BUCKET_ACCESS_NO_VIEW,
                    ("bucket-a", "dev"): BUCKET_ACCESS_NO_DOWNLOAD,
                    ("bucket-a", "prod"): BUCKET_ACCESS_GOOD,
                },
            )
            buckets = [
                BucketInfo(name="bucket-a", profile=None),
                BucketInfo(name="bucket-a", profile="dev"),
            ]
            resolved = asyncio.run(service.select_best_bucket_profiles(buckets))
            self.assertEqual(
                {(bucket.name, bucket.profile, bucket.access) for bucket in resolved},
                {
                    ("bucket-a", "prod", BUCKET_ACCESS_GOOD),
                },
            )
            self.assertEqual(
                set(service.calls),
                {
                    ("bucket-a", None),
                    ("bucket-a", "dev"),
                    ("bucket-a", "prod"),
                },
            )

    def test_select_best_bucket_profiles_marks_no_download(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            service = self._StubService(
                profiles=[None, "dev", "prod"],
                cache_path=cache_path,
                access_by_profile={
                    ("bucket-a", None): BUCKET_ACCESS_NO_VIEW,
                    ("bucket-a", "dev"): BUCKET_ACCESS_NO_DOWNLOAD,
                    ("bucket-a", "prod"): BUCKET_ACCESS_NO_VIEW,
                },
            )
            buckets = [
                BucketInfo(name="bucket-a", profile=None),
                BucketInfo(name="bucket-a", profile="dev"),
                BucketInfo(name="bucket-a", profile="prod"),
            ]
            resolved = asyncio.run(service.select_best_bucket_profiles(buckets))
            self.assertEqual(
                {(bucket.name, bucket.profile, bucket.access) for bucket in resolved},
                {("bucket-a", "dev", BUCKET_ACCESS_NO_DOWNLOAD)},
            )

    def test_select_best_bucket_profiles_marks_no_view_when_all_fail(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            service = self._StubService(
                profiles=[None, "dev", "prod"],
                cache_path=cache_path,
                access_by_profile={
                    ("bucket-a", None): BUCKET_ACCESS_NO_VIEW,
                    ("bucket-a", "dev"): BUCKET_ACCESS_NO_VIEW,
                    ("bucket-a", "prod"): BUCKET_ACCESS_NO_VIEW,
                },
            )
            buckets = [
                BucketInfo(name="bucket-a", profile=None),
                BucketInfo(name="bucket-a", profile="dev"),
                BucketInfo(name="bucket-a", profile="prod"),
            ]
            resolved = asyncio.run(service.select_best_bucket_profiles(buckets))
            self.assertEqual(
                {(bucket.name, bucket.profile, bucket.access) for bucket in resolved},
                {("bucket-a", "dev", BUCKET_ACCESS_NO_VIEW)},
            )

    def test_bucket_cache_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            service = S3Service(
                profiles=[None, "dev"],
                cache_path=cache_path,
                cache_ttl_seconds=3600,
            )
            expected = [
                BucketInfo(name="alpha", profile=None, access=BUCKET_ACCESS_NO_VIEW),
                BucketInfo(name="beta", profile="dev", access=BUCKET_ACCESS_GOOD),
            ]
            self.assertTrue(service.save_bucket_cache(expected))
            self.assertEqual(service.load_bucket_cache(), expected)


if __name__ == "__main__":
    unittest.main()
