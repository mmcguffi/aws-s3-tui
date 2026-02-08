import asyncio
import tempfile
import unittest
from pathlib import Path

from awss.s3 import BucketInfo, S3Service


class TestS3Service(unittest.TestCase):
    class _StubService(S3Service):
        def __init__(self, profiles, cache_path, scores) -> None:
            super().__init__(profiles=profiles, cache_path=cache_path)
            self._scores = scores

        def _score_profile_for_bucket(self, bucket, profile) -> int:
            return self._scores.get((bucket, profile), 0)

    def test_normalize_profiles(self) -> None:
        service = S3Service(profiles=["default", "dev", "dev"])
        self.assertEqual(service.profiles, [None, "dev"])

    def test_select_best_bucket_profiles_prefers_non_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            service = self._StubService(
                profiles=[None, "dev", "prod"],
                cache_path=cache_path,
                scores={
                    ("bucket-a", "dev"): 1,
                    ("bucket-a", "prod"): 0,
                },
            )
            buckets = [
                BucketInfo(name="bucket-a", profile=None),
                BucketInfo(name="bucket-a", profile="dev"),
                BucketInfo(name="bucket-a", profile="prod"),
                BucketInfo(name="bucket-b", profile="prod"),
                BucketInfo(name="bucket-c", profile=None),
            ]
            resolved = asyncio.run(service.select_best_bucket_profiles(buckets))
            self.assertEqual(
                {(bucket.name, bucket.profile) for bucket in resolved},
                {
                    ("bucket-a", "dev"),
                    ("bucket-b", "prod"),
                    ("bucket-c", None),
                },
            )

    def test_select_best_bucket_profiles_prefers_cached_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            service = self._StubService(
                profiles=[None, "dev", "prod"],
                cache_path=cache_path,
                scores={
                    ("bucket-a", "dev"): 1,
                    ("bucket-a", "prod"): 1,
                },
            )
            service.save_bucket_cache([BucketInfo(name="bucket-a", profile="prod")])
            buckets = [
                BucketInfo(name="bucket-a", profile=None),
                BucketInfo(name="bucket-a", profile="dev"),
                BucketInfo(name="bucket-a", profile="prod"),
            ]
            resolved = asyncio.run(service.select_best_bucket_profiles(buckets))
            self.assertEqual(
                {(bucket.name, bucket.profile) for bucket in resolved},
                {("bucket-a", "prod")},
            )

    def test_select_best_bucket_profiles_ignores_cached_default_when_non_default_works(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            service = self._StubService(
                profiles=[None, "dev", "prod"],
                cache_path=cache_path,
                scores={
                    ("bucket-a", "dev"): 1,
                    ("bucket-a", "prod"): 0,
                },
            )
            service.save_bucket_cache([BucketInfo(name="bucket-a", profile=None)])
            buckets = [
                BucketInfo(name="bucket-a", profile=None),
                BucketInfo(name="bucket-a", profile="dev"),
                BucketInfo(name="bucket-a", profile="prod"),
            ]
            resolved = asyncio.run(service.select_best_bucket_profiles(buckets))
            self.assertEqual(
                {(bucket.name, bucket.profile) for bucket in resolved},
                {("bucket-a", "dev")},
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
                BucketInfo(name="alpha", profile=None),
                BucketInfo(name="beta", profile="dev"),
            ]
            self.assertTrue(service.save_bucket_cache(expected))
            self.assertEqual(service.load_bucket_cache(), expected)


if __name__ == "__main__":
    unittest.main()
