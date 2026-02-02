import asyncio
import unittest

from awss.s3 import BucketInfo, S3Service


class TestS3Service(unittest.TestCase):
    def test_normalize_profiles(self) -> None:
        service = S3Service(profiles=["default", "dev", "dev"])
        self.assertEqual(service.profiles, [None, "dev"])

    def test_select_best_bucket_profiles(self) -> None:
        class _StubService(S3Service):
            def __init__(self, profiles, scores) -> None:
                super().__init__(profiles=profiles)
                self._scores = scores

            def _score_profile_for_bucket(self, bucket, profile) -> int:
                return self._scores.get((bucket, profile), 0)

        scores = {
            ("bucket-a", None): 1,
            ("bucket-a", "dev"): 3,
            ("bucket-a", "prod"): 2,
            ("bucket-c", None): 2,
            ("bucket-c", "dev"): 2,
        }
        service = _StubService(profiles=[None, "dev", "prod"], scores=scores)
        buckets = [
            BucketInfo(name="bucket-a", profile=None),
            BucketInfo(name="bucket-a", profile="dev"),
            BucketInfo(name="bucket-a", profile="prod"),
            BucketInfo(name="bucket-b", profile="prod"),
            BucketInfo(name="bucket-c", profile=None),
            BucketInfo(name="bucket-c", profile="dev"),
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


if __name__ == "__main__":
    unittest.main()
