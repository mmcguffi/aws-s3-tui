import asyncio
import json
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

    def test_select_best_bucket_profiles_reports_progress(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            service = self._StubService(
                profiles=[None, "dev", "prod"],
                cache_path=cache_path,
                access_by_profile={
                    ("bucket-a", None): BUCKET_ACCESS_NO_VIEW,
                    ("bucket-a", "dev"): BUCKET_ACCESS_NO_DOWNLOAD,
                    ("bucket-a", "prod"): BUCKET_ACCESS_GOOD,
                    ("bucket-b", None): BUCKET_ACCESS_GOOD,
                    ("bucket-b", "dev"): BUCKET_ACCESS_GOOD,
                    ("bucket-b", "prod"): BUCKET_ACCESS_GOOD,
                },
            )
            buckets = [
                BucketInfo(name="bucket-a", profile=None),
                BucketInfo(name="bucket-b", profile="dev"),
            ]
            progress: list[tuple[int, int, str, str | None]] = []

            def on_progress(
                completed: int, total: int, bucket: str, profile: str | None
            ) -> None:
                progress.append((completed, total, bucket, profile))

            asyncio.run(
                service.select_best_bucket_profiles(
                    buckets, progress_callback=on_progress
                )
            )

            self.assertTrue(progress)
            self.assertEqual(progress[-1][0], progress[-1][1])
            self.assertEqual(progress[-1][1], 6)

    def test_list_buckets_all_reports_progress(self) -> None:
        class _ListStubService(S3Service):
            def __init__(self, profiles, cache_path) -> None:
                super().__init__(profiles=profiles, cache_path=cache_path)

            def _list_buckets(self, profile):  # type: ignore[override]
                if profile == "prod":
                    raise Exception("boom")
                if profile == "dev":
                    return ["bucket-dev"]
                return ["bucket-default"]

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            service = _ListStubService(
                profiles=[None, "dev", "prod"],
                cache_path=cache_path,
            )
            progress: list[tuple[int, int, str | None, bool]] = []

            def on_progress(
                completed: int,
                total: int,
                profile: str | None,
                error: Exception | None,
            ) -> None:
                progress.append((completed, total, profile, error is not None))

            buckets, errors = asyncio.run(
                service.list_buckets_all(progress_callback=on_progress)
            )

            self.assertEqual(len(progress), 3)
            self.assertEqual(progress[-1][0], 3)
            self.assertEqual(progress[-1][1], 3)
            self.assertEqual(len(buckets), 2)
            self.assertEqual(len(errors), 1)

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
                BucketInfo(
                    name="beta",
                    profile="dev",
                    access=BUCKET_ACCESS_GOOD,
                    is_empty=True,
                ),
            ]
            self.assertTrue(service.save_bucket_cache(expected))
            self.assertEqual(service.load_bucket_cache(), expected)

    def test_bucket_cache_ignore_ttl_uses_hash_matched_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            service = S3Service(
                profiles=[None, "dev"],
                cache_path=cache_path,
                cache_ttl_seconds=1,
            )
            expected = [
                BucketInfo(name="alpha", profile=None, access=BUCKET_ACCESS_NO_VIEW),
            ]
            self.assertTrue(service.save_bucket_cache(expected))
            data = json.loads(cache_path.read_text())
            data["saved_at"] = "2000-01-01T00:00:00+00:00"
            cache_path.write_text(json.dumps(data))
            self.assertEqual(service.load_bucket_cache(), [])
            self.assertEqual(service.load_bucket_cache(ignore_ttl=True), expected)

    def test_bucket_cache_invalidated_on_aws_config_hash_change(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            service = S3Service(
                profiles=[None, "dev"],
                cache_path=cache_path,
                cache_ttl_seconds=3600,
            )
            service._aws_config_hash = lambda: "hash-one"  # type: ignore[method-assign]
            expected = [
                BucketInfo(name="alpha", profile=None, access=BUCKET_ACCESS_NO_VIEW),
            ]
            self.assertTrue(service.save_bucket_cache(expected))
            service._aws_config_hash = lambda: "hash-two"  # type: ignore[method-assign]
            self.assertEqual(service.load_bucket_cache(), [])

    def test_aws_config_hash_changes_when_credentials_file_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            config_path = base / "config"
            credentials_path = base / "credentials"
            config_path.write_text("[default]\nregion = us-east-1\n")
            credentials_path.write_text(
                "[default]\naws_access_key_id = a\naws_secret_access_key = b\n"
            )
            service = S3Service(profiles=[None])
            service._aws_config_path = lambda: config_path  # type: ignore[method-assign]
            service._aws_credentials_path = (  # type: ignore[method-assign]
                lambda: credentials_path
            )

            first_hash = service._aws_config_hash()
            credentials_path.write_text(
                "[default]\naws_access_key_id = a\naws_secret_access_key = c\n"
            )
            second_hash = service._aws_config_hash()

            self.assertIsNotNone(first_hash)
            self.assertIsNotNone(second_hash)
            self.assertNotEqual(first_hash, second_hash)

    def test_bucket_filter_state_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            config_path = Path(temp_dir) / "config.json"
            service = S3Service(
                profiles=[None, "dev"],
                cache_path=cache_path,
                cache_ttl_seconds=3600,
            )
            service._config_path = config_path
            expected = {
                "hide_no_view": True,
                "hide_no_download": False,
                "hide_empty": True,
                "only_favorites": True,
            }
            self.assertTrue(service.save_bucket_filter_state(expected))
            self.assertEqual(service.load_bucket_filter_state(), expected)

    def test_favorite_buckets_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "bucket-cache.json"
            config_path = Path(temp_dir) / "config.json"
            service = S3Service(
                profiles=[None, "dev"],
                cache_path=cache_path,
                cache_ttl_seconds=3600,
            )
            service._config_path = config_path
            expected = {"beta", "alpha"}
            self.assertTrue(service.save_favorite_buckets(expected))
            self.assertEqual(service.load_favorite_buckets(), expected)

    def test_probe_profile_access_reraises_sso_expired(self) -> None:
        class _ExpiredClient:
            def list_objects_v2(self, **_kwargs):
                raise Exception(
                    "UnauthorizedSSOTokenError: The SSO session associated with "
                    "this profile has expired or is otherwise invalid."
                )

        service = S3Service(profiles=[None])
        service._clients[service._profile_key(None)] = _ExpiredClient()

        with self.assertRaises(Exception):
            service._probe_profile_access_for_bucket("bucket-a", None)

    def test_probe_profile_access_returns_no_view_for_non_sso_errors(self) -> None:
        class _DeniedClient:
            def list_objects_v2(self, **_kwargs):
                raise Exception("AccessDenied: forbidden")

        service = S3Service(profiles=[None])
        service._clients[service._profile_key(None)] = _DeniedClient()

        access = service._probe_profile_access_for_bucket("bucket-a", None)
        self.assertEqual(access, BUCKET_ACCESS_NO_VIEW)

    def test_is_bucket_empty_true_when_key_count_zero(self) -> None:
        class _EmptyClient:
            def list_objects_v2(self, **_kwargs):
                return {"KeyCount": 0, "Contents": []}

        service = S3Service(profiles=[None])
        service._clients[service._profile_key(None)] = _EmptyClient()

        self.assertTrue(service._is_bucket_empty(None, "bucket-a"))

    def test_is_bucket_empty_false_with_contents(self) -> None:
        class _NonEmptyClient:
            def list_objects_v2(self, **_kwargs):
                return {"KeyCount": 1, "Contents": [{"Key": "file.txt"}]}

        service = S3Service(profiles=[None])
        service._clients[service._profile_key(None)] = _NonEmptyClient()

        self.assertFalse(service._is_bucket_empty(None, "bucket-a"))


if __name__ == "__main__":
    unittest.main()
