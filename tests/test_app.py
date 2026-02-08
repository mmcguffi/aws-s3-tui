import asyncio
import argparse
import unittest
from unittest.mock import AsyncMock

from awss.app import (
    NodeInfo,
    RowInfo,
    S3Browser,
    _parse_profiles,
    display_segment,
    format_size,
    format_time,
)
from awss.s3 import (
    BUCKET_ACCESS_GOOD,
    BUCKET_ACCESS_NO_DOWNLOAD,
    BUCKET_ACCESS_NO_VIEW,
    BucketInfo,
)


class _DummyNode:
    def __init__(self, data) -> None:
        self.data = data
        self.label = None

    def set_label(self, value) -> None:
        self.label = value


class TestAppHelpers(unittest.TestCase):
    def test_format_size(self) -> None:
        self.assertEqual(format_size(0), "0 B")
        self.assertEqual(format_size(512), "512 B")
        self.assertEqual(format_size(1024), "1.0 KB")
        self.assertEqual(format_size(1536), "1.5 KB")

    def test_format_time(self) -> None:
        self.assertEqual(format_time(None), "")

    def test_display_segment(self) -> None:
        self.assertEqual(display_segment("foo/bar/", "foo/"), "bar")
        self.assertEqual(display_segment("foo/", ""), "foo")
        self.assertEqual(display_segment("foo/bar/baz", "foo/bar/"), "baz")

    def test_parse_profiles(self) -> None:
        args = argparse.Namespace(profiles="dev, prod", profile=None)
        self.assertEqual(_parse_profiles(args), ["dev", "prod"])

        args = argparse.Namespace(profiles=None, profile=["stage", "prod"])
        self.assertEqual(_parse_profiles(args), ["stage", "prod"])

        args = argparse.Namespace(profiles=None, profile=None)
        self.assertIsNone(_parse_profiles(args))

    def test_parent_prefix(self) -> None:
        app = S3Browser(profiles=["default"])
        self.assertEqual(app._parent_prefix("foo/bar/"), "foo/")
        self.assertEqual(app._parent_prefix("foo/"), "")
        self.assertEqual(app._parent_prefix(""), "")

    def test_parse_s3_path(self) -> None:
        app = S3Browser(profiles=["default"])
        self.assertEqual(app._parse_s3_path("s3://my-bucket"), ("my-bucket", ""))
        self.assertEqual(app._parse_s3_path("s3://my-bucket/"), ("my-bucket", ""))
        self.assertEqual(
            app._parse_s3_path("s3://my-bucket/a/b/"), ("my-bucket", "a/b/")
        )
        self.assertEqual(
            app._parse_s3_path("s3://my-bucket/a/b.txt"), ("my-bucket", "a/")
        )
        self.assertEqual(app._parse_s3_path("my-bucket/a/b.txt"), ("my-bucket", "a/"))

    def test_profile_for_bucket(self) -> None:
        app = S3Browser(profiles=["default"])
        app.buckets = []
        self.assertIsNone(app._profile_for_bucket("missing"))
        app.buckets = []
        app.bucket_nodes[("dev", "bucket-a")] = object()
        self.assertIsNone(app._profile_for_bucket("missing"))
        self.assertEqual(app._profile_for_bucket("bucket-a"), "dev")
        app.buckets = [BucketInfo(name="bucket-a", profile="prod")]
        self.assertEqual(app._profile_for_bucket("bucket-a"), "prod")

    def test_resolve_input_path(self) -> None:
        app = S3Browser(profiles=["default"])
        app._canonical_path = "s3://"
        self.assertEqual(app._resolve_input_path("bucket"), "s3://bucket")
        self.assertEqual(
            app._resolve_input_path("s3://bucket/prefix/"), "s3://bucket/prefix/"
        )
        app._canonical_path = "s3://bucket/prefix/"
        self.assertEqual(app._resolve_input_path("child/"), "s3://child/")
        self.assertEqual(app._resolve_input_path("/child/"), "s3://child/")

    def test_derive_filter(self) -> None:
        app = S3Browser(profiles=["default"])
        app._content_rows = [
            ("alpha", "BUCKET", "", "", RowInfo(kind="bucket", bucket="alpha")),
            ("beta", "BUCKET", "", "", RowInfo(kind="bucket", bucket="beta")),
        ]
        app._canonical_path = "s3://"
        self.assertEqual(app._derive_filter("s3://a"), "a")
        app.current_context = NodeInfo(profile=None, bucket="my-bucket", prefix="a/b/")
        app._canonical_path = "s3://my-bucket/a/b/"
        self.assertEqual(app._derive_filter("s3://my-bucket/a/b/fo"), "fo")
        self.assertEqual(app._derive_filter("my-bucket/a/b/fo"), "fo")

    def test_profile_candidates_for_bucket_prefers_non_default(self) -> None:
        app = S3Browser(profiles=["default", "dev", "prod"])
        app.bucket_profile_candidates = {"bucket-a": [None, "prod", "dev"]}
        self.assertEqual(
            app._profile_candidates_for_bucket("bucket-a"),
            ["dev", "prod", None],
        )

    def test_switch_bucket_profile_updates_structures(self) -> None:
        app = S3Browser(profiles=["default", "dev"])
        app.buckets = [BucketInfo(name="bucket-a", profile=None)]
        bucket_node = _DummyNode(NodeInfo(profile=None, bucket="bucket-a", prefix=""))
        prefix_node = _DummyNode(NodeInfo(profile=None, bucket="bucket-a", prefix="foo/"))
        app.bucket_nodes[(None, "bucket-a")] = bucket_node
        app.prefix_nodes[(None, "bucket-a", "foo/")] = prefix_node
        app.bucket_profile_candidates = {"bucket-a": [None, "dev"]}

        app._switch_bucket_profile("bucket-a", None, "dev", prefix_node)

        self.assertNotIn((None, "bucket-a"), app.bucket_nodes)
        self.assertIn(("dev", "bucket-a"), app.bucket_nodes)
        self.assertEqual(app.buckets[0], BucketInfo(name="bucket-a", profile="dev"))
        self.assertNotIn((None, "bucket-a", "foo/"), app.prefix_nodes)
        self.assertIn(("dev", "bucket-a", "foo/"), app.prefix_nodes)
        self.assertEqual(prefix_node.data.profile, "dev")

    def test_bucket_name_style(self) -> None:
        app = S3Browser(profiles=["default"])
        self.assertEqual(app._bucket_name_style(BUCKET_ACCESS_NO_VIEW), "bold red")
        self.assertEqual(app._bucket_name_style(BUCKET_ACCESS_NO_DOWNLOAD), "bold #ff8c00")
        self.assertEqual(app._bucket_name_style(BUCKET_ACCESS_GOOD), "bold #2f80ed")

    def test_visible_buckets_respects_filter_state(self) -> None:
        app = S3Browser(profiles=["default"])
        app.buckets = [
            BucketInfo(name="red", profile="dev", access=BUCKET_ACCESS_NO_VIEW),
            BucketInfo(
                name="orange",
                profile="dev",
                access=BUCKET_ACCESS_NO_DOWNLOAD,
            ),
            BucketInfo(
                name="empty",
                profile="dev",
                access=BUCKET_ACCESS_GOOD,
                is_empty=True,
            ),
            BucketInfo(name="good", profile="dev", access=BUCKET_ACCESS_GOOD),
        ]
        app._hide_no_view_buckets = True
        app._hide_no_download_buckets = True
        app._hide_empty_buckets = True
        self.assertEqual([bucket.name for bucket in app._visible_buckets()], ["good"])

    def test_visible_buckets_respects_only_favorites_filter(self) -> None:
        app = S3Browser(profiles=["default"])
        app.buckets = [
            BucketInfo(name="alpha", profile="dev", access=BUCKET_ACCESS_GOOD),
            BucketInfo(name="beta", profile="dev", access=BUCKET_ACCESS_GOOD),
        ]
        app._favorite_buckets = {"beta"}
        app._show_only_favorite_buckets = True
        self.assertEqual([bucket.name for bucket in app._visible_buckets()], ["beta"])

    def test_bucket_filter_state_payload(self) -> None:
        app = S3Browser(profiles=["default"])
        app._hide_no_view_buckets = True
        app._hide_no_download_buckets = False
        app._hide_empty_buckets = True
        app._show_only_favorite_buckets = True
        self.assertEqual(
            app._bucket_filter_state_payload(),
            {
                "hide_no_view": True,
                "hide_no_download": False,
                "hide_empty": True,
                "only_favorites": True,
            },
        )

    def test_reuse_cached_bucket_resolution_when_bucket_set_and_profile_match(self) -> None:
        app = S3Browser(profiles=["default", "dev", "prod"])
        listed = [
            BucketInfo(name="bucket-a", profile="dev"),
            BucketInfo(name="bucket-a", profile="prod"),
            BucketInfo(name="bucket-b", profile=None),
        ]
        cached = [
            BucketInfo(
                name="bucket-a",
                profile="prod",
                access=BUCKET_ACCESS_GOOD,
                is_empty=False,
            ),
            BucketInfo(
                name="bucket-b",
                profile=None,
                access=BUCKET_ACCESS_NO_DOWNLOAD,
                is_empty=True,
            ),
        ]
        resolved = app._reuse_cached_bucket_resolution(listed, cached)
        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(
            sorted((item.name, item.profile, item.access, item.is_empty) for item in resolved),
            [
                ("bucket-a", "prod", BUCKET_ACCESS_GOOD, False),
                ("bucket-b", None, BUCKET_ACCESS_NO_DOWNLOAD, True),
            ],
        )

    def test_reuse_cached_bucket_resolution_returns_none_on_profile_mismatch(self) -> None:
        app = S3Browser(profiles=["default", "dev"])
        listed = [BucketInfo(name="bucket-a", profile="dev")]
        cached = [BucketInfo(name="bucket-a", profile="prod", access=BUCKET_ACCESS_GOOD)]
        self.assertIsNone(app._reuse_cached_bucket_resolution(listed, cached))

    def test_call_with_sso_retry_reauthenticates_and_retries(self) -> None:
        app = S3Browser(profiles=["default"])
        app.notify = lambda *args, **kwargs: None  # type: ignore[assignment]
        app._run_sso_login = AsyncMock(return_value=True)
        calls = {"count": 0}

        async def operation(*_args, **_kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise Exception(
                    "UnauthorizedSSOTokenError: The SSO session associated with "
                    "this profile has expired or is otherwise invalid."
                )
            return "ok"

        result = asyncio.run(app._call_with_sso_retry("dev", operation))

        self.assertEqual(result, "ok")
        self.assertEqual(calls["count"], 2)
        app._run_sso_login.assert_awaited_once_with("dev")

    def test_reauth_sso_profile_deduplicates_inflight_login(self) -> None:
        app = S3Browser(profiles=["default"])
        app.notify = lambda *args, **kwargs: None  # type: ignore[assignment]

        async def fake_login(_profile: str) -> bool:
            await asyncio.sleep(0.01)
            return True

        app._run_sso_login = AsyncMock(side_effect=fake_login)

        async def run_two():
            return await asyncio.gather(
                app._reauth_sso_profile("dev"),
                app._reauth_sso_profile("dev"),
            )

        results = asyncio.run(run_two())

        self.assertEqual(results, [True, True])
        app._run_sso_login.assert_awaited_once_with("dev")


if __name__ == "__main__":
    unittest.main()
