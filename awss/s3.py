from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import boto3
import botocore.session
from botocore.exceptions import ConfigNotFound


@dataclass(frozen=True)
class BucketInfo:
    name: str
    profile: Optional[str]


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    last_modified: Optional[datetime]
    storage_class: Optional[str]


class S3Service:
    def __init__(
        self, profiles: Optional[list[str]] = None, region: Optional[str] = None
    ) -> None:
        self.profiles = self._normalize_profiles(profiles)
        self._region = region
        self._clients: dict[str, object] = {}

    def _normalize_profiles(
        self, profiles: Optional[Iterable[str]]
    ) -> list[Optional[str]]:
        if profiles:
            raw_profiles = list(profiles)
        else:
            session = boto3.session.Session()
            raw_profiles = session.available_profiles

        normalized: list[Optional[str]] = []
        for profile in raw_profiles:
            if profile == "default":
                profile = None
            if profile not in normalized:
                normalized.append(profile)

        if not normalized:
            normalized = [None]
        return normalized

    def _profile_key(self, profile: Optional[str]) -> str:
        return profile or "__default__"

    def _profile_label(self, profile: Optional[str]) -> str:
        return profile or "default"

    def _client(self, profile: Optional[str]):
        key = self._profile_key(profile)
        if key in self._clients:
            return self._clients[key]
        if profile is None:
            session = boto3.session.Session()
        else:
            session = boto3.session.Session(profile_name=profile)
        if self._region:
            client = session.client("s3", region_name=self._region)
        else:
            client = session.client("s3")
        self._clients[key] = client
        return client

    def sso_login_targets(self) -> list[str]:
        start_urls = self._load_sso_profile_start_urls()
        if not start_urls:
            return []
        expirations = self._load_sso_token_expirations()
        now = datetime.now(timezone.utc)
        buffer = timedelta(minutes=5)
        targets: list[str] = []
        seen: set[str] = set()
        for profile in self.profiles:
            profile_name = self._profile_label(profile)
            start_url = start_urls.get(profile_name)
            if not start_url:
                continue
            expires_at = expirations.get(start_url)
            if expires_at and expires_at > now + buffer:
                continue
            if start_url in seen:
                continue
            seen.add(start_url)
            targets.append(profile_name)
        return targets

    async def select_best_bucket_profiles(
        self, buckets: list[BucketInfo]
    ) -> list[BucketInfo]:
        by_name: dict[str, list[Optional[str]]] = {}
        for bucket in buckets:
            by_name.setdefault(bucket.name, []).append(bucket.profile)
        if all(len(profiles) == 1 for profiles in by_name.values()):
            return buckets

        tasks: list[asyncio.Future] = []
        task_keys: list[tuple[str, Optional[str]]] = []
        for name, profiles in by_name.items():
            if len(profiles) < 2:
                continue
            for profile in profiles:
                task_keys.append((name, profile))
                tasks.append(
                    asyncio.to_thread(self._score_profile_for_bucket, name, profile)
                )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        scores: dict[tuple[str, Optional[str]], int] = {}
        for key, result in zip(task_keys, results):
            if isinstance(result, Exception):
                scores[key] = 0
            else:
                scores[key] = int(result)

        profile_rank = {profile: index for index, profile in enumerate(self.profiles)}
        resolved: list[BucketInfo] = []
        for name, profiles in by_name.items():
            if len(profiles) == 1:
                resolved.append(BucketInfo(name=name, profile=profiles[0]))
                continue

            def profile_key(profile: Optional[str]) -> tuple[int, int]:
                score = scores.get((name, profile), 0)
                rank = profile_rank.get(profile, len(profile_rank))
                return (score, -rank)

            best_profile = max(profiles, key=profile_key)
            resolved.append(BucketInfo(name=name, profile=best_profile))
        return resolved

    def _score_profile_for_bucket(self, bucket: str, profile: Optional[str]) -> int:
        client = self._client(profile)
        try:
            response = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        except Exception:
            return 0
        score = 2
        contents = response.get("Contents", [])
        if contents:
            key = contents[0].get("Key")
            if key:
                try:
                    client.head_object(Bucket=bucket, Key=key)
                    score += 1
                except Exception:
                    pass
        return score

    def _load_full_config(self) -> dict:
        session = botocore.session.get_session()
        try:
            return session.full_config
        except ConfigNotFound:
            return {}
        except Exception:
            return {}

    def _load_sso_profile_start_urls(self) -> dict[str, str]:
        full_config = self._load_full_config()
        profiles = full_config.get("profiles", {})
        sso_sessions = full_config.get("sso_sessions", {})
        start_urls: dict[str, str] = {}
        for profile_name, profile_config in profiles.items():
            if not isinstance(profile_config, dict):
                continue
            start_url = profile_config.get("sso_start_url")
            session_name = profile_config.get("sso_session")
            if not start_url and session_name:
                session_config = sso_sessions.get(session_name, {})
                if isinstance(session_config, dict):
                    start_url = session_config.get("sso_start_url")
            if start_url:
                start_urls[profile_name] = start_url
        return start_urls

    def _load_sso_token_expirations(self) -> dict[str, datetime]:
        cache_dir = Path.home() / ".aws" / "sso" / "cache"
        if not cache_dir.exists():
            return {}
        expirations: dict[str, datetime] = {}
        for path in cache_dir.glob("*.json"):
            try:
                data = json.loads(path.read_text())
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            start_url = data.get("startUrl") or data.get("start_url")
            expires_at_raw = data.get("expiresAt") or data.get("expires_at")
            if not start_url or not expires_at_raw:
                continue
            expires_at = self._parse_sso_expires_at(expires_at_raw)
            if not expires_at:
                continue
            current = expirations.get(start_url)
            if current is None or expires_at > current:
                expirations[start_url] = expires_at
        return expirations

    def _parse_sso_expires_at(self, value: str) -> Optional[datetime]:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if text.endswith("UTC"):
            text = f"{text[:-3]}+00:00"
        elif text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    async def list_buckets_all(
        self,
    ) -> tuple[list[BucketInfo], list[tuple[Optional[str], Exception]]]:
        tasks = [
            asyncio.to_thread(self._list_buckets, profile) for profile in self.profiles
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        buckets: list[BucketInfo] = []
        errors: list[tuple[Optional[str], Exception]] = []
        for profile, result in zip(self.profiles, results):
            if isinstance(result, Exception):
                errors.append((profile, result))
                continue
            for name in result:
                buckets.append(BucketInfo(name=name, profile=profile))
        return buckets, errors

    def _list_buckets(self, profile: Optional[str]) -> list[str]:
        client = self._client(profile)
        response = client.list_buckets()
        return [bucket["Name"] for bucket in response.get("Buckets", [])]

    async def list_prefixes(
        self, profile: Optional[str], bucket: str, prefix: str
    ) -> list[str]:
        return await asyncio.to_thread(self._list_prefixes, profile, bucket, prefix)

    def _list_prefixes(
        self, profile: Optional[str], bucket: str, prefix: str
    ) -> list[str]:
        client = self._client(profile)
        prefixes: list[str] = []
        continuation: Optional[str] = None
        while True:
            kwargs = {
                "Bucket": bucket,
                "Delimiter": "/",
                "Prefix": prefix,
                "MaxKeys": 1000,
            }
            if continuation:
                kwargs["ContinuationToken"] = continuation
            response = client.list_objects_v2(**kwargs)
            for entry in response.get("CommonPrefixes", []):
                value = entry.get("Prefix")
                if value:
                    prefixes.append(value)
            if response.get("IsTruncated"):
                continuation = response.get("NextContinuationToken")
            else:
                break
        return prefixes

    async def list_prefixes_and_objects(
        self, profile: Optional[str], bucket: str, prefix: str
    ) -> tuple[list[str], list[ObjectInfo], bool]:
        return await asyncio.to_thread(
            self._list_prefixes_and_objects, profile, bucket, prefix
        )

    def _list_prefixes_and_objects(
        self, profile: Optional[str], bucket: str, prefix: str
    ) -> tuple[list[str], list[ObjectInfo], bool]:
        client = self._client(profile)
        prefixes: list[str] = []
        objects: list[ObjectInfo] = []
        has_any = False
        continuation: Optional[str] = None
        while True:
            kwargs = {
                "Bucket": bucket,
                "Delimiter": "/",
                "Prefix": prefix,
                "MaxKeys": 1000,
            }
            if continuation:
                kwargs["ContinuationToken"] = continuation
            response = client.list_objects_v2(**kwargs)
            for entry in response.get("CommonPrefixes", []):
                value = entry.get("Prefix")
                if value:
                    has_any = True
                    prefixes.append(value)
            contents = response.get("Contents", [])
            if contents:
                has_any = True
            for entry in contents:
                key = entry.get("Key")
                if not key:
                    continue
                if key.endswith("/"):
                    continue
                if prefix and key == prefix:
                    continue
                objects.append(
                    ObjectInfo(
                        key=key,
                        size=int(entry.get("Size", 0)),
                        last_modified=entry.get("LastModified"),
                        storage_class=entry.get("StorageClass"),
                    )
                )
            if response.get("IsTruncated"):
                continuation = response.get("NextContinuationToken")
            else:
                break
        return prefixes, objects, has_any

    async def get_object_head(
        self, profile: Optional[str], bucket: str, key: str, max_bytes: int = 4096
    ) -> tuple[bytes, Optional[int], bool]:
        return await asyncio.to_thread(
            self._get_object_head, profile, bucket, key, max_bytes, 0
        )

    def _get_object_head(
        self, profile: Optional[str], bucket: str, key: str, max_bytes: int, start: int
    ) -> tuple[bytes, Optional[int], bool]:
        client = self._client(profile)
        end = start + max_bytes - 1
        response = client.get_object(
            Bucket=bucket,
            Key=key,
            Range=f"bytes={start}-{end}",
        )
        body = response.get("Body")
        if body is None:
            return b"", None, False
        try:
            data = body.read(max_bytes)
        finally:
            try:
                body.close()
            except Exception:
                pass
        total_size: Optional[int] = None
        truncated = False
        content_range = response.get("ContentRange")
        if content_range:
            # Expected: "bytes start-end/total"
            parts = content_range.split("/")
            if len(parts) == 2 and parts[1].isdigit():
                total_size = int(parts[1])
                truncated = (start + len(data)) < total_size
        else:
            content_length = response.get("ContentLength")
            if isinstance(content_length, int):
                total_size = content_length
                truncated = len(data) < total_size
        return data, total_size, truncated

    async def get_object_range(
        self,
        profile: Optional[str],
        bucket: str,
        key: str,
        start: int,
        max_bytes: int = 4096,
    ) -> tuple[bytes, Optional[int], bool]:
        return await asyncio.to_thread(
            self._get_object_head, profile, bucket, key, max_bytes, start
        )

    async def scan_prefix_recursive(
        self,
        profile: Optional[str],
        bucket: str,
        prefix: str,
        max_keys: Optional[int] = None,
    ) -> tuple[int, int, int, Optional[datetime], int, bool]:
        return await asyncio.to_thread(
            self._scan_prefix_recursive, profile, bucket, prefix, max_keys
        )

    def _scan_prefix_recursive(
        self,
        profile: Optional[str],
        bucket: str,
        prefix: str,
        max_keys: Optional[int],
    ) -> tuple[int, int, int, Optional[datetime], int, bool]:
        client = self._client(profile)
        base_prefix = prefix or ""
        if base_prefix and not base_prefix.endswith("/"):
            base_prefix = f"{base_prefix}/"
        continuation: Optional[str] = None
        file_count = 0
        total_size = 0
        latest_modified: Optional[datetime] = None
        subdirs: set[str] = set()
        scanned = 0
        truncated = False
        limit_reached = False
        while True:
            kwargs = {
                "Bucket": bucket,
                "Prefix": base_prefix,
                "MaxKeys": 1000,
            }
            if continuation:
                kwargs["ContinuationToken"] = continuation
            response = client.list_objects_v2(**kwargs)
            contents = response.get("Contents", [])
            for entry in contents:
                if max_keys is not None and scanned >= max_keys:
                    truncated = True
                    limit_reached = True
                    break
                key = entry.get("Key")
                if not key:
                    continue
                if key.endswith("/"):
                    continue
                if base_prefix and key == base_prefix:
                    continue
                size = int(entry.get("Size", 0))
                file_count += 1
                total_size += size
                scanned += 1
                last_modified = entry.get("LastModified")
                if last_modified and (
                    latest_modified is None or last_modified > latest_modified
                ):
                    latest_modified = last_modified
                relative = (
                    key[len(base_prefix) :]
                    if base_prefix and key.startswith(base_prefix)
                    else key
                )
                if "/" in relative:
                    parts = relative.split("/")[:-1]
                    path = ""
                    for part in parts:
                        if not part:
                            continue
                        path = f"{path}{part}/"
                        subdirs.add(path)
            if limit_reached:
                break
            if response.get("IsTruncated"):
                continuation = response.get("NextContinuationToken")
                continue
            break
        return file_count, len(subdirs), total_size, latest_modified, scanned, truncated

    async def download_object(
        self, profile: Optional[str], bucket: str, key: str, destination: str
    ) -> str:
        return await asyncio.to_thread(
            self._download_object, profile, bucket, key, destination
        )

    async def list_objects_recursive(
        self, profile: Optional[str], bucket: str, prefix: str
    ) -> list[ObjectInfo]:
        return await asyncio.to_thread(
            self._list_objects_recursive, profile, bucket, prefix
        )

    def _download_object(
        self, profile: Optional[str], bucket: str, key: str, destination: str
    ) -> str:
        client = self._client(profile)
        dest_path = str(destination)
        parent = os.path.dirname(dest_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        client.download_file(bucket, key, dest_path)
        return dest_path

    def _list_objects_recursive(
        self, profile: Optional[str], bucket: str, prefix: str
    ) -> list[ObjectInfo]:
        client = self._client(profile)
        base_prefix = prefix or ""
        if base_prefix and not base_prefix.endswith("/"):
            base_prefix = f"{base_prefix}/"
        continuation: Optional[str] = None
        objects: list[ObjectInfo] = []
        while True:
            kwargs = {
                "Bucket": bucket,
                "Prefix": base_prefix,
                "MaxKeys": 1000,
            }
            if continuation:
                kwargs["ContinuationToken"] = continuation
            response = client.list_objects_v2(**kwargs)
            contents = response.get("Contents", [])
            for entry in contents:
                key = entry.get("Key")
                if not key:
                    continue
                if key.endswith("/"):
                    continue
                if base_prefix and key == base_prefix:
                    continue
                objects.append(
                    ObjectInfo(
                        key=key,
                        size=int(entry.get("Size", 0)),
                        last_modified=entry.get("LastModified"),
                        storage_class=entry.get("StorageClass"),
                    )
                )
            if response.get("IsTruncated"):
                continuation = response.get("NextContinuationToken")
            else:
                break
        return objects
