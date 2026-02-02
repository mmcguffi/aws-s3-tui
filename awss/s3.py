from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional

import boto3


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
    def __init__(self, profiles: Optional[list[str]] = None, region: Optional[str] = None) -> None:
        self.profiles = self._normalize_profiles(profiles)
        self._region = region
        self._clients: dict[str, object] = {}

    def _normalize_profiles(self, profiles: Optional[Iterable[str]]) -> list[Optional[str]]:
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

    async def list_buckets_all(self) -> tuple[list[BucketInfo], list[tuple[Optional[str], Exception]]]:
        tasks = [asyncio.to_thread(self._list_buckets, profile) for profile in self.profiles]
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

    async def list_prefixes(self, profile: Optional[str], bucket: str, prefix: str) -> list[str]:
        return await asyncio.to_thread(self._list_prefixes, profile, bucket, prefix)

    def _list_prefixes(self, profile: Optional[str], bucket: str, prefix: str) -> list[str]:
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
                if last_modified and (latest_modified is None or last_modified > latest_modified):
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
