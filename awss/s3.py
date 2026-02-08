from __future__ import annotations

import asyncio
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import boto3
import botocore.session
from botocore.exceptions import ConfigNotFound

BUCKET_ACCESS_UNKNOWN = "unknown"
BUCKET_ACCESS_NO_VIEW = "no_view"
BUCKET_ACCESS_NO_DOWNLOAD = "no_download"
BUCKET_ACCESS_GOOD = "good"
BUCKET_ACCESS_LEVELS = {
    BUCKET_ACCESS_NO_VIEW: 0,
    BUCKET_ACCESS_NO_DOWNLOAD: 1,
    BUCKET_ACCESS_GOOD: 2,
    BUCKET_ACCESS_UNKNOWN: 0,
}


@dataclass(frozen=True)
class BucketInfo:
    name: str
    profile: Optional[str]
    access: str = BUCKET_ACCESS_UNKNOWN
    is_empty: bool = False


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    last_modified: Optional[datetime]
    storage_class: Optional[str]


class S3Service:
    def __init__(
        self,
        profiles: Optional[list[str]] = None,
        region: Optional[str] = None,
        cache_path: Optional[Path] = None,
        cache_ttl_seconds: int = 3600,
    ) -> None:
        self.profiles = self._normalize_profiles(profiles)
        self._region = region
        self._clients: dict[str, object] = {}
        self._config_path = self._default_config_path()
        self._bucket_cache_path = cache_path or self._default_bucket_cache_path()
        self._bucket_cache_ttl_seconds = max(0, int(cache_ttl_seconds))

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

    def _is_sso_expired_error(self, exc: Exception) -> bool:
        text = f"{type(exc).__name__}: {exc}".lower()
        markers = [
            "unauthorizedssotokenerror",
            "sso session",
            "sso token",
            "token has expired",
            "token is expired",
            "expiredtoken",
            "the sso session associated with this profile has expired",
            "error loading sso token",
            "run aws sso login",
            "aws sso login",
        ]
        return any(marker in text for marker in markers)

    def _config_base_dir(self) -> Path:
        config_home = os.environ.get("XDG_CONFIG_HOME")
        if config_home:
            base = Path(config_home).expanduser()
        else:
            base = Path.home() / ".config"
        return base / "awss"

    def _default_bucket_cache_path(self) -> Path:
        return self._config_base_dir() / "bucket-cache.json"

    def _default_config_path(self) -> Path:
        return self._config_base_dir() / "config.json"

    def _aws_config_path(self) -> Path:
        return Path.home() / ".aws" / "config"

    def _aws_credentials_path(self) -> Path:
        return Path.home() / ".aws" / "credentials"

    def _aws_config_hash(self) -> Optional[str]:
        hasher = hashlib.sha256()
        found = False
        sources = (
            ("config", self._aws_config_path()),
            ("credentials", self._aws_credentials_path()),
        )
        for label, path in sources:
            try:
                data = path.read_bytes()
            except Exception:
                continue
            hasher.update(label.encode("utf-8"))
            hasher.update(b"\0")
            hasher.update(data)
            hasher.update(b"\0")
            found = True
        if not found:
            return None
        return hasher.hexdigest()

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
            profiles = by_name.setdefault(bucket.name, [])
            if bucket.profile not in profiles:
                profiles.append(bucket.profile)
        if not by_name:
            return []

        probe_profiles = list(self.profiles) or [None]
        profile_rank = {profile: index for index, profile in enumerate(probe_profiles)}
        probe_keys: list[tuple[str, Optional[str]]] = []
        probe_tasks: list[asyncio.Future] = []
        for name in by_name:
            for profile in probe_profiles:
                probe_keys.append((name, profile))
                probe_tasks.append(
                    asyncio.to_thread(
                        self._probe_profile_access_for_bucket,
                        name,
                        profile,
                    )
                )

        probe_access: dict[tuple[str, Optional[str]], str] = {}
        if probe_tasks:
            probe_results = await asyncio.gather(*probe_tasks, return_exceptions=True)
            for key, result in zip(probe_keys, probe_results):
                if isinstance(result, Exception):
                    probe_access[key] = BUCKET_ACCESS_NO_VIEW
                    continue
                probe_access[key] = self._normalize_bucket_access(result)

        resolved: list[BucketInfo] = []
        for name, listed_profiles in by_name.items():
            available_profiles = set(listed_profiles)
            ranked_profiles = list(probe_profiles)
            if not ranked_profiles:
                ranked_profiles = listed_profiles or [None]

            def profile_key(profile: Optional[str]) -> tuple[int, int, int, int]:
                access = probe_access.get((name, profile), BUCKET_ACCESS_NO_VIEW)
                level = self._bucket_access_level(access)
                non_default = 1 if profile is not None else 0
                listed = 1 if profile in available_profiles else 0
                rank = profile_rank.get(profile, len(profile_rank))
                return (level, non_default, listed, -rank)

            best_profile = max(ranked_profiles, key=profile_key)
            best_access = probe_access.get((name, best_profile), BUCKET_ACCESS_NO_VIEW)
            if self._bucket_access_level(best_access) <= 0:
                fallback_profiles = listed_profiles or ranked_profiles
                best_profile = max(
                    fallback_profiles,
                    key=lambda profile: (
                        profile is not None,
                        -(profile_rank.get(profile, len(profile_rank))),
                    ),
                )
                best_access = BUCKET_ACCESS_NO_VIEW
            resolved.append(
                BucketInfo(name=name, profile=best_profile, access=best_access)
            )
        return resolved

    def _normalize_bucket_access(self, value: object) -> str:
        if not isinstance(value, str):
            return BUCKET_ACCESS_UNKNOWN
        normalized = value.strip().lower()
        if normalized in BUCKET_ACCESS_LEVELS:
            return normalized
        return BUCKET_ACCESS_UNKNOWN

    def _bucket_access_level(self, access: str) -> int:
        return BUCKET_ACCESS_LEVELS.get(access, 0)

    def _probe_profile_access_for_bucket(
        self, bucket: str, profile: Optional[str]
    ) -> str:
        client = self._client(profile)
        try:
            response = client.list_objects_v2(Bucket=bucket, MaxKeys=10)
        except Exception as exc:
            if self._is_sso_expired_error(exc):
                raise
            return BUCKET_ACCESS_NO_VIEW
        contents = response.get("Contents", []) if isinstance(response, dict) else []
        keys: list[str] = []
        for entry in contents[:10]:
            if not isinstance(entry, dict):
                continue
            key = entry.get("Key")
            if not isinstance(key, str) or not key:
                continue
            keys.append(key)
        if not keys:
            return BUCKET_ACCESS_GOOD

        for key in keys[:5]:
            try:
                response = client.get_object(
                    Bucket=bucket,
                    Key=key,
                    Range="bytes=0-0",
                )
            except Exception as exc:
                if self._is_sso_expired_error(exc):
                    raise
                continue
            body = response.get("Body") if isinstance(response, dict) else None
            if body is not None:
                try:
                    body.read(1)
                finally:
                    try:
                        body.close()
                    except Exception:
                        pass
            return BUCKET_ACCESS_GOOD
        return BUCKET_ACCESS_NO_DOWNLOAD

    async def bucket_access(self, profile: Optional[str], bucket: str) -> str:
        access = await asyncio.to_thread(
            self._probe_profile_access_for_bucket,
            bucket,
            profile,
        )
        return self._normalize_bucket_access(access)

    async def is_bucket_empty(self, profile: Optional[str], bucket: str) -> bool:
        return await asyncio.to_thread(self._is_bucket_empty, profile, bucket)

    def _is_bucket_empty(self, profile: Optional[str], bucket: str) -> bool:
        client = self._client(profile)
        try:
            response = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        except Exception as exc:
            if self._is_sso_expired_error(exc):
                raise
            return False
        if not isinstance(response, dict):
            return False
        contents = response.get("Contents", [])
        if isinstance(contents, list) and contents:
            return False
        key_count = response.get("KeyCount")
        if isinstance(key_count, int):
            return key_count == 0
        return True

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

    def _parse_cache_saved_at(self, value: object) -> Optional[datetime]:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    def _decode_profile(self, value: object) -> Optional[str]:
        if value is None:
            return None
        if not isinstance(value, str):
            return None
        normalized = value.strip()
        if not normalized or normalized in {"default", "__default__"}:
            return None
        return normalized

    def _decode_access(self, value: object) -> str:
        return self._normalize_bucket_access(value)

    def _decode_is_empty(self, value: object) -> bool:
        return bool(value)

    def _decode_cache_hash(self, value: object) -> Optional[str]:
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
        return None

    def _read_bucket_cache(
        self,
    ) -> tuple[Optional[datetime], list[BucketInfo], Optional[str]]:
        try:
            payload = json.loads(self._bucket_cache_path.read_text())
        except Exception:
            return None, [], None
        if not isinstance(payload, dict):
            return None, [], None
        items = payload.get("buckets")
        cache_hash = self._decode_cache_hash(payload.get("aws_config_sha256"))
        if not isinstance(items, list):
            return self._parse_cache_saved_at(payload.get("saved_at")), [], cache_hash
        buckets: list[BucketInfo] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if not isinstance(name, str):
                continue
            stripped = name.strip()
            if not stripped:
                continue
            profile = self._decode_profile(item.get("profile"))
            access = self._decode_access(item.get("access"))
            is_empty = self._decode_is_empty(item.get("is_empty"))
            buckets.append(
                BucketInfo(
                    name=stripped,
                    profile=profile,
                    access=access,
                    is_empty=is_empty,
                )
            )
        saved_at = self._parse_cache_saved_at(payload.get("saved_at"))
        return saved_at, buckets, cache_hash

    def load_cached_bucket_preferences(self) -> dict[str, Optional[str]]:
        buckets = self.load_bucket_cache()
        preferred: dict[str, Optional[str]] = {}
        for bucket in buckets:
            preferred[bucket.name] = bucket.profile
        return preferred

    def load_bucket_cache(self) -> list[BucketInfo]:
        saved_at, buckets, cache_hash = self._read_bucket_cache()
        if not buckets:
            return []
        if cache_hash != self._aws_config_hash():
            return []
        if self._bucket_cache_ttl_seconds <= 0:
            return buckets
        if saved_at is None:
            return []
        age = datetime.now(timezone.utc) - saved_at
        if age > timedelta(seconds=self._bucket_cache_ttl_seconds):
            return []
        return buckets

    def save_bucket_cache(self, buckets: list[BucketInfo]) -> bool:
        latest_by_name: dict[str, BucketInfo] = {}
        for bucket in buckets:
            latest_by_name[bucket.name] = bucket
        rows = [
            {
                "name": name,
                "profile": info.profile,
                "access": self._normalize_bucket_access(info.access),
                "is_empty": bool(info.is_empty),
            }
            for name, info in sorted(latest_by_name.items(), key=lambda item: item[0])
        ]
        payload = {
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "aws_config_sha256": self._aws_config_hash(),
            "buckets": rows,
        }
        try:
            self._bucket_cache_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self._bucket_cache_path.with_suffix(".tmp")
            temp_path.write_text(json.dumps(payload, indent=2))
            temp_path.replace(self._bucket_cache_path)
        except Exception:
            return False
        return True

    def _read_app_config(self) -> dict[str, object]:
        try:
            payload = json.loads(self._config_path.read_text())
        except Exception:
            return {}
        if not isinstance(payload, dict):
            return {}
        return payload

    def load_bucket_filter_state(self) -> dict[str, bool]:
        defaults = {
            "hide_no_view": False,
            "hide_no_download": False,
            "hide_empty": False,
            "only_favorites": False,
        }
        payload = self._read_app_config()
        section = payload.get("bucket_filters")
        if not isinstance(section, dict):
            return defaults
        return {
            "hide_no_view": bool(section.get("hide_no_view", defaults["hide_no_view"])),
            "hide_no_download": bool(
                section.get("hide_no_download", defaults["hide_no_download"])
            ),
            "hide_empty": bool(section.get("hide_empty", defaults["hide_empty"])),
            "only_favorites": bool(
                section.get("only_favorites", defaults["only_favorites"])
            ),
        }

    def save_bucket_filter_state(self, state: dict[str, bool]) -> bool:
        payload = self._read_app_config()
        payload["bucket_filters"] = {
            "hide_no_view": bool(state.get("hide_no_view", False)),
            "hide_no_download": bool(state.get("hide_no_download", False)),
            "hide_empty": bool(state.get("hide_empty", False)),
            "only_favorites": bool(state.get("only_favorites", False)),
        }
        try:
            self._config_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self._config_path.with_suffix(".tmp")
            temp_path.write_text(json.dumps(payload, indent=2))
            temp_path.replace(self._config_path)
        except Exception:
            return False
        return True

    def load_favorite_buckets(self) -> set[str]:
        payload = self._read_app_config()
        values = payload.get("favorite_buckets")
        if not isinstance(values, list):
            return set()
        favorites: set[str] = set()
        for value in values:
            if not isinstance(value, str):
                continue
            normalized = value.strip()
            if not normalized:
                continue
            favorites.add(normalized)
        return favorites

    def save_favorite_buckets(self, favorites: set[str]) -> bool:
        payload = self._read_app_config()
        values = sorted(
            value.strip() for value in favorites if isinstance(value, str) and value.strip()
        )
        payload["favorite_buckets"] = values
        try:
            self._config_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self._config_path.with_suffix(".tmp")
            temp_path.write_text(json.dumps(payload, indent=2))
            temp_path.replace(self._config_path)
        except Exception:
            return False
        return True

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
                buckets.append(
                    BucketInfo(
                        name=name,
                        profile=profile,
                        access=BUCKET_ACCESS_UNKNOWN,
                    )
                )
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
