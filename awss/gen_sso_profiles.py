#!/usr/bin/env python3
from __future__ import annotations

import argparse
import configparser
import datetime as dt
import json
import os
import re
import sys
from pathlib import Path
from typing import Iterable

import boto3
from botocore.config import Config

AWS_CONFIG = Path(os.environ.get("AWS_CONFIG_FILE", "~/.aws/config")).expanduser()
SSO_CACHE_DIR = Path("~/.aws/sso/cache").expanduser()

# ---- tweak these defaults if you want ----
DEFAULT_REGION_FOR_PROFILES = "us-east-1"
DEFAULT_OUTPUT = "json"

# Profile name template. You can change this.
# Using accountId prevents collisions when two accounts share the same accountName.
PROFILE_NAME_FMT = "{accountName}-{accountId}-{roleName}"
# -----------------------------------------


def load_aws_config() -> configparser.RawConfigParser:
    cp = configparser.RawConfigParser()
    cp.read(AWS_CONFIG)
    return cp


def _iter_sso_session_sections(cp: configparser.RawConfigParser) -> Iterable[str]:
    for sec in cp.sections():
        if sec.startswith("sso-session "):
            yield sec


def _read_sso_session(cp: configparser.RawConfigParser, sec: str) -> dict[str, str]:
    start_url = cp.get(sec, "sso_start_url", fallback=None)
    sso_region = cp.get(sec, "sso_region", fallback=None)
    if not start_url or not sso_region:
        raise SystemExit(f"[{sec}] must include sso_start_url and sso_region")
    name = sec[len("sso-session ") :]
    return {"name": name, "sso_start_url": start_url, "sso_region": sso_region}


def _safe_profile_name(name: str) -> str:
    """
    AWS profile names are fairly permissive, but in practice:
    - spaces + weird punctuation cause pain in shells and tooling
    - slashes show up in role names, etc.
    """
    name = name.strip()
    name = re.sub(r"\s+", "-", name)
    name = re.sub(r"[^A-Za-z0-9+=,.@_-]+", "-", name)  # conservative
    name = re.sub(r"-{2,}", "-", name).strip("-")
    return name or "profile"


def _parse_expires_at(expires_at: str) -> dt.datetime | None:
    """
    AWS CLI SSO cache uses strings like: "2026-02-07T12:34:56UTC"
    Sometimes also "Z" or "+00:00".
    We normalize to a naive UTC datetime for simple comparisons.
    """
    s = expires_at.strip()
    s = s.replace("UTC", "Z")
    try:
        # fromisoformat doesn't like bare Z, so convert to +00:00
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
            d = dt.datetime.fromisoformat(s2)
        else:
            d = dt.datetime.fromisoformat(s)
        # make naive UTC
        if d.tzinfo is not None:
            d = d.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return d
    except Exception:
        return None


def newest_token_for_start_url(start_url: str) -> str:
    """
    AWS CLI stores SSO access tokens as JSON in ~/.aws/sso/cache/*.json
    We pick the newest unexpired token matching the startUrl.
    """
    best: tuple[float, str] | None = None
    now = dt.datetime.utcnow()

    for p in SSO_CACHE_DIR.glob("*.json"):
        try:
            data = json.loads(p.read_text())
        except Exception:
            continue

        if data.get("startUrl") != start_url:
            continue

        access_token = data.get("accessToken")
        expires_at = data.get("expiresAt")
        if not access_token or not expires_at:
            continue

        exp_dt = _parse_expires_at(str(expires_at))
        if not exp_dt or exp_dt <= now:
            continue

        mtime = p.stat().st_mtime
        if best is None or mtime > best[0]:
            best = (mtime, access_token)

    if not best:
        raise SystemExit(
            f"No valid cached token found for startUrl={start_url}. "
            f"Run: aws sso login --sso-session <name>"
        )
    return best[1]


def _canonicalize_sessions(
    cp: configparser.RawConfigParser, preferred_name: str | None
) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    """
    Groups sso-session sections by (start_url, sso_region) and chooses a canonical name per group.

    Returns:
      canonical_by_key: (start_url, sso_region) -> canonical_session_name
      alias_to_canonical: existing_session_name -> canonical_session_name
    """
    sessions = [_read_sso_session(cp, sec) for sec in _iter_sso_session_sections(cp)]

    # group by (start_url, sso_region)
    by_key: dict[tuple[str, str], list[dict[str, str]]] = {}
    for s in sessions:
        key = (s["sso_start_url"], s["sso_region"])
        by_key.setdefault(key, []).append(s)

    canonical_by_key: dict[tuple[str, str], str] = {}
    alias_to_canonical: dict[str, str] = {}

    for key, group in by_key.items():
        names = [g["name"] for g in group]
        canonical = None
        if preferred_name and preferred_name in names:
            canonical = preferred_name
        else:
            # deterministic: shortest then lexicographic
            canonical = sorted(names, key=lambda x: (len(x), x))[0]

        canonical_by_key[key] = canonical
        for n in names:
            alias_to_canonical[n] = canonical

        # If there are duplicates, keep all sso-session blocks as-is,
        # but we will rewrite profiles to reference the canonical.
        if len(set(names)) > 1:
            print(
                f"Note: grouping sso-sessions {sorted(set(names))} "
                f"under canonical '{canonical}' for start_url={key[0]} region={key[1]}",
                file=sys.stderr,
            )

    return canonical_by_key, alias_to_canonical


def _rewrite_existing_profile_sessions(
    cp: configparser.RawConfigParser, alias_to_canonical: dict[str, str]
) -> int:
    """
    For any profile that has sso_session=<name>, rewrite it to the canonical session for that same start_url/region group.
    """
    rewrites = 0
    for sec in cp.sections():
        if sec == "default" or sec.startswith("profile "):
            sess = cp.get(sec, "sso_session", fallback=None)
            if sess and sess in alias_to_canonical:
                canonical = alias_to_canonical[sess]
                if canonical != sess:
                    cp.set(sec, "sso_session", canonical)
                    rewrites += 1
    return rewrites


def _ensure_profile_section(cp: configparser.RawConfigParser, prof_name: str) -> str:
    """
    Returns the config section name (default or profile X), ensuring it exists.
    """
    section = "default" if prof_name == "default" else f"profile {prof_name}"
    if not cp.has_section(section):
        cp.add_section(section)
    return section


def _fetch_and_add_profiles_for_session(
    cp: configparser.RawConfigParser,
    sso_session_name: str,
    start_url: str,
    sso_region: str,
) -> int:
    """
    Lists all accounts and roles available to the cached token and adds/updates profiles.
    """
    access_token = newest_token_for_start_url(start_url)

    sso = boto3.client(
        "sso",
        region_name=sso_region,
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )

    accounts: list[dict] = []
    for page in sso.get_paginator("list_accounts").paginate(accessToken=access_token):
        accounts.extend(page.get("accountList", []))

    updates = 0
    for acct in accounts:
        account_id = acct["accountId"]
        account_name = acct.get("accountName") or account_id

        roles: list[dict] = []
        for page in sso.get_paginator("list_account_roles").paginate(
            accessToken=access_token, accountId=account_id
        ):
            roles.extend(page.get("roleList", []))

        for role in roles:
            role_name = role["roleName"]

            prof_name_raw = PROFILE_NAME_FMT.format(
                accountName=account_name, accountId=account_id, roleName=role_name
            )
            prof_name = _safe_profile_name(prof_name_raw)

            section = _ensure_profile_section(cp, prof_name)
            cp.set(section, "sso_session", sso_session_name)
            cp.set(section, "sso_account_id", account_id)
            cp.set(section, "sso_role_name", role_name)
            cp.set(section, "region", DEFAULT_REGION_FOR_PROFILES)
            cp.set(section, "output", DEFAULT_OUTPUT)
            updates += 1

    return updates


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Generate/merge AWS CLI SSO profiles for all IAM Identity Center accounts/roles, "
            "printing the resulting ~/.aws/config to stdout."
        )
    )
    ap.add_argument(
        "--sso-session",
        default=None,
        help=(
            "Only generate profiles for this sso-session name. "
            "If omitted, all [sso-session ...] blocks in the config are used. "
            "Also preferred as canonical when grouping duplicates."
        ),
    )
    args = ap.parse_args(argv)

    cp = load_aws_config()

    # Find all sso-session blocks
    session_secs = list(_iter_sso_session_sections(cp))
    if not session_secs:
        print(
            f"No [sso-session ...] blocks found in {AWS_CONFIG}. "
            f"Create one with: aws configure sso-session",
            file=sys.stderr,
        )
        return 2

    # Decide which sessions we will query
    sessions = [_read_sso_session(cp, sec) for sec in session_secs]
    if args.sso_session is not None:
        sessions = [s for s in sessions if s["name"] == args.sso_session]
        if not sessions:
            print(
                f"Requested --sso-session '{args.sso_session}' not found in {AWS_CONFIG}.",
                file=sys.stderr,
            )
            return 2

    # Group equivalent sessions and pick canonical per (start_url, region)
    canonical_by_key, alias_to_canonical = _canonicalize_sessions(cp, args.sso_session)

    # Rewrite existing profiles to use canonical sessions where possible
    rewrites = _rewrite_existing_profile_sessions(cp, alias_to_canonical)
    if rewrites:
        print(
            f"Rewrote {rewrites} existing profile(s) to canonical sso_session names.",
            file=sys.stderr,
        )

    # Generate profiles for each selected SSO session group (deduped by key)
    total_updates = 0
    seen_keys: set[tuple[str, str]] = set()

    for s in sessions:
        key = (s["sso_start_url"], s["sso_region"])
        if key in seen_keys:
            continue
        seen_keys.add(key)

        canonical_name = canonical_by_key[key]
        print(
            f"Fetching accounts/roles for sso_session='{canonical_name}' "
            f"(start_url={key[0]}, region={key[1]})",
            file=sys.stderr,
        )
        total_updates += _fetch_and_add_profiles_for_session(
            cp, canonical_name, key[0], key[1]
        )

    print(f"Updated/created {total_updates} profile(s).", file=sys.stderr)

    # Always print resulting config to stdout
    cp.write(sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
