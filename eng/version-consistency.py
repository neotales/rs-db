#!/usr/bin/env python3
"""Check or set the shared version across this workspace's publishable crates.

Every first-party manifest carries the same literal `version`, and every
internal path dependency pins that version exactly (`=X`). The release tag must
match both. `--verify` proves that invariant without changing anything; `--set`
rewrites every occurrence. The external `neotales-resilience` pin is not an
internal path dependency and is deliberately left alone.

Usage:
    python3 eng/version-consistency.py --verify
    python3 eng/version-consistency.py --set 0.0.0-alpha.1
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Every workspace member, including the deliberately unpublished direct MSSQL
# provider. Its exact requirements still have to resolve locally. The detached
# D1 emulator and the vendored quiet roots are not members and are not rewritten.
MANIFESTS = [
    "dbapi/Cargo.toml",
    "dbapi-clickhouse/Cargo.toml",
    "dbapi-d1/Cargo.toml",
    "dbapi-libsql/Cargo.toml",
    "dbapi-mariadb/Cargo.toml",
    "dbapi-mssql/Cargo.toml",
    "dbapi-mssql-direct/Cargo.toml",
    "dbapi-mysql/Cargo.toml",
    "dbapi-postgres/Cargo.toml",
    "dbapi-postgres-tls/Cargo.toml",
    "dbapi-sqlite/Cargo.toml",
    "dbapi-sqlite-direct/Cargo.toml",
    "klickhouse/Cargo.toml",
    "libsql/Cargo.toml",
    "migration/Cargo.toml",
    "migration-clickhouse/Cargo.toml",
    "migration-libsql/Cargo.toml",
    "migration-mariadb/Cargo.toml",
    "migration-mssql/Cargo.toml",
    "migration-mysql/Cargo.toml",
    "migration-postgres/Cargo.toml",
    "migration-sqlite/Cargo.toml",
    "sqlx-mssql/Cargo.toml",
    "tokio-postgres/Cargo.toml",
]

PACKAGE_SECTION = re.compile(r"^\[package\]\s*$")
SECTION = re.compile(r"^\[[^\]]+\]\s*$")
PACKAGE_VERSION = re.compile(r'^version\s*=\s*"([^"]+)"\s*$')
PATH_DEP_VERSION = re.compile(r'version\s*=\s*"=([^"]+)"')


def parse_manifest(path: pathlib.Path) -> tuple[str, list[str]]:
    """Return the package version and every exact path-dependency version."""
    package_version = None
    in_package = False
    requirement_versions: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if SECTION.match(line):
            in_package = bool(PACKAGE_SECTION.match(line))
            continue
        if in_package:
            match = PACKAGE_VERSION.match(line)
            if match:
                package_version = match.group(1)
                in_package = False
            continue
        if 'path = "../' in line:
            match = PATH_DEP_VERSION.search(line)
            if match:
                requirement_versions.append(match.group(1))
    if package_version is None:
        raise SystemExit(f"{path}: no literal [package] version")
    return package_version, requirement_versions


def set_manifest(path: pathlib.Path, new_version: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    in_package = False
    seen_package_version = False
    out: list[str] = []
    for line in lines:
        if SECTION.match(line):
            in_package = bool(PACKAGE_SECTION.match(line))
            out.append(line)
            continue
        if in_package and not seen_package_version and PACKAGE_VERSION.match(line):
            out.append(f'version = "{new_version}"\n')
            seen_package_version = True
            in_package = False
            continue
        if 'path = "../' in line:
            line = PATH_DEP_VERSION.sub(f'version = "={new_version}"', line)
        out.append(line)
    path.write_text("".join(out), encoding="utf-8")


def workspace_metadata() -> dict:
    output = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return json.loads(output)


def verify() -> int:
    problems: list[str] = []
    versions: dict[str, str] = {}
    for relative in MANIFESTS:
        path = ROOT / relative
        package_version, requirements = parse_manifest(path)
        versions[relative] = package_version
        for requirement in requirements:
            if requirement != package_version:
                problems.append(
                    f"{relative}: package version {package_version} but an internal "
                    f'path dependency requires "={requirement}"'
                )
    distinct = sorted(set(versions.values()))
    if len(distinct) != 1:
        for relative, version in sorted(versions.items()):
            problems.append(f"{relative}: version {version}")
        problems.append(f"manifests carry {len(distinct)} distinct versions: {distinct}")
    else:
        print(f"shared version: {distinct[0]}")
        print(f"checked {len(MANIFESTS)} manifests and every internal exact requirement")

    # Cargo's own view catches a manifest the static list missed.
    metadata = workspace_metadata()
    internal = {package["name"]: package["version"] for package in metadata["packages"]}
    expected = distinct[0] if len(distinct) == 1 else None
    for name, version in sorted(internal.items()):
        if expected is not None and version != expected and not name.startswith("mssql-tds"):
            # Vendored crates.io forks keep their own upstream versions.
            vendored = {"mssql-tds", "native-tls", "tokio-native-tls"}
            if name not in vendored:
                problems.append(f"cargo metadata: {name} has version {version}, expected {expected}")

    for problem in problems:
        print(f"error: {problem}", file=sys.stderr)
    return 1 if problems else 0


def set_version(new_version: str) -> int:
    for relative in MANIFESTS:
        set_manifest(ROOT / relative, new_version)
    print(f"set {len(MANIFESTS)} manifests to {new_version}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--verify", action="store_true", help="check consistency only")
    group.add_argument("--set", metavar="VERSION", help="rewrite every internal version")
    args = parser.parse_args()
    if args.verify:
        return verify()
    return set_version(args.set)


if __name__ == "__main__":
    raise SystemExit(main())
