#!/usr/bin/env python3
"""Run `cargo publish --dry-run` for every released crate and classify failures.

Before the first upload, a crate whose internal sibling is not on crates.io yet
cannot be packaged. That is the only tolerated failure. Accepting any
"no matching package named" error would hide a typo in an external dependency,
an unpublished external crate, a wrong internal version requirement, or a
verification/build failure as "expected".

A tolerated failure must name at least one package that:

* is a cargo package in this workspace (not an external crate), and
* carries the same version as the crate being checked (the release contract),
  and
* for the `failed to select a version` form, appears with that exact version in
  the requirement text.

The cause must be the crates.io index. Compile, manifest, packaging, and
tarball-verification errors are always fatal. `--self-test` exercises the
classifier against fixed fixtures, including the negative cases, without
network access.

Usage:
    python3 eng/package-preflight.py
    python3 eng/package-preflight.py --self-test
    python3 eng/package-preflight.py --classify FILE --package NAME --status 101
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

PACKAGES = [
    "neotales-klickhouse",
    "neotales-libsql",
    "neotales-tokio-postgres",
    "neotales-dbapi",
    "neotales-migration",
    "neotales-sqlx-mssql",
    "neotales-dbapi-d1",
    "neotales-dbapi-sqlite",
    "neotales-dbapi-sqlite-direct",
    "neotales-dbapi-libsql",
    "neotales-dbapi-mysql",
    "neotales-dbapi-mariadb",
    "neotales-dbapi-mssql",
    "neotales-dbapi-clickhouse",
    "neotales-dbapi-postgres-tls",
    "neotales-dbapi-postgres",
    "neotales-migration-sqlite",
    "neotales-migration-libsql",
    "neotales-migration-mysql",
    "neotales-migration-mariadb",
    "neotales-migration-mssql",
    "neotales-migration-clickhouse",
    "neotales-migration-postgres",
]

NO_MATCHING = re.compile(r"no matching package named `([^`]+)` found")
SELECT_VERSION = re.compile(
    r"failed to select a version for the requirement `([^`=<>~^ ]+)\s*(?:[=<>~^]+)\s*([^`]+)`"
)
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text: str) -> str:
    return ANSI.sub("", text)


def workspace_versions() -> dict[str, str]:
    output = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return {
        package["name"]: package["version"]
        for package in json.loads(output)["packages"]
        if not package["name"] in {"mssql-tds", "native-tls", "tokio-native-tls"}
    }


def normalize_requirement(text: str) -> str:
    value = text.strip().strip('"').strip()
    return re.sub(r"^[=<>~^]+\s*", "", value)


def classify(
    output: str, status: int, package: str, versions: dict[str, str]
) -> tuple[bool, str]:
    """Return (accepted, reason) for one dry-run result."""
    if status == 0:
        return True, "packaged and verified"

    # CI sets CARGO_TERM_COLOR=always, so the captured output contains SGR
    # escapes around `error:` and paths. Strip them before matching text.
    output = strip_ansi(output)

    expected = versions.get(package)
    if expected is None:
        return False, f"{package} is not a workspace package"

    # A real build or tarball verification failure is never an index-lag pass.
    if "failed to verify package tarball" in output:
        return False, "tarball verification failed"
    if "error: failed to prepare local package for uploading" not in output:
        return False, "unexpected cargo publish failure"
    if "location searched: crates.io index" not in output:
        return False, "failure was not an unpublished crates.io resolution"

    names = NO_MATCHING.findall(output)
    requirements = SELECT_VERSION.findall(output)
    if not names and not requirements:
        return False, "no internal dependency named in the resolution failure"

    for name in names:
        if name not in versions:
            return False, f"unpublished dependency `{name}` is not a workspace package"
        if versions[name] != expected:
            return False, (
                f"workspace dependency `{name}` is {versions[name]}, "
                f"but {package} is {expected}"
            )
    for name, requirement in requirements:
        if name not in versions:
            return False, f"version conflict for external package `{name}`"
        if versions[name] != expected:
            return False, (
                f"workspace dependency `{name}` is {versions[name]}, "
                f"but {package} is {expected}"
            )
        if normalize_requirement(requirement) != expected:
            return False, (
                f"`{name}` requirement is {requirement.strip()}, "
                f"not the shared {expected}"
            )

    # The bare "error:" line from cargo must be the resolution wrapper, not a
    # compiler diagnostic that happened to accompany it.
    wrappers = {
        "failed to prepare local package for uploading",
        "failed to get",
    }
    for match in re.finditer(r"^error(?:\[E[0-9]+\])?: (.+)$", output, re.MULTILINE):
        message = match.group(1)
        if any(message.startswith(prefix) for prefix in wrappers):
            continue
        if message.startswith("failed to select a version for the requirement"):
            continue
        if message.startswith("no matching package named"):
            continue
        return False, f"unexpected error line: {message}"

    return True, "expected unpublished workspace dependency"


SELF_TESTS = [
    (
        "expected sibling",
        101,
        """error: failed to prepare local package for uploading

Caused by:
  no matching package named `neotales-dbapi` found
  location searched: crates.io index
  required by package `neotales-dbapi-sqlite v0.0.0-alpha.0 (/tmp/x)`
""",
        True,
    ),
    (
        "expected sibling with CI colour codes",
        101,
        "\x1b[1m\x1b[91merror\x1b[0m: failed to prepare local package for uploading\n\nCaused by:\n  no matching package named `neotales-dbapi` found\n  location searched: crates.io index\n  required by package `neotales-dbapi-sqlite v0.0.0-alpha.0 (/tmp/x)`\n",
        True,
    ),
    (
        "expected sibling by version select",
        101,
        """error: failed to prepare local package for uploading

Caused by:
  failed to select a version for the requirement `neotales-dbapi = "^0.0.0-alpha.0"`
  candidate versions found which didn't match: 0.0.0-alpha.0
  location searched: crates.io index
  required by package `neotales-dbapi-sqlite v0.0.0-alpha.0 (/tmp/x)`
""",
        True,
    ),
    (
        "external missing package",
        101,
        """error: failed to prepare local package for uploading

Caused by:
  no matching package named `totally-external` found
  location searched: crates.io index
  required by package `neotales-dbapi-sqlite v0.0.0-alpha.0 (/tmp/x)`
""",
        False,
    ),
    (
        "internal wrong requirement version",
        101,
        """error: failed to prepare local package for uploading

Caused by:
  failed to select a version for the requirement `neotales-dbapi = "^0.0.0-alpha.1"`
  location searched: crates.io index
  required by package `neotales-dbapi-sqlite v0.0.0-alpha.0 (/tmp/x)`
""",
        False,
    ),
    (
        "compile failure",
        101,
        """error: failed to prepare local package for uploading

Caused by:
  failed to verify package tarball

Caused by:
  error[E0425]: cannot find value `missing` in this scope
""",
        False,
    ),
    (
        "manifest failure",
        101,
        """error: failed to prepare local package for uploading

Caused by:
  failed to parse manifest at `/tmp/x/Cargo.toml`

Caused by:
  no `version` field was specified
""",
        False,
    ),
    (
        "path resolution mistaken for index lag",
        101,
        """error: failed to prepare local package for uploading

Caused by:
  no matching package named `neotales-dbapi` found
  location searched: `/tmp/vendor`
""",
        False,
    ),
    (
        "mixed external and internal",
        101,
        """error: failed to prepare local package for uploading

Caused by:
  no matching package named `neotales-dbapi` found
  location searched: crates.io index
  required by package `neotales-dbapi-sqlite v0.0.0-alpha.0 (/tmp/x)`
  no matching package named `totally-external` found
""",
        False,
    ),
    (
        "success",
        0,
        "",
        True,
    ),
]


def self_test() -> int:
    versions = {
        "neotales-dbapi": "0.0.0-alpha.0",
        "neotales-dbapi-sqlite": "0.0.0-alpha.0",
    }
    failed = 0
    for label, status, output, expected in SELF_TESTS:
        accepted, reason = classify(output, status, "neotales-dbapi-sqlite", versions)
        if accepted != expected:
            failed += 1
            print(f"FAIL {label}: accepted={accepted}, expected {expected}: {reason}")
        else:
            print(f"ok   {label}: {reason}")
    if failed:
        print(f"{failed} classifier self-test(s) failed", file=sys.stderr)
        return 1
    print(f"{len(SELF_TESTS)} classifier self-tests passed")
    return 0


def run(allow_dirty: bool) -> int:
    versions = workspace_versions()
    failed = 0
    for package in PACKAGES:
        print(f"::group::cargo publish --dry-run -p {package}")
        command = ["cargo", "publish", "-p", package, "--dry-run", "--locked"]
        if allow_dirty:
            command.append("--allow-dirty")
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        output = result.stdout + result.stderr
        print(output, end="")
        print("::endgroup::")
        accepted, reason = classify(output, result.returncode, package, versions)
        if accepted:
            print(f"ok: {package}: {reason}")
        else:
            print(f"::error::{package} failed preflight: {reason}")
            failed += 1
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="local verification only; never use for a release run",
    )
    parser.add_argument("--classify", metavar="FILE", help="classify one captured failure")
    parser.add_argument("--package", help="package name for --classify")
    parser.add_argument("--status", type=int, default=101, help="exit status for --classify")
    args = parser.parse_args()
    if args.classify:
        if not args.package:
            parser.error("--classify requires --package")
        output = pathlib.Path(args.classify).read_text(encoding="utf-8", errors="replace")
        accepted, reason = classify(output, args.status, args.package, workspace_versions())
        print(reason)
        return 0 if accepted else 1
    return self_test() if args.self_test else run(args.allow_dirty)


if __name__ == "__main__":
    raise SystemExit(main())
