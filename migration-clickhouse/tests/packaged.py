#!/usr/bin/env python3
"""Run a consumer of an isolated source archive, never the checkout's library."""

import io
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import tomllib


def main():
    workspace = Path(__file__).resolve().parents[2]
    cache = Path.home() / ".cache/neotales/plab-clickhouse-experimental-tmp"
    cache.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(cache).free < 8 * 1024**3:
        raise SystemExit("packaged consumer requires at least 8 GiB free")
    members = ["dbapi", "dbapi-clickhouse", "migration", "migration-clickhouse"]
    manifest = (workspace / "Cargo.toml").read_text()
    manifest = re.sub(
        r"\[workspace\].*?(?=\[workspace\.package\])",
        '[workspace]\nresolver = "3"\nmembers = ['
        + ", ".join(f'"{name}"' for name in members) + "]\n",
        manifest, count=1, flags=re.S,
    )
    with tempfile.TemporaryDirectory(prefix="consumer-", dir=cache) as temporary:
        root = Path(temporary)
        bundle = root / "source.tar"
        paths = list(members)
        # Only tracked source paths, read from this dirty worktree (not HEAD).
        tracked = subprocess.check_output(
            ["git", "ls-files", "-z", "--", *paths], cwd=workspace
        ).decode().split("\0")
        with tarfile.open(bundle, "w") as archive:
            for name in filter(None, tracked):
                archive.add(workspace / name, arcname=name, recursive=False)
            encoded = manifest.encode()
            info = tarfile.TarInfo("Cargo.toml")
            info.size = len(encoded)
            archive.addfile(info, io.BytesIO(encoded))
            archive.add(workspace / "Cargo.lock", arcname="Cargo.lock")
        source = root / "source"
        source.mkdir()
        with tarfile.open(bundle) as archive:
            archive.extractall(source, filter="data")
        consumer = root / "consumer"
        (consumer / "tests/support").mkdir(parents=True)
        shutil.copy2(source / "Cargo.lock", consumer / "Cargo.lock")
        fixture = source / "migration-clickhouse/tests"
        shutil.copy2(fixture / "live.rs", consumer / "tests/live.rs")
        shutil.copytree(fixture / "support", consumer / "tests/support", dirs_exist_ok=True)
        tables = tomllib.loads(manifest)
        # Copy the tested workspace versions, not a second set of dependency pins.
        tokio = tables["workspace"]["dependencies"]["tokio"]["version"]
        getrandom = tables["workspace"]["dependencies"]["getrandom"]
        testcontainers = tomllib.loads(
            (source / "migration-clickhouse/Cargo.toml").read_text()
        )["dev-dependencies"]["testcontainers"]
        (consumer / "Cargo.toml").write_text(f"""
[workspace]
resolver = "3"
[package]
name = "clickhouse-packaged-consumer"
version = "0.0.0"
edition = "2024"
[features]
experimental-native-migrations = ["neotales-migration-clickhouse/experimental-native-migrations"]
[dependencies]
neotales-dbapi = {{ path = "../source/data/dbapi" }}
neotales-dbapi-clickhouse = {{ path = "../source/data/dbapi-clickhouse" }}
neotales-migration = {{ path = "../source/data/migration" }}
neotales-migration-clickhouse = {{ path = "../source/data/migration-clickhouse", default-features = false }}
getrandom = "{getrandom}"
testcontainers = "{testcontainers}"
tokio = {{ version = "{tokio}", features = ["io-util", "macros", "net", "rt-multi-thread", "time"] }}
""")
        environment = os.environ.copy()
        environment["TMPDIR"] = str(cache)
        environment["CARGO_TARGET_DIR"] = str(
            Path.home() / ".cache/neotales/plab-clickhouse-experimental-target"
        )
        for features, selected in [
            ([], "the_backend_fails_closed_on_the_fetch_only_provider"),
            (["--features", "experimental-native-migrations"],
             "experimental_dependency_recovery_contract"),
        ]:
            command = ["cargo", "test", "--offline", "--test", "live", *features]
            listing = subprocess.check_output(
                [*command, "--", "--ignored", "--list"], cwd=consumer, env=environment
            ).decode()
            if f"{selected}: test" not in listing.splitlines():
                raise SystemExit(f"missing consumer test: {selected}")
            print(f"packaged consumer: {selected}", flush=True)
            subprocess.run(
                [*command, "--", "--ignored", "--exact", selected, "--nocapture"],
                cwd=consumer, env=environment, check=True,
            )


if __name__ == "__main__":
    main()
