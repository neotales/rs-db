#!/usr/bin/env python3
"""Exercise the vendor sealer in a disposable copy without touching its input."""
import hashlib
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


SOURCE = Path(__file__).resolve().parents[1]
PACKAGES = (
    "mssql-tds-0.3.0",
    "native-tls-0.2.18",
    "tokio-native-tls-0.3.1",
)


def main() -> None:
    checked_inventory = (SOURCE / "vendor" / "SHA256SUMS").read_bytes()
    with tempfile.TemporaryDirectory(prefix="mssql-seal-test-") as temporary:
        copied = Path(temporary) / "crate"
        copied.mkdir()
        shutil.copy2(SOURCE / "Cargo.toml", copied / "Cargo.toml")
        (copied / "tools").mkdir(parents=True)
        shutil.copy2(SOURCE / "tools" / "seal-vendor.py", copied / "tools")
        for package in PACKAGES:
            shutil.copytree(SOURCE / "vendor" / package, copied / "vendor" / package)

        sealer = copied / "tools" / "seal-vendor.py"
        subprocess.run([sys.executable, str(sealer)], check=True)
        inventory = (copied / "vendor" / "SHA256SUMS").read_bytes()
        subprocess.run([sys.executable, str(sealer)], check=True)
        assert (copied / "vendor" / "SHA256SUMS").read_bytes() == inventory
        entries = [
            line.split("  ", 1)[1]
            for line in inventory.decode().splitlines()
        ]
        assert "Cargo.lock" not in entries

        for line in inventory.decode().splitlines():
            digest, relative = line.split("  ", 1)
            assert hashlib.sha256((copied / relative).read_bytes()).hexdigest() == digest

    assert (SOURCE / "vendor" / "SHA256SUMS").read_bytes() == checked_inventory


if __name__ == "__main__":
    main()
