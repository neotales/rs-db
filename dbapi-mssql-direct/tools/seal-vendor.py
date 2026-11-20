#!/usr/bin/env python3
"""Explicit review operation: inventory vendored files and this crate manifest."""
import hashlib
from pathlib import Path

root = Path(__file__).resolve().parents[1]
paths = [root / "Cargo.toml"]
for package in ("mssql-tds-0.3.0", "native-tls-0.2.18", "tokio-native-tls-0.3.1"):
    paths.extend(p for p in (root / "vendor" / package).rglob("*") if p.is_file())
lines = [
    f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(root)}\n"
    for p in sorted(paths)
]
(root / "vendor" / "SHA256SUMS").write_text("".join(lines))
