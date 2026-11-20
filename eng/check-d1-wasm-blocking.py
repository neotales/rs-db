#!/usr/bin/env python3
"""Require the sole intentional blocking-feature error from Cargo's wasm check."""
import json
import os
import re
import subprocess
import sys

EXPECTED = "neotales-dbapi's blocking feature is unsupported on wasm32; disable it and consult your provider's supported APIs"
SUMMARY = 'error: could not compile `neotales-dbapi` (lib) due to 1 previous error'
COMMAND = ["cargo", "check", "--target", "wasm32-unknown-unknown", "-p", "neotales-dbapi", "-p", "neotales-dbapi-d1", "--all-features", "--message-format=json"]


def verify(result):
    if result.returncode == 0:
        return False
    errors, finished = [], []

    def collect(message):
        if message.get("level") in ("error", "failure-note"):
            errors.append(message.get("message"))
        for child in message.get("children", []):
            collect(child)

    try:
        for line in result.stdout.splitlines():
            record = json.loads(line)
            if record.get("reason") == "compiler-message":
                collect(record["message"])
            elif record.get("reason") == "build-finished":
                finished.append(record.get("success"))
    except (ValueError, TypeError, KeyError, AttributeError):
        return False
    stderr_errors = [line for line in result.stderr.splitlines()
                     if re.match(r"^error(?:\[.*?\])?:", line)]
    return errors == [EXPECTED] and finished == [False] and stderr_errors == [SUMMARY]


def main():
    # Cargo colours stderr when CARGO_TERM_COLOR=always, which CI sets. The
    # summary regex below is anchored on the plain `error:` prefix, so force a
    # colour-free child process rather than teach the parser ANSI codes.
    environment = os.environ.copy()
    environment["CARGO_TERM_COLOR"] = "never"
    try:
        result = subprocess.run(
            COMMAND, capture_output=True, text=True, errors="replace",
            check=False, env=environment,
        )
        valid = verify(result)
    except OSError:
        valid = False
    if not valid:
        # Cargo output may contain sensitive paths or data; never print it here.
        print("::error::wasm blocking check did not produce exactly the expected diagnostic", file=sys.stderr)
        return 1
    print("wasm blocking check produced exactly the expected diagnostic")
    return 0


if __name__ == "__main__":
    sys.exit(main())
