#!/usr/bin/env python3
"""Reproduce the quiet forks from checksum-verified crates.io archives.

No source is fetched implicitly. Supply a directory containing the three .crate
archives. Rust source changes only rewrite diagnostic macros/imports and
instrumentation; discarded token trees remain reviewable but are never
expanded/evaluated. Manifest adjustments keep the fork resolvable and lintable
as a dependency in the containing workspace.
"""
import hashlib
import io
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parents[1]
PACKAGES = {
    "mssql-tds-0.3.0": "c7a53513324677e601ca5b91e477840329ec6c244875fd55b155e5a8e8af7805",
    "native-tls-0.2.18": "465500e14ea162429d264d44189adc38b199b62b1c21eea9f69e4b73cb03bbf2",
    "tokio-native-tls-0.3.1": "bbae76ab933c85776efabc971569dd6119c580d8f5d448769dec1764bf796ef2",
}
EDITIONS = {
    "mssql-tds-0.3.0": "2024",
    "native-tls-0.2.18": "2021",
    "tokio-native-tls-0.3.1": "2018",
}


def quiet(source):
    source = re.sub(
        r"\b(?:tracing::|log::)?(?:trace|debug|info|warn|error|event)!(?=\s*[(\[{])",
        "quiet!",
        source,
    )
    # Instrument attribute arguments contain balanced parentheses, not brackets.
    source = re.sub(r"#\[(?:tracing::)?instrument(?:\([^]]*\))?\]", "", source)
    source = re.sub(
        r"use (?:tracing|log)::(?:\{[^}]*\}|(?:trace|debug|info|warn|error|event|instrument));",
        "",
        source,
    )
    return source


def main():
    output = Path(sys.argv[2]) if len(sys.argv) > 2 else ROOT
    for package, checksum in PACKAGES.items():
        data = (Path(sys.argv[1]) / f"{package}.crate").read_bytes()
        assert hashlib.sha256(data).hexdigest() == checksum, package
        destination = output / "vendor" / package
        assert not destination.exists(), f"remove {destination} before regenerating"
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            for member in archive.getmembers():
                relative = Path(member.name).relative_to(package)
                assert ".." not in relative.parts and not relative.is_absolute()
                assert member.isfile() or member.isdir()
                if member.isdir():
                    continue
                target = destination / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                content = archive.extractfile(member).read()
                if target.suffix == ".rs":
                    content = quiet(content.decode()).encode()
                target.write_bytes(content)
        lib = destination / "src/lib.rs"
        text = lib.read_text()
        # Removing diagnostics leaves log-only imports/locals in upstream code.
        # Preserve them to avoid mixing a protocol refactor into this patch.
        prefix = (
            "// Quiet fork: log-only locals/imports intentionally retained.\n"
            "#![allow(unused_imports, unused_variables, unused_assignments, unused_macros, dead_code)]\n"
        )
        # Inner docs must precede items, so put macro after the last //! line.
        lines = text.splitlines(keepends=True)
        first_item = next(i for i, line in enumerate(lines) if re.match(r"(?:pub mod |use )", line))
        lines.insert(first_item, "\nmacro_rules! quiet { ($($ignored:tt)*) => {{}}; }\n")
        lib.write_text(prefix + "".join(lines))
        if package.startswith("mssql-tds"):
            with lib.open("a") as handle:
                handle.write(
                    '\n/// Regression probe: discarded diagnostic arguments cannot run.\n'
                    '#[cfg(feature = "test-util")]\n'
                    'pub fn quiet_arguments_regression_probe() {\n'
                    '    quiet!(panic!("discarded diagnostic argument evaluated"));\n'
                    '}\n'
                )
        manifest = destination / "Cargo.toml"
        text = manifest.read_text()
        if package.startswith("mssql-tds"):
            # Cargo ignores package-level resolvers and warns when both
            # equivalent SPDX and license-file metadata are present.
            text = text.replace('resolver = "2"\n', "").replace(
                'license-file = "LICENSE"\n', ""
            )
        if package.startswith("native-tls"):
            # Keep strict workspace clippy clean without changing this
            # third-party implementation for a non-semantic upstream idiom.
            text += (
                '\n[lints.clippy]\n'
                'unnecessary_lazy_evaluations = "allow"\n'
            )
        text = re.sub(
            r"\[(?:target\.[^\n]+?\.)?dependencies\.(?:tracing|log)\]\n.*?(?=\n\[)",
            "",
            text,
            flags=re.S,
        )
        if package.startswith("mssql-tds"):
            text = text.replace(
                '[dependencies.native-tls]\n',
                '[dependencies.native-tls]\npath = "../native-tls-0.2.18"\n',
            ).replace(
                '[dependencies.tokio-native-tls]\n',
                '[dependencies.tokio-native-tls]\npath = "../tokio-native-tls-0.3.1"\n',
            )
        if package.startswith("tokio-native-tls"):
            text = text.replace(
                '[dependencies.native-tls]\n',
                '[dependencies.native-tls]\npath = "../native-tls-0.2.18"\n',
            )
            # This vendor is a dependency, not a test workspace. The upstream
            # Apple-only dev dependency is yanked and irrelevant to the
            # Linux-only source audit/live lane; retaining it makes Cargo try
            # to resolve the vendor's own tests as workspace targets.
            text = text.replace(
                '[target."cfg(any(target_os = \\"macos\\", target_os = \\"ios\\"))".dev-dependencies.security-framework]\n'
                'version = "0.2"\n',
                "",
            )
        text = re.sub(r"\n{3,}", "\n\n", text).rstrip() + "\n"
        manifest.write_text(text)

    # Format each packaged Rust file, including cfg-specific and non-target
    # sources, with the repository's pinned toolchain and configuration.
    # skip_children avoids interpreting standalone module files as crate roots;
    # every file is explicitly visited instead.
    toolchain = tomllib.loads((WORKSPACE / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    for package, edition in EDITIONS.items():
        for source in sorted((output / "vendor" / package).rglob("*.rs")):
            subprocess.run(
                [
                    "rustup", "run", toolchain, "rustfmt",
                    "--edition", edition,
                    "--config-path", str(WORKSPACE / "rustfmt.toml"),
                    "--config", "skip_children=true",
                    str(source),
                ],
                check=True,
            )


if __name__ == "__main__":
    main()
