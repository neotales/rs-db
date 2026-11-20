# Local D1 binding diagnostic (not hosted conformance)

This builds Rust Wasm and exercises the actual local Miniflare D1 binding through the
explicit uncertain write API. It checks positional values, float and large-integer rejection,
SQLite storage classes and arithmetic, rollback, and that all DBAPI pool operations fail before
dispatch. A successful real batch checks both safe integer endpoints, their returned changes and
stored values; rejected positive and negative out-of-range batch values must not dispatch or write
an earlier statement. A deterministic binding double tests dropping before poll, dropping after dispatch,
rejected promises, reacquisition, clones and batches. The double demonstrates that uncertain
operations may continue or be followed by more operations; it does not prove cancellation or
binding isolation. The separate D1-specific uncertain API has no DBAPI session guarantee.

The paging probes exercise the D1-only requery API through instrumented bindings: a keyset
sequence with typed bindings, page/row/byte bounds, an empty page, a placeholder mismatch before
dispatch, a missing table, a rejected promise, a promise that never settles, malformed raw results,
and an actual null cell. The malformed double omits the header, supplies an empty header, returns a
non-array, and supplies an `undefined` cell and an array hole; all are `Decode`. A plain `null` cell
is preserved as a SQL null. An empty `SELECT` is also checked through the real binding: the pinned
binding returns the column header even for zero rows, so it yields an empty page; a result without
that header is only produced by the malformed double. The sequence's final non-empty page is short
and the follow-up request is empty, which ends it; the wrappers assert
that the one fixed SQL is prepared once per page and never rewritten, that no request follows an
error, and that no request follows a dropped stream. The pending probe only demonstrates that
dropping does not dispatch again; it does not prove cancellation, and the promise may still settle
later. Raw decoding checks BLOB bytes, a non-integral float, a safe integer, unsafe-integer
rejection, and positional column order with integer-named aliases. A synthetic raw result supplies
`9007199254740992n`, `9007199254740993n`, an out-of-range BigInt, and non-safe Number literals, so
the JavaScript-aware decoder is exercised in wasm: an in-range BigInt is accepted exactly, an
out-of-range BigInt is rejected, and a Number past the safe boundary is rejected rather than
rounded. The local binding returns a BLOB as a plain JavaScript number array and an infinite `REAL`
as SQL NULL; the decoder handles the former and does not claim to recover the latter. A hosted
binding may represent either differently.

Integral float parameters, including signed zero, are rejected before dispatch. The local D1
binding stored integer parameters as REAL in columns with no affinity; SQL affinity can change
the result. Do not rely on an exact SQL storage class. The binding represents `CAST(1 AS REAL)`
and `CAST(1 AS INTEGER)` as equal JavaScript numbers. `numeric_to_f64` is a fallible explicit
conversion; it cannot recover type or precision lost before Rust sees a number. A green local
run does not establish hosted behavior.

Install dependencies separately, with network access, using `npm ci --ignore-scripts
--no-audit --no-fund` in this directory. The lock pins Miniflare and its transitive packages.
Then, on Linux with unprivileged user/network namespaces, `ip`, Rust 1.98.1 including the
wasm32-unknown-unknown target, and wasm-bindgen-cli 0.2.130:

```sh
mkdir -p /path/to/owned/tmp /path/to/owned/target
TMPDIR=/path/to/owned/tmp CARGO_TARGET_DIR=/path/to/owned/target ./run.sh
```

The test runs inside a fresh network namespace with only loopback enabled, sets `cf: false`,
uses ephemeral D1 state, and does not contact a Cloudflare account. The separate `npm ci`
step may access the npm registry; Cargo build runs offline and requires the locked fixture
crates to have been fetched beforehand. Two GiB free space is checked on both designated
filesystems. The harness removes its generated glue on exit. Miniflare's
[D1 documentation](https://developers.cloudflare.com/workers/testing/miniflare/storage/d1/)
describes the binding setup.
