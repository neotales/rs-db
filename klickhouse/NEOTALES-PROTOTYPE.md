# Vendored native-protocol fork

Source: crates.io `klickhouse` 0.16.0, archive SHA-256
`9ae09879df1d2f379f5d4370cc508878cc42c878534bfe127788b5c46835dd4a`.
The MIT and Apache-2.0 license files are from the upstream v0.16.0 tag.

Local changes expose the native EndOfStream packet, make the existing block stream
reject channel closure without that packet, propagate active read errors, bind insert
blocks to query IDs, poison the connection on an abandoned active insert, and make
single-row helpers drain to the terminal packet. Local typed-insert serialization
errors report an unknown insert outcome rather than skip rows. The no-compression
build is also fixed. This fork is the public provider's only transport. Raw
event consumers must still reject closure without EndOfStream and discard
provisional data. The experimental wire implementation advertises revision 54459 and
negotiates the lesser of that and the server revision. Its revision-boundary
fields, including empty parameters, have synthetic coverage, and an unparameterized
select, DDL and insert have been exercised against the pinned 24.8 image.
Custom column serialization is rejected; profile-event blocks are parsed and
discarded. Totals and extremes packets are rejected rather than silently omitted.
Other server versions and protocol features are not comprehensively
validated. Experimental `query_bound` supports `Bool`, `Int64`, text and bytes
(up to 8192 bytes per value), and only the `Float64` values ±0 and ±1. It requires
distinct `ntp_`-prefixed names, exact matching typed placeholders and no quoted
SQL, comments (including `//`), dollar signs or semicolons. It sends numeric
values as ClickHouse 24.8 custom Settings `Field` string dumps and text/bytes as
ASCII double-escaped hex, never rewriting SQL. Null, unsigned integer, all other
Float64 values and older protocol revisions are rejected before dispatch. A live
test found 24.8's default float parser rounded `f64::MAX` to infinity, so general
finite floats must not be enabled without an enforceable exact parsing guarantee.
This limited subset is tested only against the pinned 24.8 image; it is not a general SQL
parser or a claim of compatibility with other server versions. Do not use
`QueryBuilder` as a binding substitute; it rewrites SQL.

Nonempty query settings are rejected before any query bytes are sent when the
negotiated revision is below 54429, instead of being silently omitted.
OpenTelemetry settings are also rejected if unsupported or malformed. Hello and
ClientInfo use the same client name; the live 24.8 test enables and verifies
`validate_tcp_client_information`. Query dispatch acknowledgements distinguish a
confirmed pre-dispatch rejection from loss during dispatch; raw and typed inserts
return the former directly and classify the latter as an unknown outcome.
When an active query fails, queued requests receive `QueryNotDispatched`; this
also applies when a mailbox reservation outlives shutdown. An error while
sending the active query returns `QueryOutcomeUnknown` with the original cause.
The packet parser retains partially read packets while processing requests.
Dropping a bound or unbound query future before dispatch marks it cancelled.
Dropping an unfinished query event stream or interrupting dispatch aborts the
connection; server-side effects may already have happened or still be running.
During initialization the task also watches the request mailbox, so dropping the
Client while a silent peer withholds the native hello releases the task and its
socket.
Server exceptions also close the connection instead of dispatching queued work.
External timeout callers must classify a potentially dispatched write as unknown;
dropping a future cannot itself return an outcome to the caller.
After a query event stream has been returned, transport, protocol, decoding and
premature-EOF errors are wrapped as `QueryOutcomeUnknown` with their original
cause. Server exceptions remain distinguishable for diagnostics but do **not**
prove that a write had no partial effects: an adapter must classify them as an
unknown write outcome unless it can establish otherwise.

Decode hardening is a **checkpoint, not an end-to-end allocation proof**. Native
compression is disabled; blocks reject more than 64 columns, 4096 rows or 16384
cells, unsupported/nested types, and custom serialization. Block decoding charges
cell storage, a fixed per-column metadata allowance and string payloads against
a 2 MiB estimate; individual strings are capped at 64 KiB and column names/types
at 256/64 bytes. Hello and exception strings use the individual cap; table-status
and part-UUID packets are rejected. The event channel holds at most 32 blocks,
plus an in-flight block. This is not a measured allocator-wide budget: container
overhead and copies are estimated, other concurrent connections multiply it, and
`query_collect` and caller-owned streams can accumulate unbounded total results.
Outbound queries, request queues, native TLS internals and direct non-wire codec
APIs do not have an end-to-end shared allocation budget. Do not claim bounded
process memory. The outer connection controller interrupts blocked
I/O and event delivery on an active insert abort, drops the transport and pending
requests, and preserves a failure after queued provisional events. Insert errors
after dispatch are conservatively reported as unknown outcomes; other write paths
and cancellation boundaries still require an end-to-end dbapi audit.
Nested server exceptions are rejected as protocol errors before their unread
payload can be mistaken for another packet; the connection is closed.
The `connect_tls_verified` helper verifies the supplied trust roots and server name;
an in-memory-certificate TLS proxy test against the pinned server covers good and
bad CA/name cases. The upstream `connect_tls` still accepts a caller-supplied
connector with an unconstrained policy. Direct server TLS and client certificate
authentication are not tested.
The public provider lives in `neotales-dbapi-clickhouse` and uses this fork as
its only transport. It maps Int64 and String result columns; String is mapped
consistently to DBAPI Bytes, including valid UTF-8. Empty result schemas are
validated too. The collector's RowLimit bounds detached row payload, not
process-wide allocations; separate native packet, block and string decode limits
apply. Public-provider TLS, version-gate, timeout and cancellation behavior are
covered by the provider's live suite.
Native progress does not establish affected counts for every write: ClickHouse
24.8's TCPHandler notes regular INSERT VALUES cannot send progress without a
protocol compatibility change. The provider therefore rejects execute before
dispatch instead of manufacturing a count.
