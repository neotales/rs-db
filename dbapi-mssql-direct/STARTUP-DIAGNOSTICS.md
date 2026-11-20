# SQL Server fixture startup diagnosis

The expired-certificate fixture has intermittently hit testcontainers'
90-second startup timeout. A successful later run does not identify the cause.
Do not change the bound, retry the fixture, or weaken TLS on that evidence.

## What the current evidence cannot establish

Testcontainers 0.28.0 creates the container and copies fixture files before it
starts its startup timeout. The timeout wraps the Docker start request,
container initialization and the configured ready-log wait. A stopwatch
started before `.start()` includes work outside that interval. In particular,
the observed 98.34 seconds from the beginning of a diagnostic attempt to the
ready marker is **not** proof that the 90-second timeout was insufficient.

The public `Image::exec_before_ready` hook is after the Docker start response
and after an inspection request. It cannot time the start request itself. The
public runner does not expose a callback immediately before that request, and
its startup timeout can drop the container before a caller can inspect it.

## Next controlled experiment

The test-only Bollard runner is now implemented in `tests/support/startup.rs`.
Run only when assigned the exclusive Docker lane, from `rs/lib`:

```sh
cargo test --locked -p neotales-dbapi-mssql-direct --lib \
  live::startup::expired_fixture_start_boundary -- --ignored --exact --nocapture
```

It requires the pinned image to be present already; it neither pulls nor
retries. It shares the TLS fixture's image, shell command and ready marker,
copies the same files/modes, generates fresh expired TLS material and a
disposable password, and publishes no ports. It does not run the TLS assertions
or replace the normal live lane. It uses Bollard's local Docker transport
(on Unix, a `unix://` `DOCKER_HOST` is honored); TCP/SSH Docker contexts are
not supported.

The runner retains the fixture through the 90-second observation even if the
marker arrives early. Otherwise it observes for at most 180 seconds after start
dispatch. Start dispatch and response completion use the same monotonic clock
inside the task issuing the actual API request. Marker polling is bounded to
512 KiB per read and three seconds; only marker presence and the observation
interval are printed, never logs. The boundary timer/snapshot and marker polling
are independently polled sibling futures: even a stalled log response cannot
postpone dispatch of the 90-second inspection. The boundary wake time and each
inspection's request/response times are printed relative to start dispatch.
They expose scheduler/transport delay, not an exact server-side sampling
timestamp or proof of historical state at precisely 90 seconds. Incomplete
reads are not evidence of absence. Listener reachability is a TCP check,
not a successful SQL login.

Create/upload each have a 30-second transport bound. The supervised start
request has a separate 210-second bound and is joined even after cancellation
or the observation deadline. Label-checked, ID-targeted forced removal (including
anonymous volumes) and an absence check share a 30-second cleanup budget.
Cancellation does not shut down the runtime before the tracked supervisor
drains. Cancellation of the entire process or an unavailable Docker daemon
cannot guarantee removal: the printed unique name is the reconciliation key.
In particular, absence after an unacknowledged create is explicitly
**inconclusive**, not successful cleanup; a late daemon operation is possible.
Never remove a container whose ownership label does not match that name.
Docker errors are reported only as fixed categories, not response bodies.

Non-Docker tests use a loopback fake Engine through the real Bollard HTTP
transport to cover delayed create/start cancellation, the independent boundary
snapshot (including a log response held across the boundary), lost create
responses, foreign labels and inconclusive absence.
These do not establish the reason for the intermittent SQL Server startup.

When the Docker lane is exclusively assigned, use a disposable, uniquely
labeled diagnostic fixture with the same image digest, command, copied files,
environment keys and ready marker as the expired fixture. Instrument an
isolated test-only copy of the runner, or an equivalent diagnostic runner, at
these boundaries:

1. Immediately before dispatching the Docker start request, record monotonic
   time. Preserve the unique container ID privately; do not print its
   configuration or environment.
2. Record start-response completion and first observation of the exact ready
   marker. Classify logs in memory and output only marker presence and elapsed
   times, never log contents.
3. At 90 seconds after start dispatch, retain the container and record only
   sanitized running/exited status, exit code, OOM flag, health category,
   listener reachability and resource/IO aggregates. If still running and not
   ready, observe until a separately bounded diagnostic deadline, then record
   the same categories.
4. Always stop and remove only that uniquely labeled container, including
   startup error, timeout and cancellation paths. Verify removal.

Measure with the same monotonic clock around the actual start call; a Docker
started event or `StartedAt` is after dispatch and is only a lower bound on
time already consumed. A ready marker observed more than 90 seconds after that
event proves the configured interval was exceeded, but the converse does not
prove it was met. Preserve timestamp uncertainty when reporting polling-based
marker observations.

Only a reproduced observation that isolates the timeout interval can justify
adjusting a startup budget. Any proposed change must also recalculate the
sequential TLS scenario deadline and both CI job budgets, and retain bounded
cleanup and all existing TLS assertions. A crash, OOM kill, unavailable
listener, missing marker or ambiguous start timing requires its own diagnosis;
none is evidence to increase the timeout.

The independent migration waiter lifecycle trace is emitted by the live lane
on completion or cancellation. Preserve it if the live lane is subsequently
scheduled; a single passing run does not explain earlier lock or reconciliation
timeouts.
