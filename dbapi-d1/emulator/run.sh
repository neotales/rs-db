#!/usr/bin/env bash
# Local-only reproducer; run npm ci separately before using this script.
set -euo pipefail
cd "$(dirname "$0")"
: "${TMPDIR:?Set TMPDIR to an owned, disk-guarded directory}"
: "${CARGO_TARGET_DIR:?Set CARGO_TARGET_DIR to an owned, disk-guarded directory}"
[[ "$(wasm-bindgen --version)" == 'wasm-bindgen 0.2.130' ]]
[[ $(df -Pk "$TMPDIR" | awk 'NR==2 {print $4}') -ge 2097152 ]]
[[ $(df -Pk "$CARGO_TARGET_DIR" | awk 'NR==2 {print $4}') -ge 2097152 ]]
test -x node_modules/.bin/workerd
export CARGO_NET_OFFLINE=true
cargo +1.100.0 build --locked --manifest-path worker/Cargo.toml --target wasm32-unknown-unknown
D1_GLUE_DIR=$(mktemp -d "$TMPDIR/d1-glue.XXXXXXXX")
export D1_GLUE_DIR
trap 'rm -rf -- "$D1_GLUE_DIR"' EXIT
wasm-bindgen --target web --out-dir "$D1_GLUE_DIR" "$CARGO_TARGET_DIR/wasm32-unknown-unknown/debug/d1_emulator_worker.wasm"
cat > "$D1_GLUE_DIR/index.js" <<'JS'
import init, {
  probe,
  paging_probe_bigint,
  paging_probe_bounds,
  paging_probe_errors,
  paging_probe_malformed,
  paging_probe_pending,
  paging_probe_sequence,
  paging_probe_values,
} from './d1_emulator_worker.js';
import wasm from './d1_emulator_worker_bg.wasm';

const SEQUENCE_SQL = 'SELECT id, name, payload FROM pages WHERE id > ? ORDER BY id LIMIT ?';

function countedBinding(env) {
  const state = { prepares: 0, raws: 0, sqls: [] };
  return { state, binding: {
    prepare(sql) {
      state.prepares++;
      state.sqls.push(sql);
      const statement = env.DB.prepare(sql);
      const bound = (target) => ({
        raw(options) { state.raws++; return target.raw(options); },
        run() { return target.run(); },
      });
      return {
        bind(...args) { return bound(statement.bind(...args)); },
        raw(options) { state.raws++; return statement.raw(options); },
        run() { return statement.run(); },
      };
    },
    batch(statements) { return env.DB.batch(statements); },
  } };
}

function pendingBinding() {
  const state = { prepares: 0, raws: 0 };
  return { state, binding: {
    prepare() {
      state.prepares++;
      return {
        bind() { return this; },
        raw() { state.raws++; return new Promise(() => {}); },
      };
    },
    batch() { return Promise.reject(new Error('unused')); },
  } };
}

function valueBinding() {
  const state = { prepares: 0, raws: 0 };
  const results = {
    bigint_exact: [['n'], [9007199254740992n]],
    bigint_exact_plus_one: [['n'], [9007199254740993n]],
    bigint_overflow: [['n'], [9223372036854775808n]],
    number_unsafe: [['n'], [9007199254740992]],
    number_rounded: [['n'], [9007199254740993]],
  };
  return { state, binding: {
    prepare(sql) {
      state.prepares++;
      const result = results[sql];
      return {
        bind() { return this; },
        raw() { state.raws++; return Promise.resolve(result); },
      };
    },
    batch() { return Promise.reject(new Error('unused')); },
  } };
}

function malformedBinding() {
  const state = { prepares: 0, raws: 0 };
  const results = {
    missing_header: [],
    empty_header: [[]],
    not_array: null,
    undefined_cell: [['a'], [undefined]],
    hole_cell: [['a'], [,]],
    null_cell: [['a'], [null]],
  };
  return { state, binding: {
    prepare(sql) {
      state.prepares++;
      const result = results[sql];
      return {
        bind() { return this; },
        raw() { state.raws++; return Promise.resolve(result); },
      };
    },
    batch() { return Promise.reject(new Error('unused')); },
  } };
}

function rejectBinding(env) {
  const state = { prepares: 0, raws: 0 };
  return { state, binding: {
    prepare(sql) {
      state.prepares++;
      const statement = env.DB.prepare(sql);
      return {
        bind() { return this; },
        raw() { state.raws++; return Promise.reject(new Error('deliberately uncertain')); },
      };
    },
    batch() { return Promise.reject(new Error('unused')); },
  } };
}

export default { async fetch(request, env) {
  if (typeof env.DB?.prepare !== 'function') throw new Error('D1 binding missing');
  await init({ module_or_path: wasm });
  let dispatches = 0, prepares = 0, batches = 0, realBatches = 0;
  const real = { prepare(sql) { return env.DB.prepare(sql); },
    batch(statements) { realBatches++; return env.DB.batch(statements); } };
  const delayed = { prepare(sql) {
    prepares++;
    const statement = env.DB.prepare(sql);
    return { run() { dispatches++; return dispatches === 1 ? new Promise(() => {}) : Promise.reject(new Error("uncertain")); },
      bind(...args) { return statement.bind(...args); } };
  }, batch() { batches++; return Promise.reject(new Error("uncertain")); } };
  const result = await probe(real, delayed);
  const rows = await env.DB.prepare('SELECT id, v FROM t ORDER BY id').all();
  const types = await env.DB.prepare("SELECT i, f, b, n, hex(payload) AS payload FROM types WHERE i=7").first();
  const numeric = await env.DB.prepare("SELECT i, f, typeof(i) AS it, typeof(f) AS ft, i+f AS sum FROM types ORDER BY i").all();
  const numbers = await env.DB.prepare('SELECT CAST(1 AS REAL) AS real, CAST(1 AS INTEGER) AS integer').first();
  const edges = await env.DB.prepare('SELECT n, n = -9007199254740991 OR n = 9007199254740991 AS exact FROM edges ORDER BY n').all();
  if (dispatches !== 2 || prepares !== 3 || batches !== 1 || realBatches !== 2 || JSON.stringify(rows.results) !== JSON.stringify([{id:1,v:'hello'}]))
    throw new Error('unexpected dispatch or database state');
  if (JSON.stringify(edges.results) !== JSON.stringify([{n:-9007199254740991,exact:1},{n:9007199254740991,exact:1}]))
    throw new Error('endpoint precision or unexpected batch write: ' + JSON.stringify(edges.results));
  if (JSON.stringify(types) !== JSON.stringify({i:7,f:1.25,b:1,n:null,payload:'0102FF'}))
    throw new Error('unexpected bound types');
  if (JSON.stringify(numeric.results) !== JSON.stringify([{i:-7,f:-1.25,it:'real',ft:'real',sum:-8.25},{i:7,f:1.25,it:'real',ft:'real',sum:8.25}]))
    throw new Error('numeric storage class or arithmetic mismatch: ' + JSON.stringify(numeric.results));
  if (typeof numbers.real !== 'number' || numbers.real !== numbers.integer)
    throw new Error('unexpected D1 numeric representation');

  const sequence = countedBinding(env);
  if (await paging_probe_sequence(env.DB, sequence.binding) !== 'paging-sequence')
    throw new Error('paging sequence result mismatch');
  if (sequence.state.prepares !== 4 || sequence.state.raws !== 4)
    throw new Error('paging sequence dispatched ' + JSON.stringify(sequence.state));
  if (!sequence.state.sqls.every((sql) => sql === SEQUENCE_SQL))
    throw new Error('paging sequence rewrote its SQL: ' + JSON.stringify(sequence.state.sqls));

  const bounds = countedBinding(env);
  if (await paging_probe_bounds(bounds.binding) !== 'paging-bounds')
    throw new Error('paging bounds result mismatch');
  if (bounds.state.prepares !== 4 || bounds.state.raws !== 4)
    throw new Error('paging bounds dispatched ' + JSON.stringify(bounds.state));

  const errors = countedBinding(env);
  const reject = rejectBinding(env);
  if (await paging_probe_errors(errors.binding, reject.binding) !== 'paging-errors')
    throw new Error('paging errors result mismatch');
  if (errors.state.prepares !== 1 || errors.state.raws !== 1)
    throw new Error('paging errors dispatched ' + JSON.stringify(errors.state));
  if (reject.state.prepares !== 1 || reject.state.raws !== 1)
    throw new Error('paging rejected binding dispatched ' + JSON.stringify(reject.state));

  const pending = pendingBinding();
  if (await paging_probe_pending(pending.binding) !== 'paging-pending')
    throw new Error('paging pending result mismatch');
  if (pending.state.prepares !== 1 || pending.state.raws !== 1)
    throw new Error('paging pending dispatched ' + JSON.stringify(pending.state));
  const live = await env.DB.prepare('SELECT count(*) AS n FROM pages').first();
  if (!live || live.n !== 5)
    throw new Error('binding state changed after a dropped pending stream: ' + JSON.stringify(live));

  if (await paging_probe_values(env.DB) !== 'paging-values')
    throw new Error('paging values result mismatch');

  const values = valueBinding();
  if (await paging_probe_bigint(values.binding) !== 'paging-bigint')
    throw new Error('paging bigint result mismatch');
  if (values.state.prepares !== 5 || values.state.raws !== 5)
    throw new Error('paging bigint dispatched ' + JSON.stringify(values.state));

  const malformed = malformedBinding();
  if (await paging_probe_malformed(malformed.binding) !== 'paging-malformed')
    throw new Error('paging malformed result mismatch');
  if (malformed.state.prepares !== 6 || malformed.state.raws !== 6)
    throw new Error('paging malformed dispatched ' + JSON.stringify(malformed.state));

  return new Response(result);
} };

JS
# A fresh network namespace with only loopback makes accidental remote access impossible.
unshare -Urn sh -c 'ip link set lo up; exec node test.mjs'
