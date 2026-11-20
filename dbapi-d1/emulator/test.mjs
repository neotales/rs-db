import assert from 'node:assert/strict';
import { Miniflare } from 'miniflare';

const root = process.env.D1_GLUE_DIR;
assert.ok(root, 'D1_GLUE_DIR must point to generated Worker modules');
const mf = new Miniflare({
  modules: true,
  modulesRoot: root,
  modulesRules: [
    { type: 'ESModule', include: ['**/*.js'], fallthrough: true },
    { type: 'CompiledWasm', include: ['**/*.wasm'] },
  ],
  scriptPath: `${root}/index.js`,
  compatibilityDate: '2025-10-01',
  host: '127.0.0.1',
  cf: false,
  d1Databases: { DB: '00000000-0000-0000-0000-000000000001' },
  d1Persist: false,
});
try {
  const response = await mf.dispatchFetch('http://localhost/');
  if (response.status !== 200) console.error('Worker failed:', await response.clone().text());
  assert.equal(response.status, 200);
  assert.equal(await response.text(), 'binding-exercised');
  console.log('Actual local D1 binding: uncertain writes, rollback, numeric checks, requery paging and fail-closed DBAPI passed.');
} finally {
  await mf.dispose();
}
