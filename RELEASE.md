# Releasing

Publishing runs from `release.yml`. It is driven by a tag and authenticates with
a short-lived crates.io token obtained over OIDC. There is no
`CARGO_REGISTRY_TOKEN` secret in this repository.

## The version rule

Every publishable crate carries the same literal version. The tag is `v` plus
that version. Tag `v0.3.1` publishes only if every manifest says
`version = "0.3.1"`; the `version` job fails the run otherwise. The first
version in this repository is `0.0.0-alpha.0`.

The version appears twice per manifest: once as `[package] version`, and once in
every internal path dependency as an exact requirement
(`version = "=X"`). It also appears in dev-dependencies and target-specific
dependency tables. All of them must move together, including
`neotales-dbapi-mssql-direct`, which is `publish = false` but is still a
workspace member whose exact requirements must resolve locally. Do not edit only
the 23 published manifests: the unpublished member would then fail to build
against the new local versions.

Run the checked operation instead of editing by hand:

```sh
python3 eng/version-consistency.py --verify
python3 eng/version-consistency.py --set 0.3.1
cargo build --workspace
git diff --stat
```

`--verify` fails if any workspace package disagrees or any internal exact
requirement is not `=X`. `--set` rewrites both forms in all 24 member manifests.
It deliberately does not touch `neotales-resilience`: that is an external crate
published from `neotales/rs-retry`, not a path dependency in this repository.

Commit the version change and the updated `Cargo.lock`, push, then tag that
commit:

```sh
git tag v0.3.1
git push origin dev
git push origin v0.3.1
```

The one exception is a new crate's first version, which is uploaded by hand and
never from a tag. That is the next section.

## Publishable crates and order

`neotales-dbapi-mssql-direct` is `publish = false` and is deliberately absent.
The other 23 crates are published one at a time, in this order. A crate can only
be packaged after every internal dependency above it exists on crates.io at the
same version.

| Order | Crate | Directory | Internal dependencies |
| ---: | --- | --- | --- |
| 1 | `neotales-klickhouse` | `klickhouse/` | none |
| 2 | `neotales-libsql` | `libsql/` | none |
| 3 | `neotales-tokio-postgres` | `tokio-postgres/` | none |
| 4 | `neotales-dbapi` | `dbapi/` | `neotales-resilience` (registry) |
| 5 | `neotales-migration` | `migration/` | none |
| 6 | `neotales-sqlx-mssql` | `sqlx-mssql/` | none |
| 7 | `neotales-dbapi-d1` | `dbapi-d1/` | `neotales-dbapi` |
| 8 | `neotales-dbapi-sqlite` | `dbapi-sqlite/` | `neotales-dbapi` |
| 9 | `neotales-dbapi-sqlite-direct` | `dbapi-sqlite-direct/` | `neotales-dbapi` |
| 10 | `neotales-dbapi-libsql` | `dbapi-libsql/` | `neotales-dbapi`, `neotales-libsql` |
| 11 | `neotales-dbapi-mysql` | `dbapi-mysql/` | `neotales-dbapi` |
| 12 | `neotales-dbapi-mariadb` | `dbapi-mariadb/` | `neotales-dbapi`, `neotales-dbapi-mysql` |
| 13 | `neotales-dbapi-mssql` | `dbapi-mssql/` | `neotales-dbapi`, `neotales-sqlx-mssql` |
| 14 | `neotales-dbapi-clickhouse` | `dbapi-clickhouse/` | `neotales-dbapi`, `neotales-klickhouse` |
| 15 | `neotales-dbapi-postgres-tls` | `dbapi-postgres-tls/` | `neotales-tokio-postgres` |
| 16 | `neotales-dbapi-postgres` | `dbapi-postgres/` | `neotales-dbapi`, `neotales-tokio-postgres`, `neotales-dbapi-postgres-tls` |
| 17 | `neotales-migration-sqlite` | `migration-sqlite/` | `neotales-migration`, `neotales-dbapi`, `neotales-dbapi-sqlite`, `neotales-dbapi-sqlite-direct` |
| 18 | `neotales-migration-libsql` | `migration-libsql/` | `neotales-migration`, `neotales-dbapi`, `neotales-dbapi-libsql` |
| 19 | `neotales-migration-mysql` | `migration-mysql/` | `neotales-migration`, `neotales-dbapi`, `neotales-dbapi-mysql` |
| 20 | `neotales-migration-mariadb` | `migration-mariadb/` | `neotales-migration`, `neotales-dbapi`, `neotales-dbapi-mariadb` |
| 21 | `neotales-migration-mssql` | `migration-mssql/` | `neotales-migration`, `neotales-dbapi`, `neotales-dbapi-mssql` |
| 22 | `neotales-migration-clickhouse` | `migration-clickhouse/` | `neotales-migration`, `neotales-dbapi`, `neotales-dbapi-clickhouse` |
| 23 | `neotales-migration-postgres` | `migration-postgres/` | `neotales-migration`, `neotales-dbapi`; provider is a dev-dependency |

Internal path dependencies carry an exact version requirement
(`=0.0.0-alpha.0`). Cargo rewrites the path to that registry requirement when
packaging. A path without a version is rejected before packaging, so do not
remove the version to get past a local failure.

`neotales-dbapi` depends on the published `neotales-resilience` from
`neotales/rs-retry`, not on a copy in this repository. That crate's current
published version is `0.0.0-alpha.0`.

## First publish of a new crate

crates.io trusted publishing cannot create a brand-new crate. The OIDC exchange
needs a crate that exists and a trusted publisher configured on it, so a cold
start over OIDC is rejected by crates.io. Do not try to build one.

The first upload of each crate is manual, in the table order, through
`~/work/rs/publish.sh <dir>`. That script lives beside this folder and is
deliberately not copied into the repository: it is how the first version of
every crate is uploaded, not part of any one crate.

1. Confirm every manifest says `version = "0.0.0-alpha.0"` and the working tree
   is clean. Do not publish from a tree with uncommitted changes.
2. Upload each crate in order:

   ```sh
   ~/work/rs/publish.sh klickhouse
   ~/work/rs/publish.sh libsql
   ~/work/rs/publish.sh tokio-postgres
   ~/work/rs/publish.sh dbapi
   ~/work/rs/publish.sh migration
   ~/work/rs/publish.sh sqlx-mssql
   ~/work/rs/publish.sh dbapi-d1
   ~/work/rs/publish.sh dbapi-sqlite
   ~/work/rs/publish.sh dbapi-sqlite-direct
   ~/work/rs/publish.sh dbapi-libsql
   ~/work/rs/publish.sh dbapi-mysql
   ~/work/rs/publish.sh dbapi-mariadb
   ~/work/rs/publish.sh dbapi-mssql
   ~/work/rs/publish.sh dbapi-clickhouse
   ~/work/rs/publish.sh dbapi-postgres-tls
   ~/work/rs/publish.sh dbapi-postgres
   ~/work/rs/publish.sh migration-sqlite
   ~/work/rs/publish.sh migration-libsql
   ~/work/rs/publish.sh migration-mysql
   ~/work/rs/publish.sh migration-mariadb
   ~/work/rs/publish.sh migration-mssql
   ~/work/rs/publish.sh migration-clickhouse
   ~/work/rs/publish.sh migration-postgres
   ```

   The script reads the token itself and passes it to `cargo` through the
   environment. Do not put a token on the command line, in the repository, or in
   shell history.
3. Add the trusted publisher on crates.io for every crate, in each crate's
   settings: owner `neotales`, repository `rs-db`, workflow filename
   `release.yml`, and an empty environment. Leave the environment field empty.
   The `publish` job sets no environment, and crates.io compares the two fields.
   An environment name on one side only fails the token exchange with no other
   symptom.
4. Bump to the next version and let tags take over. Do not run `publish.sh` for
   these crates again.

The first tagged run after the manual uploads does not fail on the versions
already published. Before each upload the publish job asks crates.io whether
that exact version exists and skips it when it does; a tag whose 23 versions are
all present therefore completes as a no-op. The tag is provenance for the first
version, not its publish trigger. Every later version publishes from its `v*`
tag.

Before the first upload, `cargo publish --dry-run` succeeds only for crates whose
internal dependencies are all already on the registry. In this repository that
is `neotales-klickhouse`, `neotales-libsql`, `neotales-tokio-postgres`,
`neotales-dbapi`, `neotales-migration` and `neotales-sqlx-mssql`. Every other
crate fails with an exact registry-resolution error such as:

```text
no matching package named `neotales-dbapi` found
location searched: crates.io index
```

That is the dependency rule, not a packaging fault. The `check` job accepts only
that expected failure: the missing package must be a workspace sibling whose
version equals the crate under test, the cause must be the crates.io index, and
the failure must be a resolution failure. An unpublished external crate, a
wrong internal requirement, a manifest error, or a tarball verification failure
fails the job. The same classifier is tested on every CI run by
`python3 eng/package-preflight.py --self-test`, including the negative cases.
The publish job uploads in dependency order and retries only the same
verified index-lag condition, plus HTTP 429. Any other failure stops the run at
that crate, so a real error cannot be hidden behind 20 retries.

Registry-name availability was unverified at import time. A read-only crates.io
API check on 2026-11-21 returned `404` for all 23 published names, so they were
free at that moment. That is not a reservation: recheck immediately before the
first upload. If a name is taken by someone else, stop and rename the package
everywhere rather than publish under an unowned name.

## Later releases

1. Set the new version in every workspace member manifest, including every
   internal exact dependency requirement:

   ```sh
   python3 eng/version-consistency.py --set X.Y.Z
   python3 eng/version-consistency.py --verify
   cargo build --workspace
   ```

   `Cargo.lock` is committed on purpose, so commit the updated lockfile with the
   manifests. The lockfile is what the scheduled audit checks. Do not change the
   external `neotales-resilience` pin as part of this repository's bump.
2. Push the commit.
3. Tag it `vX.Y.Z` and push the tag.
4. The workflow runs the checks and dry runs, then publishes in order. One OIDC
   exchange covers every crate whose trusted publisher matches this repository,
   `release.yml` and an empty environment.
5. A re-run skips a version already on crates.io. Do not republish it, and do not
   delete or move the tag.

A manual run, `workflow_dispatch`, runs the checks and dry runs only. It never
publishes, whatever branch it is started from.

After the manual first upload is done, switch each crate to Trusted Publishing
Only mode on crates.io. That rejects token publishes everywhere, so a token
cannot publish these crates later.

### Partial runs and retries

Publishing 23 crates is not atomic. The job publishes in order and stops at the
first crate that fails. That leaves a prefix of the version on crates.io. A
re-run is the supported recovery: it skips the uploaded prefix and starts again
at the failed crate. The intended sequence is always

```text
check -> version -> publish in order -> skip existing -> retry index lag -> stop on a real error
```

Do not bump the version because a run was interrupted. Bump only when the
content already uploaded is wrong, or when a crate is structurally unpublishable
at that version; then the whole workspace moves to a new version and the old
partial set stays as an immutable record.

Two failure classes need different handling:

- **Not uploaded.** The job reports a normal `cargo publish` error. Fix the
  cause, then re-run the job. No version bump is involved.
- **Uploaded, but the run failed later.** The version exists on crates.io and
  cannot be replaced. Re-run; the job sees it and skips it. If the uploaded
  bytes are wrong, bump the version for every crate and release again.

## What is not allowed

- `pull_request_target` and `workflow_run` are not used anywhere, and must not be
  added. Both can run untrusted code with publishing credentials in scope.
- No long-lived crates.io token in GitHub secrets for this workflow. The publish
  job reads `id-token: write` and nothing else.
- No publishing from a branch. The tag check exists so the artifact has a name.
- No republishing a version. crates.io rejects it. A re-run of the workflow
  skips a version that is already present; if the uploaded content itself was
  wrong, bump the shared version and release it as a new version.
- No `[patch]`, local registry or unversioned path dependency to make a packaging
  check pass. It would not describe what crates.io receives.

## Failure modes

| Symptom | Cause |
| --- | --- |
| `version` job: tag does not match `vX.Y.Z` | A manifest version and the tag disagree, or the manifests disagree. Delete the tag, run `eng/version-consistency.py --set`, fix, re-tag. |
| `version` job: internal requirement mismatch | `eng/version-consistency.py --verify` found a path dependency whose exact version differs from the package version. Run `--set` and commit every manifest. |
| `check` job: dry run fails with `no matching package named` | An internal dependency is not on crates.io yet. Expected before the first upload and on a releasing commit; the publish job handles it. The classifier rejects an unpublished external crate, a wrong requirement version, or a non-index cause. |
| Publish job: OIDC exchange fails | The crate does not exist yet, or the trusted publisher does not match: wrong repository, wrong workflow filename, or an environment name set on one side only. |
| Publish job: version already uploaded | The version was published before, usually by the manual first release or an earlier partial run. The job skips it. Do not bump the version unless the uploaded content is wrong. |
| Publish job: `cargo publish` rejects the token | Trusted Publishing Only mode is on and the crate has no trusted publisher configured. |
| Publish job: cannot select a sibling version | Index lag after the previous upload. The job retries for a few minutes. If it still fails, confirm the version is on crates.io and re-run the job. A re-run skips versions already uploaded. Do not publish that version by hand again. |
