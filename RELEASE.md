# Releasing

Publishing runs from `release.yml`. It is driven by a tag and authenticates with
a short-lived crates.io token obtained over OIDC. There is no
`CARGO_REGISTRY_TOKEN` secret in this repository.

## The version rule

Every publishable crate carries the same literal version. The tag is `v` plus
that version. Tag `v0.3.1` publishes only if every manifest says
`version = "0.3.1"`; the `version` job fails the run otherwise. The first
version in this repository is `0.0.0-alpha.0`.

Bump the version, commit, push, then tag that commit:

```sh
cargo publish -p neotales-klickhouse --dry-run --locked
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
`/home/dev/work/rs/publish.sh <dir>`. That script lives beside this folder and is
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

The release run for the first tagged version fails, because the version it would
publish is already uploaded. The tag is provenance for the first version, not
its publish trigger. Every later version publishes from its `v*` tag.

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
that expected failure and still fails on a compile, manifest or metadata error.
The publish job uploads in dependency order and retries while the index catches
up.

Registry-name availability for the `neotales-*` fork names was unverified at
import time. Check ownership before the first upload. If a name is taken by
someone else, stop: rename the package everywhere rather than publish under an
unowned name.

## Later releases

1. Bump the version in all 23 publishable manifests and commit it. `Cargo.lock`
   is committed on purpose, so run `cargo build` and commit the updated lockfile
   with it. The lockfile is what the scheduled audit checks.
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

## What is not allowed

- `pull_request_target` and `workflow_run` are not used anywhere, and must not be
  added. Both can run untrusted code with publishing credentials in scope.
- No long-lived crates.io token in GitHub secrets for this workflow. The publish
  job reads `id-token: write` and nothing else.
- No publishing from a branch. The tag check exists so the artifact has a name.
- No republishing a version. crates.io rejects it. If a publish fails after the
  upload was accepted, bump the patch version and release again.
- No `[patch]`, local registry or unversioned path dependency to make a packaging
  check pass. It would not describe what crates.io receives.

## Failure modes

| Symptom | Cause |
| --- | --- |
| `version` job: tag does not match `vX.Y.Z` | A manifest version and the tag disagree, or the manifests disagree. Delete the tag, fix, re-tag. |
| `check` job: dry run fails with `no matching package named` | An internal dependency is not on crates.io yet. Expected before the first upload and on a releasing commit; the publish job handles it. Any other dry-run error fails the job. |
| Publish job: OIDC exchange fails | The crate does not exist yet, or the trusted publisher does not match: wrong repository, wrong workflow filename, or an environment name set on one side only. |
| Publish job: version already uploaded | The version was published before, usually by the manual first release. Bump the version. |
| Publish job: `cargo publish` rejects the token | Trusted Publishing Only mode is on and the crate has no trusted publisher configured. |
| Publish job: cannot select a sibling version | Index lag after the previous upload. The job retries for a few minutes. If it still fails, confirm the version is on crates.io and re-run the job. A re-run skips versions already uploaded. Do not publish that version by hand again. |
