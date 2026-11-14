# AGENTS.md

**[`docs/PLAN.md`](docs/PLAN.md) is the design**, and [`contract/v1/`](contract/v1/) is the
trigger contract. The plan points at the authorities, which win when this repo disagrees with
them: NIP-FB (the schedule and its runner rules), NIP-FA (the AI keys) and Quartz's
`quartz/plans/2026-11-11-observer-site.md` (the Observer site and the Assistant as runner). Fix
the repo, not them.

## What this is

- `runner/`: the Brainstorm Assistants' scheduler, a container beside brainstorm_server. It reads
  the NIP-FB schedules (kind 34817) that name an owner's Assistant as runner, decrypts them with
  the Assistant's key, decides with Quartz, and wakes the task's service (`POST /v1/run`). It
  prints, signs and spends nothing, and writes nothing anywhere: "already started here" is the
  service's `200` answer, kept in memory. Its key reading (Fernet, the key file, `AssistantKeys`
  over four columns of brainstorm_server's `brainstorm_nsec`: `pubkey`, `assistant_pubkey`,
  `encrypted_nsec`, `nsec`, granted column by column, so a server migration that renames or drops
  one breaks it) and the leader lock are its own. Its tests start a throwaway Postgres cluster.
- `contract/v1/`: the JSON both sides test against. A change that breaks a vector is `v2`.

**No code is shared with the task services, in either direction.** Never add a library module
for them, a dependency on their repositories, or a read of their tables. The Observer has its own
copy of the key reading and its own schema; what the two share is only the wake's contract (these
vectors, which the Observer copies into its test resources), the server's four columns and the
Fernet key file. A fix to the key reading is ported by hand to the other side.

The relay reads (`RunnerRelays.kt`) and the SSRF guard (`SafeHttp.kt`) came from
the-nostr-observer's `runner/`. Port fixes between the two rather than letting them drift.

## Build

    ./gradlew build --max-workers=2           # compile + test + spotlessCheck
    ./gradlew spotlessApply                   # before every commit
    ./gradlew :runner:installDist             # the runner; see README.md

Other agents build Quartz, Amethyst and the Observer on the same machine: **one Gradle
invocation at a time**, `--max-workers=2`, and stop the daemons when done.

## Rules

- **Never log a key, a plaintext or a ciphertext.** Not an nsec (plain or Fernet), not a Fernet
  key, not a decrypted schedule (its task name and params included), not a NIP-44 content. Log
  the owner, the task's public `d`, an outcome enum, a count. An exception that could carry any
  of them is logged by its class only. The tests assert this on captured logs.
- **Never touch a real relay, task service or production database from a test.** Relays are a
  fake `RelayAccess` or a scripted `INostrClient`; the wake is a fake `TaskWaker` or an OkHttp
  interceptor; Postgres is a throwaway cluster (`initdb` into a temp dir) or CI's `postgres`
  service. Tests that need Postgres and find neither are skipped, and say so.
- **The runner holds the Assistant's key only to read schedules.** It never signs, never
  decrypts a kind 12000, never publishes. Anything that spends is the task service's.
- **Postgres** is plain JDBC (`org.postgresql:postgresql`), every call on `Dispatchers.IO`, one
  connection per operation (no pool: a poll every five minutes makes a handful of calls). The
  runner only reads, and holds its advisory lock: its role has a column grant and no schema.
  Every connection is bounded (`Db`: connect, login, socket and statement timeouts); keep any new
  call under them. The lock is a session lock, so `DATABASE_URL` must be a direct, session-mode
  connection to the primary, never a transaction-mode pooler or a standby.
- **Quartz** comes from JitPack pinned by a full commit hash (`gradle/libs.versions.toml`).
  Hashes carry no order, so every module `force()`s the pin; the only relation between two
  hashes is `git merge-base --is-ancestor`. Quartz is KMP with Android in its graph, so
  `settings.gradle.kts` needs `google()`. `-PquartzDir=../quartz` builds a local checkout.
- Log through Quartz's `Log` (`Log.d(TAG) { "…" }` for interpolated lines), never `println` or
  another logger.
- No fully-qualified class names inline in function bodies: import them.
- **Dependency licences**: read the LICENSE or the published POM before adding one. Permissive
  (MIT, Apache-2.0, BSD, ISC, MPL-2.0) is fine; LGPL or GPL with a linking exception, say so in
  the summary; anything stricter (GPL/AGPL without an exception, SSPL, proprietary), stop and
  ask. The repo is MIT.
- Comments say why, especially why not. Run `./gradlew spotlessApply build` before committing.
- Commits are conventional (`feat:`, `fix:`, `docs:`, `refactor!:`). Never `--no-verify`, never
  force-push.
