# assistant-runner

The Brainstorm Assistants' scheduler. It reads the NIP-FB scheduled tasks (kind 34817) whose
owners named their Brainstorm Assistant as runner, and wakes the task's service when an
occurrence is due. It prints nothing, signs nothing and spends nothing.

The design is [`docs/PLAN.md`](docs/PLAN.md); the trigger contract is [`contract/v1/`](contract/v1/);
the rules for working here are in [`AGENTS.md`](AGENTS.md).

## Modules

- **`:core`**, a library the runner and the task services share:
  - `Fernet`, `MultiFernet`, `FernetKeyFile`: brainstorm_server's nsec encryption, read from its
    key file (re-read on an mtime change, and once more on a token no key opens);
  - `AssistantKeys` (an `AssistantDirectory`): brainstorm_server's `brainstorm_nsec` table, read
    as its own repo reads it (four columns: `pubkey`, `assistant_pubkey`, `encrypted_nsec`,
    `nsec`; a server migration that renames or drops one breaks the services), paged without
    decrypting, and one owner's Assistant key as a Quartz `NostrSignerInternal`, or
    `NO_ASSISTANT` / `NSEC_UNREADABLE` / `NSEC_MISMATCH`;
  - `Db`, `Migrations` (the `assistant_runner` schema), `RunStore`, `PendingStore`, each with a
    Postgres and an in-memory implementation;
  - `LeaderLock`: one active runner;
  - `WakeRequest`, `SettledAnswer`: the trigger contract's bodies, parsed and validated.
- **`:runner`**, the scheduler (`Main.kt`, `Runner.kt`, `RunnerLoop.kt`, `TaskWaker.kt`), with the
  relay reads (`RunnerRelays.kt`) and the SSRF guard (`SafeHttp.kt`) moved from
  the-nostr-observer's worker.

## What one poll does

1. Pages `SELECT pubkey, assistant_pubkey FROM brainstorm_nsec WHERE assistant_pubkey IS NOT NULL
   ORDER BY pubkey`, 500 at a time. Nothing is decrypted.
2. Reads every kind 34817 by those owners (by author, never `#p` or `#d`): the index relays first,
   then each owner's NIP-65 write relays, with the worker's quorum rules (a full page counts as
   unanswered; each relay gets 20 s and 2,000 events; events must verify and be dated at most 10
   minutes ahead). It keeps the newest version of each `d`.
3. `ScheduledTaskClaims.checkRunner(event, assistant, owner)`: exactly one public `p`, the owner's
   Assistant, authored by that owner. No `p` is `NOT_NAMED`; more than half the owner's write
   relays must have answered (`NO_QUORUM`).
4. Decrypts the private tags with the Assistant's key, the owner as NIP-44 peer
   (`ScheduledTaskEvent.task`). The plaintext stays in memory by event id.
5. The `task` tag picks the service (`TASK_URL_<TASK>`); others are `UNKNOWN_TASK`.
6. Decides with `ScheduledTaskClaims.decideAsRunner`; "already started here" is a row in
   `assistant_runner.runs` for (task, owner, occurrence).
7. Wakes `POST {TASK_URL_<TASK>}/v1/run` with `{"v":1,"task","owner","occurrence"}`, at most once
   per (task, owner, occurrence) every `WAKE_RETRY_SECONDS`, or after a `429`'s `Retry-After`.

Outcomes are logged by owner and enum (`RunnerOutcome`), the steady ones at debug.

## Configuration

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | (required) | JDBC URL of brainstorm_server's Postgres, e.g. `jdbc:postgresql://postgres:5432/brainstorm` |
| `DATABASE_USER`, `DATABASE_PASSWORD` | | the `assistant_runner` role (PLAN.md, "Roles and grants"). A secret: from a k8s Secret |
| `NSEC_KEYS_FILE` | `/run/secrets/nsec_encryption_keys` | the Fernet key file brainstorm_server writes, mounted read-only |
| `INDEX_RELAYS` | `wss://search.brainstorm.world` | comma-separated. The search relay is asked with `include:spam` |
| `TASK_URL_<TASK>` | (at least one) | a task service's base URL, e.g. `TASK_URL_OBSERVER=http://observer:8080` |
| `POLL_SECONDS` | `300` | at least 30 |
| `WAKE_RETRY_SECONDS` | `600` | at least 60 |

Exit status: `2` a bad setting, `4` the leader lock's connection died, `5` the migration or the
loop failed. The orchestrator restarts it.

## Deploying beside brainstorm_server

    docker build -t assistant-runner .

- One Deployment. Two replicas are safe (the second waits for the leader lock), but one is enough.
  No Service, no Ingress: nothing connects to it.
- It migrates `assistant_runner` at startup, under its own advisory lock, so whichever of the
  runner and the task services starts first does it.
- Mount the same key file brainstorm_server writes (read-only). The runner holds each Assistant's
  key only to read schedules.
- Egress: Postgres, the task services inside the cluster, and WSS to relays, directly (an owner's
  relay is refused when it names an internal host or resolves to one).

## Build

    ./gradlew build --max-workers=2

The Postgres tests start a throwaway cluster (`initdb`) when the binaries are there, else use
`TEST_DATABASE_URL` (CI's `postgres` service), else are skipped and say why.
