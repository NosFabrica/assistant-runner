# assistant-runner

The Brainstorm Assistants' scheduler. It reads the NIP-FB scheduled tasks (kind 34817) whose
owners named their Brainstorm Assistant as runner, and wakes the task's service when an
occurrence is due. It prints nothing, signs nothing and spends nothing.

The design is [`docs/PLAN.md`](docs/PLAN.md); the trigger contract is [`contract/v1/`](contract/v1/);
the rules for working here are in [`AGENTS.md`](AGENTS.md).

## What's here

One module, **`:runner`**, the scheduler:
- `Main.kt`, `Runner.kt`, `RunnerLoop.kt`, `TaskWaker.kt`, with the relay reads
  (`RunnerRelays.kt`) and the SSRF guard (`SafeHttp.kt`) moved from the-nostr-observer's worker;
- `Fernet`, `MultiFernet`, `FernetKeyFile`: brainstorm_server's nsec encryption, read from its key
  file (re-read on an mtime change, and once more on a token no key opens);
- `AssistantKeys` (an `AssistantDirectory`): brainstorm_server's `brainstorm_nsec` table, read as
  its own repo reads it (four columns: `pubkey`, `assistant_pubkey`, `encrypted_nsec`, `nsec`; a
  server migration that renames or drops one breaks the runner), paged without decrypting, and one
  owner's Assistant key as a Quartz `NostrSignerInternal`, or `NO_ASSISTANT` / `NSEC_UNREADABLE` /
  `NSEC_MISMATCH`;
- `Db` (read-only) and `LeaderLock`: one active runner;
- `WakeRequest`, `SettledAnswer`: the wake it sends and the answer it reads.

**No code is shared with the task services.** The Observer has its own copy of the key reading and
its own state; the two share only the wake's contract (the vectors in `contract/v1/`, which it
copies into its tests), the server's four `brainstorm_nsec` columns and the Fernet key file.

## What one poll does

1. Pages `SELECT pubkey, assistant_pubkey FROM brainstorm_nsec WHERE assistant_pubkey IS NOT NULL
   ORDER BY pubkey`, 500 at a time. Nothing is decrypted.
2. Reads every kind 34817 by those owners (by author, never `#p` or `#d`): the index relays first,
   then each owner's NIP-65 write relays, with the worker's quorum rules (a full page counts as
   unanswered; each relay gets 20 s and 2,000 events; events must verify and be dated at most 10
   minutes ahead). It keeps the newest version of each `d`. An owner's relay is asked through the
   SSRF guard, its socket reads at most 4 MB per 10 s, and its WebSocket is never compressed
   (`permessage-deflate` is not offered, and a relay that compresses anyway is dropped: OkHttp
   inflates a whole message in memory, past any byte count). At most 8 relays are read at once,
   so owners' relays can make one poll hold about 96 MB of frames at worst: give the container a
   heap of 512 MB or more.
3. `ScheduledTaskClaims.checkRunner(event, assistant, owner)`: exactly one public `p`, the owner's
   Assistant, authored by that owner. No `p` is `NOT_NAMED`; more than half the owner's write
   relays must have answered (`NO_QUORUM`).
4. Decrypts the private tags with the Assistant's key, the owner as NIP-44 peer
   (`ScheduledTaskEvent.task`). The plaintext stays in memory by event id.
5. The `task` tag picks the service (`TASK_URL_<TASK>`); others are `UNKNOWN_TASK`.
6. Decides with `ScheduledTaskClaims.decideAsRunner`; "already started here" is the service's own
   `200 {outcome}` for (task, owner, occurrence), kept in memory.
7. Wakes `POST {TASK_URL_<TASK>}/v1/run` with `{"v":1,"task","owner","occurrence"}`, at most once
   per (task, owner, occurrence) every `WAKE_RETRY_SECONDS`, or after a `429`'s `Retry-After`,
   until the service answers `200`: then never again for that occurrence. A restart forgets what
   was settled, which costs at most one wake per due occurrence, answered as settled.

The runner writes nothing, to Postgres or anywhere else.

Outcomes are logged by owner and enum (`RunnerOutcome`), the steady ones at debug.

## Configuration

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | (required) | JDBC URL of brainstorm_server's Postgres, e.g. `jdbc:postgresql://postgres:5432/brainstorm` |
| `DATABASE_USER`, `DATABASE_PASSWORD` | | the `assistant_runner` role (below). A secret: from a k8s Secret |
| `NSEC_KEYS_FILE` | `/run/secrets/nsec_encryption_keys` | the Fernet key file brainstorm_server writes, mounted read-only |
| `INDEX_RELAYS` | `wss://search.brainstorm.world` | comma-separated. The search relay is asked with `include:spam` |
| `TASK_URL_<TASK>` | (at least one) | a task service's base URL, e.g. `TASK_URL_OBSERVER=http://observer:8080` |
| `POLL_SECONDS` | `300` | at least 30 |
| `WAKE_RETRY_SECONDS` | `600` | at least 60 |

Exit status: `2` a bad setting, `4` the leader lock's connection died, `5` the loop failed. The
orchestrator restarts it.

## Deploying beside brainstorm_server

CI (`.github/workflows/build.yml`) builds and pushes the image on every push, after the tests
pass: `ghcr.io/nosfabrica/assistant-runner`, tagged with the branch name, `sha-<short>`, a `v*`
tag's version, and `latest` on `main`. Staging pins one of those tags. By hand:

    docker build -t assistant-runner .

- One Deployment. Two replicas are safe (the second waits for the leader lock), but one is enough.
  No Service, no Ingress: nothing connects to it.
- Its role needs one column grant and nothing else: no schema, no CREATE (it writes nothing; the
  advisory lock needs no grant):

      CREATE ROLE assistant_runner LOGIN PASSWORD '…';
      GRANT SELECT (pubkey, assistant_pubkey, encrypted_nsec, nsec) ON brainstorm_nsec TO assistant_runner;

- Mount the same key file brainstorm_server writes (read-only). The runner holds each Assistant's
  key only to read schedules.
- Egress: Postgres, the task services inside the cluster, and WSS to relays, directly (an owner's
  relay is refused when it names an internal host or resolves to one).

## Build

    ./gradlew build --max-workers=2

The Postgres tests start a throwaway cluster (`initdb`) when the binaries are there, else use
`TEST_DATABASE_URL` (CI's `postgres` service), else are skipped and say why.
