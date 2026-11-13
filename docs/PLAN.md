# assistant-runner: the design

The Brainstorm Assistants' scheduler. Its one job: read the NIP-FB scheduled tasks (kind 34817)
whose owners named their Brainstorm Assistant as runner, and wake the task's service when an
occurrence is due. It prints nothing, signs nothing and spends nothing.

The task's service does the whole job itself. The Observer's is in `NosFabrica/the-nostr-observer`.
It gets an owner, opens Postgres, decrypts the Assistant's AI key, prints, signs as the Assistant
and publishes to the Assistant's outbox relays.

**brainstorm_server (Python) is never involved in a run.** It owns:
- the `brainstorm_nsec` table, which the services read four columns of, as its own repo does;
- the Fernet key file, which it writes and rotates.

The authorities, which win when this repo disagrees with them:
- NIP-FB: the schedule, the runner rules;
- NIP-FA: the AI keys (kind 12000);
- Quartz's `quartz/plans/2026-11-11-observer-site.md`: the Observer site and the Assistant as runner.

## The pieces

| | assistant-runner `:runner` | a task service (the Observer) | brainstorm_server |
|---|---|---|---|
| Does | finds due occurrences, wakes the service | re-checks, prints, signs, publishes | creates Assistants, AI-key endpoints |
| Keys | decrypts schedules only (the owner as peer) | decrypts the schedule and the Assistant's 12000; signs its own task's events | holds and rotates |
| Postgres | reads four columns of `brainstorm_nsec` and `assistant_runner.runs` | reads the same four columns; writes `assistant_runner.*` | owns `brainstorm_nsec` |
| Talks to | relays; the service's trigger endpoint | relays, Blossom, the AI endpoint | |

- **`:core`** is a library both use: Fernet, the key read, the `assistant_runner` schema and its
  stores, the advisory lock.
- **Why the runner needs keys too:** NIP-FB keeps every tag but `d` and `p` private, `cron`
  included, so knowing when a schedule is due takes a decrypt.
- **Why it still signs nothing:** waking a service is harmless. The service re-reads and re-decides
  everything itself before it spends, so a wrong wake costs one cheap refusal.

## The runner (`:runner`)

One process. **Exactly one is active**: it holds a Postgres advisory lock (`pg_try_advisory_lock`)
on a dedicated connection. A second pod, during a rolling deploy for example, waits for the lock.
If that connection drops, the runner exits and the orchestrator restarts it.

Each poll (default every 5 minutes):

1. **Pairs.** Page `SELECT pubkey, assistant_pubkey FROM brainstorm_nsec WHERE assistant_pubkey IS
   NOT NULL ORDER BY pubkey` (by `pubkey > ?`), the server's own `select_owner_assistant_pairs_on_db`.
   Nothing is decrypted to list them.
2. **Schedules.** Read kind 34817 by author (the owners), with the quorum rules the
   the-nostr-observer worker already has (`RunnerRelays`, moved here as is):
   - index relays first, then each owner's NIP-65 write relays;
   - a full page counts as unanswered;
   - every relay gets 20 s and 2,000 events;
   - events must verify, and be dated at most 10 minutes ahead.
3. **Runner check.** `ScheduledTaskClaims.checkRunner(event, assistant, owner)`: exactly one public
   `p`, the owner's Assistant, and the author is that owner.
4. **Decrypt** the private tags with the Assistant's key (`:core` `AssistantKeys`). The plaintext is
   kept in memory by event id, never logged and never stored.
5. **Decide** with Quartz's `ScheduledTaskClaims.decideAsRunner`: due, not paused, after the time it
   was set, no live claim by one of the owner's devices. "Already started here" is a row in
   `assistant_runner.runs` for `(task, owner, occurrence)`.
6. **Task type.** The schedule's `task` tag picks it. Unknown types are skipped.
7. **Wake** `POST {service}/v1/run`, at most once per `(task, owner, occurrence)` every
   `WAKE_RETRY_SECONDS` (default 600), until the service has written a `runs` row. The last wake per
   occurrence is kept in memory: a restart only means one extra wake, which the service refuses as
   settled.

The runner writes nothing about runs. The service is the one that spends, so it is the one that
records.

## The trigger contract (v1)

**`POST /v1/run`** on the task service, internal to the cluster, no token. A wake only makes the
service do what the owner's own signed schedule asks, and the service re-checks all of it.

```json
{"v": 1, "task": "observer", "owner": "<64-hex>", "occurrence": 1763020800}
```

- **`occurrence`** is a hint, unix seconds. The service decides the occurrence itself, from the
  schedule it reads.
- **`202`** accepted: it will look at this owner now.
- **`200 {"outcome": "…"}`** settled, nothing to do (`NOT_DUE`, `ATTEMPTED`, `NOT_NAMED`, …).
- **`429`** busy, with `Retry-After`: the runner wakes again after it.
- **`400`** malformed.

The service is idempotent per owner: a wake for an owner already printing joins that run.
`contract/v1/` holds the JSON vectors both sides test against.

## The shared state (`:core`)

### The keys

- **`brainstorm_nsec`**, brainstorm_server's table, read as its repo (`app/repos/brainstorm_nsec.py`)
  reads it. There is no view and no migration on the server's side: the services read four of its
  columns, `pubkey`, `assistant_pubkey`, `encrypted_nsec` and `nsec`, and **are coupled to them**. A
  server migration that renames or drops one breaks the runner and every task service.
- **`AssistantKeys`** reads one row per owner it needs,
  `SELECT assistant_pubkey, encrypted_nsec, nsec FROM brainstorm_nsec WHERE pubkey = ?`, and opens
  it as the server's `_plaintext_nsec` does:
  - `encrypted_nsec` when it is non-empty, decrypted with the key file (a value that already
    starts `nsec1` is taken as it is, as `decrypt_nsec` does);
  - `nsec` only otherwise. `nsec` is `NOT NULL` and may still hold a stale plaintext beside a
    token: the token wins, and that plaintext is never used.
  - No row, or one with no `assistant_pubkey`, is `NO_ASSISTANT`. The nsec must give the stored
    `assistant_pubkey`, or the result is `NSEC_MISMATCH`. The key is held in memory only, and never
    logged.
- **The Fernet keys** are a file of comma-separated keys (`NSEC_KEYS_FILE`, default
  `/run/secrets/nsec_encryption_keys`), tried in order, as Python's `MultiFernet` does.
  - The file is re-read when its modification time changes, and once more on a token no key
    opens (a rotation writes `[new, old]`, then re-encrypts).
  - A token no key opens even then is `NSEC_UNREADABLE` for that owner. It is never fatal to the
    poll.
- **Fernet** (version `0x80`, AES-128-CBC, HMAC-SHA256, URL-safe base64) is implemented with
  `javax.crypto`. It is tested against tokens Python's `cryptography` made. TTL is not checked:
  the server sets none.
- **Deployment:** the runner and the services must mount the same key file brainstorm_server
  writes. Today the server writes it itself (it generates the first key when the file is missing,
  and `rotate_key` rewrites it), so it must be on a volume the three can read, with only the
  server writing.

### The `assistant_runner` schema

Migrated at startup by whichever process gets there first, under its own advisory lock: versioned
SQL in `core/src/main/resources/assistant_runner/`, with a `schema_version` table.

- **`runs`**: `task`, `owner`, `occurrence`, `state` (`started` | `ended`), `outcome`,
  `schedule_id`, `started_at`, `ended_at`. The primary key is `(task, owner, occurrence)`.
  - A service claims an occurrence with `INSERT … ON CONFLICT DO NOTHING`. Only the inserter
    runs it, so two replicas can never print it twice.
  - The claim is written before anything is spent. If it can't be written, nothing is spent.
  - A run that stopped before spending deletes its row (`forget`) and may be tried again.
  - A run that may have spent ends, and is never run again.
- **`pending`** and **`pending_files`**: a printed paper waiting to be published (the worker's
  `PendingPapers`), stored before its first publish and kept 12 hours. They're written and read by
  the Observer.

### Roles and grants (ops, not migrations)

```sql
CREATE ROLE assistant_runner LOGIN PASSWORD '…';
GRANT SELECT (pubkey, assistant_pubkey, encrypted_nsec, nsec) ON brainstorm_nsec TO assistant_runner;
CREATE SCHEMA IF NOT EXISTS assistant_runner AUTHORIZATION assistant_runner;
```

A column grant: the role reads those four columns of `brainstorm_nsec` and is refused every other
one (`session_version`, `nip05_local_part`, `SELECT *`). The runner and the Observer may share this
role, or each get its own with the same grants.

## Configuration (`:runner`)

| Variable | Meaning |
|---|---|
| `DATABASE_URL` | JDBC URL of brainstorm_server's Postgres |
| `DATABASE_USER`, `DATABASE_PASSWORD` | the `assistant_runner` role |
| `NSEC_KEYS_FILE` | the Fernet key file (default `/run/secrets/nsec_encryption_keys`) |
| `INDEX_RELAYS` | comma-separated; default `wss://search.brainstorm.world` |
| `TASK_URL_<TASK>` | a task service's base URL, e.g. `TASK_URL_OBSERVER=http://observer:8080` |
| `POLL_SECONDS` | default 300 |
| `WAKE_RETRY_SECONDS` | default 600 |

## Tests

No test touches a real relay, AI endpoint or production database:
- relays are a fake `INostrClient`, ported from the worker's `RunnerWorld`/`Fakes`;
- the trigger is a fake `TaskWaker`;
- Postgres is a throwaway cluster (`initdb` into a temp dir) where the binaries are there (here:
  `/usr/lib/postgresql/16/bin`), and the `postgres` service in CI. Without either, those tests are
  skipped and say so.

What they cover:
- **Fernet:** Python-made vectors, the key order, rotation (reload on mtime, retry once), tampered
  tokens.
- **Keys:** `legacy_nsec`, `NSEC_MISMATCH`, never logging a key (captured logs).
- **Migrations:** they apply to an empty schema; a second run is a no-op; two concurrent migrators
  are fine.
- **Runs:** only one of two concurrent claims wins; forget; end.
- **The advisory lock:** a second runner doesn't poll.
- **The runner:**
  - due and undue schedules;
  - an unknown task;
  - a schedule naming someone else;
  - no quorum;
  - a wake isn't repeated within `WAKE_RETRY_SECONDS`;
  - an occurrence with a `runs` row isn't woken;
  - `429` honours `Retry-After`.
- **The worker's relay tests that still apply** move over: quorum, bounds, limits.
- **The contract vectors** parse.

## What moves where

| the-nostr-observer `runner/` | Goes to |
|---|---|
| `RunnerRelays.kt` | here (`:runner`), as it is |
| the schedule read and decision in `ObserverRunner.kt` | here (`:runner`), generic |
| `RunRecords.kt`, `PendingPapers.kt` | `:core` stores (Postgres), same semantics; the Observer uses them |
| `InternalApi.kt` | replaced in the Observer by `:core` `AssistantKeys` plus local NIP-44 and signing, with the same allowlist the Python service enforced |
| everything else (printing, Routstr, sites, Blossom, publish) | stays in the Observer |
| the poll over every owner | becomes the Observer's `POST /v1/run` for one owner |

The Observer's side is the next change, in the-nostr-observer. Until it lands, the old worker
keeps working, unchanged.
