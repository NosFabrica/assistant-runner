# assistant-runner: the design

The Brainstorm Assistants' scheduler. Its one job: read the NIP-FB scheduled tasks (kind 34817)
whose owners named their Brainstorm Assistant as runner, and wake the task's service when an
occurrence is due. It prints nothing, signs nothing and spends nothing.

The task's service does the whole job itself. The Observer's is in `NosFabrica/the-nostr-observer`.
It gets an owner, opens Postgres, decrypts the Assistant's AI key, prints, signs as the Assistant
and publishes to the Assistant's outbox relays.

**brainstorm_server (Python) is never involved in a run.** It owns:
- the `brainstorm_nsec` table, which the runner and the services read four columns of, as its own
  repo does;
- the Fernet key file, which it writes and rotates.

**The runner and the task services share no code.** Neither depends on the other's codebase. What
they share is only what both talk to:
- the wake, `POST /v1/run`, and its JSON (`contract/v1/vectors/`, which each side copies into its
  own tests);
- brainstorm_server's four `brainstorm_nsec` columns, read with the server's `_plaintext_nsec` rule;
- the Fernet key file.

The Observer has its own copy of the key reading (Fernet, the key file, the table read), tested on
its own, and its own state in its own schema. A fix to one side's key reading is ported by hand.

The authorities, which win when this repo disagrees with them:
- NIP-FB: the schedule, the runner rules;
- NIP-FA: the AI keys (kind 12000);
- Quartz's `quartz/plans/2026-11-11-observer-site.md`: the Observer site and the Assistant as runner.

## The pieces

| | assistant-runner `:runner` | a task service (the Observer) | brainstorm_server |
|---|---|---|---|
| Does | finds due occurrences, wakes the service | re-checks, prints, signs, publishes | creates Assistants, AI-key endpoints |
| Keys | decrypts schedules only (the owner as peer) | decrypts the schedule and the Assistant's 12000; signs its own task's events | holds and rotates |
| Postgres | reads four columns of `brainstorm_nsec`; holds an advisory lock; writes nothing | reads the same four columns with its own code; writes its own schema (the Observer's `observer`) | owns `brainstorm_nsec` |
| Talks to | relays; the service's trigger endpoint | relays, Blossom, the AI endpoint | |

- **Why the runner needs keys too:** NIP-FB keeps every tag but `d` and `p` private, `cron`
  included, so knowing when a schedule is due takes a decrypt.
- **Why it still signs nothing:** waking a service is harmless. The service re-reads and re-decides
  everything itself before it spends, so a wrong wake costs one cheap refusal.

## The runner (`:runner`)

One process. **Exactly one is active**: it holds a Postgres advisory lock (`pg_try_advisory_lock`)
on a dedicated connection. A second pod, during a rolling deploy for example, waits for the lock.
If that connection drops, the runner exits and the orchestrator restarts it.

- **`DATABASE_URL` must reach the primary directly, in session mode.** Behind a transaction-mode
  pooler the lock's session isn't the runner's, and a standby gives every runner its own lock.
  The runner refuses to lead on a standby (`pg_is_in_recovery()`); a pooler it can't see.
- The lock is checked every 30 s between polls and every 5 s during one: a lock lost mid-poll
  cancels the poll, and every wake first asks whether the lock still holds (an answer at most 5 s
  old), so two runners never wake for long side by side.
- The lock's session sets its own TCP keepalives (`tcp_keepalives_idle=30`, `_interval=10`,
  `_count=3`, `tcp_user_timeout=60000`, `idle_session_timeout=0`; all `user` context, no grant):
  after a partition the server frees the lock within about a minute.
- Every connection is bounded: connect 10 s, login 15 s, an answer 30 s (`socketTimeout`, which
  never touches an idle connection), a statement 30 s (`statement_timeout`).

Polls start every `POLL_SECONDS` on a monotonic clock, from one start to the next: a poll that took
longer is followed at once.

Each poll (default every 5 minutes):

1. **Pairs.** Page `SELECT pubkey, assistant_pubkey FROM brainstorm_nsec WHERE pubkey ~
   '^[0-9a-f]{64}$' AND assistant_pubkey ~ '^[0-9a-f]{64}$' ORDER BY pubkey` (by `pubkey > ?`), the
   server's own `select_owner_assistant_pairs_on_db` with the rows no schedule could match left out
   in SQL (a page short of the limit ends the paging, so they can't be filtered after it).
   Nothing is decrypted to list them.
2. **Schedules.** Read kind 34817 by author (the owners), with the quorum rules the
   the-nostr-observer worker already has (`RunnerRelays`, moved here as is):
   - index relays first, then each owner's NIP-65 write relays;
   - the quorum is more than half the owner's write relays. The index relays stand in only when
     an index relay answered for the owner in full and holds no list: an unread list (CLOSED, a
     timeout, a cap) or one whose every relay is refused is `NO_QUORUM`, never the index alone;
   - a full page (judged on what the relay sent, before any check), or one the event cap cut
     off, is paged on with `until`, copies deduplicated by id; a relay answers for a chunk of owners
     only on a page that isn't full. A CLOSED chunk is asked again in halves, never one owner twice,
     and nothing more after `rate-limited:` or `blocked:`. At most 64 REQs a relay per 500 owners,
     which covers 500 owners with six schedules each on a relay that cuts at 100;
   - every relay gets 20 s and 2,000 events a read (never fewer than its `limit`, at most 500);
   - events must verify, and be dated at most 10 minutes ahead;
   - an owner's relay socket reads at most 4 MB per 10 s, uncompressed: `permessage-deflate` is
     never offered, and a relay that compresses anyway is dropped (OkHttp would inflate a whole
     message in memory, past what the socket counted). With 8 relays read at once, that bounds
     what owners' relays can make a poll hold to about 96 MB of frames.
3. **Runner check.** `ScheduledTaskClaims.checkRunner(event, assistant, owner)`: exactly one public
   `p`, the owner's Assistant, and the author is that owner.
4. **Decrypt** the private tags with the Assistant's key (`AssistantKeys`). The plaintext is
   kept in memory by event id, never logged and never stored.
5. **Decide** with Quartz's `ScheduledTaskClaims.decideAsRunner`: due, not paused, after the time it
   was set, no live claim by one of the owner's devices. "Already started here" is the service's
   own answer: a `200 {outcome}` for that `(task, owner, occurrence)`, kept in memory.
6. **Task type.** The schedule's `task` tag picks it. Unknown types are skipped.
7. **Wake** `POST {service}/v1/run`, at most once per `(task, owner, occurrence)` every
   `WAKE_RETRY_SECONDS` (default 600), or after a `429`'s `Retry-After`, until the service answers
   `200`. A `200 {outcome}` settles the occurrence: it is never woken again.

Both the last wake and the settled occurrences are kept in memory only, per `(task, owner,
occurrence)`: two schedules of one task (06:00 and 06:15) each settle their own. A settled
occurrence is forgotten after two days, or with its owner's Assistant. Only one wake per
occurrence is under way at a time, so two schedules due at the same occurrence wake it once. A
restart forgets them, which costs at most one extra wake per due occurrence, and the service
answers that wake as settled.

The runner writes nothing, to Postgres or anywhere else, and never reads the services' state. The
service is the one that spends, so it is the one that records.

## The trigger contract (v1)

**`POST /v1/run`** on the task service, internal to the cluster, no token. A wake only makes the
service do what the owner's own signed schedule asks, and the service re-checks all of it.

```json
{"v": 1, "task": "observer", "owner": "<64-hex>", "occurrence": 1763020800}
```

- **`occurrence`** is a hint, unix seconds. The service decides the occurrence itself, from the
  schedule it reads.
- **`202`** accepted: it will look at this owner now.
- **`200 {"outcome": "…"}`** settled, nothing to do (`NOT_DUE`, `ATTEMPTED`, `NOT_NAMED`, …). The
  runner doesn't wake that occurrence again.
- **`429`** busy, with `Retry-After`: the runner wakes again after it.
- **`400`** malformed.

The service is idempotent per owner: a wake for an owner already printing joins that run.
`contract/v1/` holds the JSON vectors both sides test against: the runner reads them where they
live, and a service copies them into its own test resources. The two sides' types are each their
own code.

## The keys

What the runner reads, with code of its own; a task service that needs the Assistant's key reads
the same columns the same way with its own copy.

- **`brainstorm_nsec`**, brainstorm_server's table, read as its repo (`app/repos/brainstorm_nsec.py`)
  reads it. There is no view and no migration on the server's side: the runner reads four of its
  columns, `pubkey`, `assistant_pubkey`, `encrypted_nsec` and `nsec`, and **is coupled to them**. A
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

### Roles and grants (ops)

```sql
CREATE ROLE assistant_runner LOGIN PASSWORD '…';
GRANT SELECT (pubkey, assistant_pubkey, encrypted_nsec, nsec) ON brainstorm_nsec TO assistant_runner;
```

A column grant, and nothing else: the role reads those four columns of `brainstorm_nsec` and is
refused every other one (`session_version`, `nip05_local_part`, `SELECT *`). No schema and no
CREATE: the runner writes nothing. Its advisory lock needs no grant. Each task service has a role
of its own, with the same column grant and whatever its own state needs (the Observer's README).

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
- **Keys:** `legacy_nsec`, `NSEC_MISMATCH`, never logging a key (captured logs); the runner's role
  reads its four columns and no other.
- **The advisory lock:** a second runner doesn't poll.
- **The runner:**
  - due and undue schedules;
  - an unknown task;
  - a schedule naming someone else;
  - no quorum;
  - a wake isn't repeated within `WAKE_RETRY_SECONDS`;
  - a `200` settled answer stops further wakes of that occurrence;
  - a restart wakes once and accepts the settled answer;
  - `429` honours `Retry-After`.
- **The worker's relay tests that still apply** move over: quorum, bounds, limits.
- **The contract vectors:** the runner writes every wake vector exactly, can't write an invalid
  one, and reads every answer.

## What moves where

| the-nostr-observer `runner/` | Goes to |
|---|---|
| `RunnerRelays.kt` | here (`:runner`), as it is |
| the schedule read and decision in `ObserverRunner.kt` | here (`:runner`), generic |
| `RunRecords.kt`, `PendingPapers.kt` | stay in the Observer, as Postgres stores in its own schema |
| `InternalApi.kt` | replaced in the Observer by its own copy of the key reading plus local NIP-44 and signing, with the same allowlist the Python service enforced |
| everything else (printing, Routstr, sites, Blossom, publish) | stays in the Observer |
| the poll over every owner | becomes the Observer's `POST /v1/run` for one owner |

The Observer's side is the-nostr-observer's `service/`. The two repositories share no code.
