-- The runs: one row per (task, owner, occurrence) a task service claimed. The claim is the
-- INSERT ... ON CONFLICT DO NOTHING that wins; only its inserter runs the occurrence.
CREATE TABLE assistant_runner.runs (
    task        text        NOT NULL,
    owner       text        NOT NULL,
    -- Unix seconds: the NIP-FB occurrence, as Quartz's decideAsRunner names it.
    occurrence  bigint      NOT NULL,
    state       text        NOT NULL CHECK (state IN ('started', 'ended')),
    outcome     text,
    schedule_id text,
    started_at  timestamptz NOT NULL DEFAULT now(),
    ended_at    timestamptz,
    PRIMARY KEY (task, owner, occurrence)
);

-- A task's output waiting to be published (the Observer's printed paper): stored before its
-- first publish, retried until `until`, deleted once it is out.
CREATE TABLE assistant_runner.pending (
    task       text        NOT NULL,
    owner      text        NOT NULL,
    occurrence bigint      NOT NULL,
    -- The task's own fields (the Observer's edition: day, model, headline, ...).
    meta       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    -- Unix seconds after which it is dropped, published or not.
    until      bigint      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (task, owner, occurrence)
);

CREATE INDEX pending_owner ON assistant_runner.pending (owner);

CREATE TABLE assistant_runner.pending_files (
    task         text   NOT NULL,
    owner        text   NOT NULL,
    occurrence   bigint NOT NULL,
    path         text   NOT NULL,
    sha256       text   NOT NULL,
    content_type text   NOT NULL,
    bytes        bytea  NOT NULL,
    PRIMARY KEY (task, owner, occurrence, path),
    FOREIGN KEY (task, owner, occurrence) REFERENCES assistant_runner.pending ON DELETE CASCADE
);
