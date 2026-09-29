"""Runtime configuration sourced from environment variables."""
from __future__ import annotations

import os

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://origin:origin@db:5432/origin"
)
# Size of each bytea slice pulled from PostgreSQL while streaming a body.
CHUNK_SIZE = int(os.environ.get("ORIGIN_CHUNK_SIZE", str(1024 * 1024)))
# Default / upper bound for entries per copy-session segment.
COPY_SEGMENT_SIZE = int(os.environ.get("ORIGIN_COPY_SEGMENT_SIZE", "16"))
COPY_MAX_SEGMENT_SIZE = int(os.environ.get("ORIGIN_COPY_MAX_SEGMENT_SIZE", "1024"))

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS objects (
    path       text PRIMARY KEY,
    current_id bigint
);

CREATE TABLE IF NOT EXISTS revisions (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_path text NOT NULL REFERENCES objects (path) ON DELETE CASCADE,
    revision_no bigint NOT NULL,
    content     bytea NOT NULL,
    sha256      text NOT NULL CHECK (char_length(sha256) = 64),
    media_type  text NOT NULL DEFAULT 'application/octet-stream',
    created_at  timestamptz NOT NULL
                            DEFAULT date_trunc('second', now()),
    UNIQUE (object_path, revision_no)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'objects_current_id_fkey'
    ) THEN
        ALTER TABLE objects
            ADD CONSTRAINT objects_current_id_fkey
            FOREIGN KEY (current_id) REFERENCES revisions (id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS revisions_object_idx
    ON revisions (object_path, revision_no);

-- Server-side singletons (e.g. the HMAC key that signs copy cursors).
CREATE TABLE IF NOT EXISTS service_meta (
    key   text PRIMARY KEY,
    value text NOT NULL
);

-- Resumable copy sessions.  A session pins an immutable manifest of
-- (source path, revision id) pairs captured in one database snapshot;
-- segments of the manifest are pulled and acknowledged by cursor.
CREATE TABLE IF NOT EXISTS copy_sessions (
    id              text PRIMARY KEY,
    target_prefix   text NOT NULL,
    source_hash     text NOT NULL,
    manifest_sha256 text NOT NULL CHECK (char_length(manifest_sha256) = 64),
    segment_size    integer NOT NULL,
    total_entries   integer NOT NULL,
    total_segments  integer NOT NULL,
    next_segment    integer NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL
                            DEFAULT date_trunc('second', now())
);

CREATE TABLE IF NOT EXISTS copy_session_entries (
    session_id  text NOT NULL REFERENCES copy_sessions (id) ON DELETE CASCADE,
    ordinal     integer NOT NULL,
    segment_no  integer NOT NULL,
    source_path text NOT NULL,
    target_path text NOT NULL,
    revision_id bigint NOT NULL REFERENCES revisions (id),
    revision_no bigint NOT NULL,
    sha256      text NOT NULL CHECK (char_length(sha256) = 64),
    media_type  text NOT NULL,
    length      bigint NOT NULL,
    -- Revision-identity baseline of the target path captured in the very
    -- same snapshot as the source manifest.  A NULL baseline_revision_id
    -- pins "target did not exist (had no revision) yet".  Acknowledgement
    -- compares revision *identity* against this baseline, so a same-bytes
    -- re-publish after session creation cannot masquerade as
    -- already_present.  baseline_captured is FALSE for rows that existed
    -- before this column existed (upgrade-era sessions): such sessions
    -- must not be acknowledged by digest comparison alone.
    baseline_captured   boolean NOT NULL DEFAULT FALSE,
    baseline_revision_id bigint REFERENCES revisions (id),
    baseline_revision_no bigint,
    baseline_sha256      text
                           CHECK (baseline_sha256 IS NULL
                                  OR char_length(baseline_sha256) = 64),
    PRIMARY KEY (session_id, ordinal)
);

CREATE INDEX IF NOT EXISTS copy_session_entries_segment_idx
    ON copy_session_entries (session_id, segment_no, ordinal);

-- Upgrade path for databases created before revision baselines existed:
-- existing manifest rows keep baseline_captured = FALSE (the column
-- default), which marks them as sessions without a usable baseline.
ALTER TABLE copy_session_entries
    ADD COLUMN IF NOT EXISTS baseline_captured boolean NOT NULL DEFAULT FALSE;
ALTER TABLE copy_session_entries
    ADD COLUMN IF NOT EXISTS baseline_revision_id bigint REFERENCES revisions (id);
ALTER TABLE copy_session_entries
    ADD COLUMN IF NOT EXISTS baseline_revision_no bigint;
ALTER TABLE copy_session_entries
    ADD COLUMN IF NOT EXISTS baseline_sha256 text;

-- One row per acknowledged segment: the persisted batch result that
-- makes repeated acknowledgements and replays byte-identical.  Stored
-- as text (not jsonb) to preserve the exact key order of the original
-- response.
CREATE TABLE IF NOT EXISTS copy_session_acks (
    session_id text NOT NULL REFERENCES copy_sessions (id) ON DELETE CASCADE,
    segment_no integer NOT NULL,
    cursor     text NOT NULL,
    status     text NOT NULL,
    result     text NOT NULL,
    created_at timestamptz NOT NULL
                           DEFAULT date_trunc('second', now()),
    PRIMARY KEY (session_id, segment_no)
);
"""
