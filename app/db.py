"""PostgreSQL access: connection pool, revision lookup and publishing.

All conditional decisions run inside a transaction that locks the
``objects`` row of the path (``SELECT ... FOR SHARE`` for reads,
``FOR UPDATE`` for writes).  Concurrent publishes therefore serialise on
that row, and a streaming GET keeps its share lock until the body has
been fully sent, so the revision selected for the headers can neither
disappear nor be replaced while bytes are streamed.

Copy sessions reuse the same building blocks: the manifest is captured
with ``FOR SHARE`` snapshots, and acknowledging a segment applies its
entries inside a single transaction that takes ``FOR UPDATE`` locks on
the target rows in ascending path order (so concurrent acknowledgements
cannot deadlock).

When a session is created a *revision baseline* is recorded for every
target path in the same snapshot transaction (a NULL revision means the
target did not exist).  Acknowledgements compare the locked target's
current revision identity against that baseline: any publish after the
baseline -- even of byte-identical content -- is reported as a conflict,
because a digest match alone cannot distinguish "unchanged" from
"re-published since the session was pinned".
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from dataclasses import dataclass
from datetime import datetime

import asyncpg

from .config import CHUNK_SIZE, SCHEMA_SQL


@dataclass(frozen=True, slots=True)
class CurrentRevision:
    id: int
    revision_no: int
    sha256: str
    media_type: str
    length: int
    created_at: datetime


async def create_pool(database_url: str, retries: int = 30) -> asyncpg.Pool:
    """Create the pool and ensure the schema exists.

    Retries briefly so `docker compose up` works while PostgreSQL is
    still initialising.
    """
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            pool: asyncpg.Pool = await asyncpg.create_pool(
                database_url, min_size=1, max_size=20
            )
            async with pool.acquire() as conn:
                await conn.execute(SCHEMA_SQL)
            return pool
        except (OSError, asyncpg.PostgresError) as exc:  # not ready yet
            last_error = exc
            await asyncio.sleep(1)
    raise RuntimeError(
        f"could not connect to PostgreSQL after {retries} attempts"
    ) from last_error


async def current_revision(
    conn: asyncpg.Connection, path: str, *, for_update: bool
) -> CurrentRevision | None:
    """Lock the objects row and fetch its current revision.

    ``FOR SHARE`` (safe methods) blocks concurrent writes but allows
    parallel readers; ``FOR UPDATE`` (publishing) is exclusive.
    """
    lock = "FOR UPDATE OF o" if for_update else "FOR SHARE OF o"
    row = await conn.fetchrow(
        f"""
        SELECT r.id, r.revision_no, r.sha256, r.media_type,
               octet_length(r.content) AS length, r.created_at
          FROM objects o
          JOIN revisions r ON r.id = o.current_id
         WHERE o.path = $1
         {lock}
        """,
        path,
    )
    if row is None:
        return None
    return _to_revision(row)


async def lock_new_or_existing_object(
    conn: asyncpg.Connection, path: str
) -> CurrentRevision | None:
    """Writer path: create the object row if needed, then FOR UPDATE it.

    The upsert guarantees two concurrent first-publishers serialise on
    the same primary-key row instead of colliding on INSERT; the loser
    waits and then sees the winner's committed current revision.
    """
    await conn.execute(
        "INSERT INTO objects (path) VALUES ($1) ON CONFLICT (path) DO NOTHING",
        path,
    )
    row = await conn.fetchrow(
        """
        SELECT r.id, r.revision_no, r.sha256, r.media_type,
               octet_length(r.content) AS length, r.created_at
          FROM objects o
          JOIN revisions r ON r.id = o.current_id
         WHERE o.path = $1
         FOR UPDATE OF o
        """,
        path,
    )
    return None if row is None else _to_revision(row)


def _to_revision(row: asyncpg.Record) -> CurrentRevision:
    return CurrentRevision(
        id=row["id"],
        revision_no=row["revision_no"],
        sha256=row["sha256"],
        media_type=row["media_type"],
        length=row["length"],
        created_at=row["created_at"],
    )


async def stream_revision(
    conn: asyncpg.Connection,
    revision_id: int,
    first: int,
    last: int,
    chunk_size: int = CHUNK_SIZE,
):
    """Yield bytes [first, last] (inclusive) of a revision, in slices."""
    pos = first
    while pos <= last:
        end = min(pos + chunk_size - 1, last)
        # substring(bytea FROM start FOR length), SQL positions are 1-based.
        chunk = await conn.fetchval(
            "SELECT substring(content FROM $2 FOR $3) FROM revisions WHERE id = $1",
            revision_id,
            pos + 1,
            end - pos + 1,
        )
        if not chunk:
            break
        yield bytes(chunk)
        pos = end + 1


async def publish_revision(
    conn: asyncpg.Connection,
    path: str,
    content: bytes,
    media_type: str,
) -> tuple[str, int, datetime]:
    """Insert a new revision and point the object at it (one transaction).

    Returns (sha256 hex, revision number, creation time).  The SHA-256 is
    computed in Python so it is available for the response ETag without a
    second round trip.  Callers must already hold the FOR UPDATE lock on
    the objects row (see lock_new_or_existing_object).
    """
    digest = hashlib.sha256(content).hexdigest()
    revision_no = await conn.fetchval(
        "SELECT COALESCE(max(revision_no), 0) + 1 FROM revisions WHERE object_path = $1",
        path,
    )
    new_id = await conn.fetchval(
        """
        INSERT INTO revisions (object_path, revision_no, content, sha256, media_type)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """,
        path,
        revision_no,
        content,
        digest,
        media_type,
    )
    await conn.execute(
        "UPDATE objects SET current_id = $2 WHERE path = $1", path, new_id
    )
    created_at = await conn.fetchval(
        "SELECT created_at FROM revisions WHERE id = $1", new_id
    )
    return digest, revision_no, created_at


# ---- copy sessions -----------------------------------------------------------

COPY_SECRET_KEY = "copy_cursor_secret"


@dataclass(frozen=True, slots=True)
class CopySession:
    id: str
    target_prefix: str
    source_hash: str
    manifest_sha256: str
    segment_size: int
    total_entries: int
    total_segments: int
    next_segment: int
    created_at: datetime


@dataclass(frozen=True, slots=True)
class CopyEntry:
    ordinal: int
    segment_no: int
    source_path: str
    target_path: str
    revision_id: int
    revision_no: int
    sha256: str
    media_type: str
    length: int


@dataclass(frozen=True, slots=True)
class TargetBaseline:
    """Revision identity of a target path captured at session creation.

    ``revision_id is None`` means the target had no current revision
    (its object row was absent or pointed at no revision).  The id --
    never the digest -- is the identity compared on acknowledgement, so
    re-publishing identical bytes after the baseline is detectable.
    """

    ordinal: int
    target_path: str
    revision_id: int | None
    revision_no: int | None
    sha256: str | None


async def ensure_copy_secret(pool: asyncpg.Pool) -> str:
    """Return the HMAC key that signs copy cursors, creating it once.

    The secret lives in the database so cursors stay valid across
    restarts; concurrent first-boots converge on whichever insert wins.
    """
    async with pool.acquire() as conn:
        secret = await conn.fetchval(
            "SELECT value FROM service_meta WHERE key = $1", COPY_SECRET_KEY
        )
        if secret:
            return secret
        await conn.execute(
            "INSERT INTO service_meta (key, value) VALUES ($1, $2)"
            " ON CONFLICT (key) DO NOTHING",
            COPY_SECRET_KEY,
            secrets.token_hex(32),
        )
        return await conn.fetchval(
            "SELECT value FROM service_meta WHERE key = $1", COPY_SECRET_KEY
        )


async def snapshot_source_revisions(
    conn: asyncpg.Connection, paths: list[str]
) -> list[asyncpg.Record]:
    """Lock the source set boundary and read current revisions, one snapshot.

    A single ``SELECT ... FOR SHARE`` statement: every matching objects
    row is locked against concurrent publishes until the caller commits,
    and all rows are read from the same statement snapshot, ordered by
    stable path and revision order.
    """
    return await conn.fetch(
        """
        SELECT o.path AS source_path, r.id AS revision_id, r.revision_no,
               r.sha256, r.media_type, octet_length(r.content) AS length
          FROM objects o
          JOIN revisions r ON r.id = o.current_id
         WHERE o.path = ANY($1::text[])
         ORDER BY o.path ASC, r.revision_no ASC
         FOR SHARE OF o
        """,
        paths,
    )


async def lock_path_set(conn: asyncpg.Connection, paths: list[str]) -> None:
    """Ensure object rows exist for every path and share-lock them.

    Placeholder rows (``current_id`` NULL) are inserted first so even
    paths with no revision yet get a real row to lock: a concurrent
    publisher of such a path then blocks until this transaction commits
    instead of slipping an invisible INSERT past the baseline snapshot.
    All rows -- source and target -- are locked in one ascending-order
    statement, which is the same global order acknowledgements use for
    their ``FOR UPDATE`` locks, so creation and application cannot
    deadlock against each other.
    """
    await conn.executemany(
        "INSERT INTO objects (path) VALUES ($1) ON CONFLICT (path) DO NOTHING",
        [(p,) for p in paths],
    )
    await conn.execute(
        """
        SELECT o.path
          FROM objects o
         WHERE o.path = ANY($1::text[])
         ORDER BY o.path ASC
         FOR SHARE OF o
        """,
        paths,
    )


async def snapshot_target_baselines(
    conn: asyncpg.Connection, target_paths: list[str]
) -> dict[str, asyncpg.Record]:
    """Read current revision identity of every target path.

    Callers must have already share-locked the rows (``lock_path_set``);
    the LEFT JOIN keeps target paths without a revision, represented by a
    record whose revision columns are NULL.
    """
    rows = await conn.fetch(
        """
        SELECT o.path AS target_path, r.id AS revision_id,
               r.revision_no, r.sha256
          FROM objects o
          LEFT JOIN revisions r ON r.id = o.current_id
         WHERE o.path = ANY($1::text[])
        """,
        target_paths,
    )
    return {r["target_path"]: r for r in rows}


async def insert_copy_session(
    conn: asyncpg.Connection,
    session_id: str,
    target_prefix: str,
    source_hash: str,
    manifest_sha256: str,
    segment_size: int,
    entries: list[CopyEntry],
    baselines: list[TargetBaseline],
) -> None:
    """Persist the session header, manifest entries and target baselines
    (one transaction with the snapshot that produced them)."""
    total_entries = len(entries)
    total_segments = (
        (total_entries + segment_size - 1) // segment_size
        if total_entries
        else 0
    )
    await conn.execute(
        """
        INSERT INTO copy_sessions
            (id, target_prefix, source_hash, manifest_sha256,
             segment_size, total_entries, total_segments)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        session_id,
        target_prefix,
        source_hash,
        manifest_sha256,
        segment_size,
        total_entries,
        total_segments,
    )
    await conn.executemany(
        """
        INSERT INTO copy_session_entries
            (session_id, ordinal, segment_no, source_path, target_path,
             revision_id, revision_no, sha256, media_type, length)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
        [
            (
                session_id,
                e.ordinal,
                e.segment_no,
                e.source_path,
                e.target_path,
                e.revision_id,
                e.revision_no,
                e.sha256,
                e.media_type,
                e.length,
            )
            for e in entries
        ],
    )
    await conn.executemany(
        """
        INSERT INTO copy_session_target_baselines
            (session_id, ordinal, target_path, revision_id,
             revision_no, sha256)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        [
            (
                session_id,
                b.ordinal,
                b.target_path,
                b.revision_id,
                b.revision_no,
                b.sha256,
            )
            for b in baselines
        ],
    )


def _to_copy_session(row: asyncpg.Record) -> CopySession:
    return CopySession(
        id=row["id"],
        target_prefix=row["target_prefix"],
        source_hash=row["source_hash"],
        manifest_sha256=row["manifest_sha256"],
        segment_size=row["segment_size"],
        total_entries=row["total_entries"],
        total_segments=row["total_segments"],
        next_segment=row["next_segment"],
        created_at=row["created_at"],
    )


async def get_copy_session(
    conn: asyncpg.Connection, session_id: str
) -> CopySession | None:
    row = await conn.fetchrow(
        """
        SELECT id, target_prefix, source_hash, manifest_sha256,
               segment_size, total_entries, total_segments, next_segment,
               created_at
          FROM copy_sessions
         WHERE id = $1
        """,
        session_id,
    )
    return None if row is None else _to_copy_session(row)


def _to_copy_entry(row: asyncpg.Record) -> CopyEntry:
    return CopyEntry(
        ordinal=row["ordinal"],
        segment_no=row["segment_no"],
        source_path=row["source_path"],
        target_path=row["target_path"],
        revision_id=row["revision_id"],
        revision_no=row["revision_no"],
        sha256=row["sha256"],
        media_type=row["media_type"],
        length=row["length"],
    )


async def fetch_segment_entries(
    conn: asyncpg.Connection, session_id: str, segment_no: int
) -> list[CopyEntry]:
    """Manifest entries of one segment, in stable manifest order."""
    rows = await conn.fetch(
        """
        SELECT ordinal, segment_no, source_path, target_path, revision_id,
               revision_no, sha256, media_type, length
          FROM copy_session_entries
         WHERE session_id = $1 AND segment_no = $2
         ORDER BY ordinal
        """,
        session_id,
        segment_no,
    )
    return [_to_copy_entry(r) for r in rows]


def _to_target_baseline(row: asyncpg.Record) -> TargetBaseline:
    return TargetBaseline(
        ordinal=row["ordinal"],
        target_path=row["target_path"],
        revision_id=row["revision_id"],
        revision_no=row["revision_no"],
        sha256=row["sha256"],
    )


async def session_has_baselines(
    conn: asyncpg.Connection, session_id: str
) -> bool:
    """Whether this session carries target revision baselines.

    Sessions created before the baseline upgrade have no rows here; their
    unfinished segments must not be acknowledged on digest evidence
    alone.
    """
    return await conn.fetchval(
        "SELECT EXISTS ("
        "SELECT 1 FROM copy_session_target_baselines WHERE session_id = $1)",
        session_id,
    )


async def fetch_segment_baselines(
    conn: asyncpg.Connection, session_id: str, segment_no: int
) -> dict[str, TargetBaseline]:
    """Baselines for the targets of one segment, keyed by target path."""
    rows = await conn.fetch(
        """
        SELECT b.ordinal, b.target_path, b.revision_id,
               b.revision_no, b.sha256
          FROM copy_session_target_baselines b
          JOIN copy_session_entries e
            ON e.session_id = b.session_id
           AND e.target_path = b.target_path
         WHERE b.session_id = $1 AND e.segment_no = $2
        """,
        session_id,
        segment_no,
    )
    return {r["target_path"]: _to_target_baseline(r) for r in rows}


async def fetch_revision_content(
    conn: asyncpg.Connection, revision_id: int
) -> bytes:
    """Full content of a pinned revision (revisions are immutable)."""
    content = await conn.fetchval(
        "SELECT content FROM revisions WHERE id = $1", revision_id
    )
    return bytes(content)


async def fetch_ack(
    conn: asyncpg.Connection, session_id: str, segment_no: int
) -> tuple[str, dict] | None:
    """Stored batch result for a segment, if it was already acknowledged."""
    row = await conn.fetchrow(
        """
        SELECT status, result FROM copy_session_acks
         WHERE session_id = $1 AND segment_no = $2
        """,
        session_id,
        segment_no,
    )
    if row is None:
        return None
    return row["status"], json.loads(row["result"])


def _canonical_json(result: dict) -> str:
    """Serialise exactly like ``JSONResponse`` (compact, insertion order),
    so a stored result replays byte-identically to the original response."""
    return json.dumps(result, ensure_ascii=False, separators=(",", ":"))


async def insert_ack(
    conn: asyncpg.Connection,
    session_id: str,
    segment_no: int,
    cursor: str,
    status: str,
    result: dict,
) -> bool:
    """Persist the batch result; False when a concurrent ack already did.

    ``ON CONFLICT DO NOTHING`` makes concurrent acknowledgements of the
    same segment converge on the first stored result instead of failing
    with a unique-violation.
    """
    row = await conn.fetchrow(
        """
        INSERT INTO copy_session_acks
            (session_id, segment_no, cursor, status, result)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (session_id, segment_no) DO NOTHING
        RETURNING segment_no
        """,
        session_id,
        segment_no,
        cursor,
        status,
        _canonical_json(result),
    )
    return row is not None


async def advance_session(
    conn: asyncpg.Connection, session_id: str, next_segment: int
) -> None:
    await conn.execute(
        "UPDATE copy_sessions SET next_segment = $2 WHERE id = $1",
        session_id,
        next_segment,
    )


async def apply_copy_entries(
    conn: asyncpg.Connection,
    entries: list[CopyEntry],
    baselines: dict[str, TargetBaseline],
) -> tuple[list[dict], list[dict]]:
    """Apply one segment to the target prefix inside the caller's transaction.

    Target rows are locked in ascending path order (``FOR UPDATE``), so
    concurrent batches and publishes serialise without deadlocks.  Each
    entry is checked against the revision baseline captured when the
    session was created:

    * baseline missing the revision and target still missing -> create
      the pinned revision as rev 1;
    * baseline revision identity unchanged and digest equal ->
      idempotent no-op (``already_present``);
    * anything else is a conflict -- including a re-publish of the very
      same bytes, which a digest comparison could not distinguish from
      "never changed".

    The caller decides: commit everything, or roll back and report the
    conflicts — never a partial application.
    """
    applied: list[dict] = []
    conflicts: list[dict] = []
    for entry in sorted(entries, key=lambda e: e.target_path):
        baseline = baselines[entry.target_path]
        # Upsert first so concurrent creators serialise on the same row.
        await conn.execute(
            "INSERT INTO objects (path) VALUES ($1)"
            " ON CONFLICT (path) DO NOTHING",
            entry.target_path,
        )
        row = await conn.fetchrow(
            """
            SELECT r.id, r.revision_no, r.sha256
              FROM objects o
              JOIN revisions r ON r.id = o.current_id
             WHERE o.path = $1
             FOR UPDATE OF o
            """,
            entry.target_path,
        )

        if row is None:
            if baseline.revision_id is None:
                # Baseline said "absent" and it still is: copy pinned rev 1.
                new_id = await conn.fetchval(
                    """
                    INSERT INTO revisions
                        (object_path, revision_no, content, sha256, media_type)
                    SELECT $1, 1, content, sha256, media_type
                      FROM revisions WHERE id = $2
                    RETURNING id
                    """,
                    entry.target_path,
                    entry.revision_id,
                )
                await conn.execute(
                    "UPDATE objects SET current_id = $2 WHERE path = $1",
                    entry.target_path,
                    new_id,
                )
                applied.append(
                    {
                        "target_path": entry.target_path,
                        "source_path": entry.source_path,
                        "revision_no": 1,
                        "sha256": entry.sha256,
                        "action": "created",
                    }
                )
            else:
                # Existed at baseline, current revision gone since.
                conflicts.append(
                    {
                        "target_path": entry.target_path,
                        "source_path": entry.source_path,
                        "reason": "target_changed_after_baseline",
                        "expected_sha256": entry.sha256,
                        "actual_sha256": None,
                        "actual_revision_no": None,
                        "baseline_revision_no": baseline.revision_no,
                        "baseline_sha256": baseline.sha256,
                    }
                )
            continue

        if row["id"] == baseline.revision_id:
            # Same revision identity as at baseline: nothing was published
            # here since the session was pinned.
            if row["sha256"] == entry.sha256:
                applied.append(
                    {
                        "target_path": entry.target_path,
                        "source_path": entry.source_path,
                        "revision_no": row["revision_no"],
                        "sha256": entry.sha256,
                        "action": "already_present",
                    }
                )
            else:
                # Cannot happen on immutable revisions, but never apply
                # over content that contradicts the pinned digest.
                conflicts.append(
                    {
                        "target_path": entry.target_path,
                        "source_path": entry.source_path,
                        "reason": "content_diverged",
                        "expected_sha256": entry.sha256,
                        "actual_sha256": row["sha256"],
                        "actual_revision_no": row["revision_no"],
                        "baseline_revision_no": baseline.revision_no,
                        "baseline_sha256": baseline.sha256,
                    }
                )
            continue

        # A different revision identity: the target was published after
        # the baseline -- even byte-identical content counts as a change.
        reason = (
            "target_republished_same_content"
            if row["sha256"] == entry.sha256
            else "content_diverged"
        )
        conflicts.append(
            {
                "target_path": entry.target_path,
                "source_path": entry.source_path,
                "reason": reason,
                "expected_sha256": entry.sha256,
                "actual_sha256": row["sha256"],
                "actual_revision_no": row["revision_no"],
                "baseline_revision_no": baseline.revision_no,
                "baseline_sha256": baseline.sha256,
            }
        )
    return applied, conflicts
