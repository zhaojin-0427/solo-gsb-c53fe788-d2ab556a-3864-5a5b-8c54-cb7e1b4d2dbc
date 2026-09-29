"""PostgreSQL access: connection pool, revision lookup and publishing.

All conditional decisions run inside a transaction that locks the
``objects`` row of the path (``SELECT ... FOR SHARE`` for reads,
``FOR UPDATE`` for writes).  Concurrent publishes therefore serialise on
that row, and a streaming GET keeps its share lock until the body has
been fully sent, so the revision selected for the headers can neither
disappear nor be replaced while bytes are streamed.

Copy sessions reuse the same building blocks: the manifest and the
target revision-identity baselines are captured together in one
REPEATABLE READ transaction (a ``FOR SHARE`` snapshot statement for the
sources and one for the targets), and acknowledging a segment applies
its entries inside a single transaction that first takes ``FOR UPDATE``
locks on the target rows in ascending path order (so concurrent
acknowledgements cannot deadlock) and then compares each target's
current revision identity against the stored baseline.
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
class TargetBaseline:
    """Revision identity of a target path as of the session snapshot.

    ``revision_id is None`` pins the state "the target path had no
    current revision yet".  Any current revision at acknowledge time is
    then proof of a publish after the baseline was taken.
    """

    target_path: str
    revision_id: int | None
    revision_no: int | None
    sha256: str | None


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
    baseline: TargetBaseline | None = None


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


async def snapshot_target_baselines(
    conn: asyncpg.Connection, target_paths: list[str]
) -> dict[str, TargetBaseline]:
    """Capture target revision identities from the transaction snapshot.

    Run in the *same transaction* (and, under REPEATABLE READ, the same
    statement snapshot) as :func:`snapshot_source_revisions`.  Existing
    objects rows are ``FOR SHARE`` locked so a concurrent publish blocks
    until the session (and its baseline) has been committed; paths with
    no objects row at all are recorded as a ``None`` revision identity
    ("target did not exist yet").
    """
    rows = await conn.fetch(
        """
        SELECT o.path AS target_path, r.id AS revision_id,
               r.revision_no, r.sha256
          FROM unnest($1::text[]) WITH ORDINALITY AS t(path, ord)
          JOIN objects o ON o.path = t.path
          LEFT JOIN revisions r ON r.id = o.current_id
         ORDER BY t.ord
         FOR SHARE OF o
        """,
        target_paths,
    )
    baselines: dict[str, TargetBaseline] = {}
    for row in rows:
        revision_id = row["revision_id"]
        baselines[row["target_path"]] = TargetBaseline(
            target_path=row["target_path"],
            revision_id=None if revision_id is None else revision_id,
            revision_no=row["revision_no"],
            sha256=row["sha256"],
        )
    # Absent rows carry no lock and no revision identity.
    for path in target_paths:
        baselines.setdefault(path, TargetBaseline(path, None, None, None))
    return baselines


async def insert_copy_session(
    conn: asyncpg.Connection,
    session_id: str,
    target_prefix: str,
    source_hash: str,
    manifest_sha256: str,
    segment_size: int,
    entries: list[CopyEntry],
) -> None:
    """Persist the session header and its manifest entries (one transaction
    with the snapshot that produced them)."""
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
             revision_id, revision_no, sha256, media_type, length,
             baseline_captured, baseline_revision_id,
             baseline_revision_no, baseline_sha256)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
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
                e.baseline is not None,
                None if e.baseline is None else e.baseline.revision_id,
                None if e.baseline is None else e.baseline.revision_no,
                None if e.baseline is None else e.baseline.sha256,
            )
            for e in entries
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
    baseline = None
    if row["baseline_captured"]:
        baseline = TargetBaseline(
            target_path=row["target_path"],
            revision_id=row["baseline_revision_id"],
            revision_no=row["baseline_revision_no"],
            sha256=row["baseline_sha256"],
        )
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
        baseline=baseline,
    )


async def fetch_segment_entries(
    conn: asyncpg.Connection, session_id: str, segment_no: int
) -> list[CopyEntry]:
    """Manifest entries of one segment, in stable manifest order."""
    rows = await conn.fetch(
        """
        SELECT ordinal, segment_no, source_path, target_path, revision_id,
               revision_no, sha256, media_type, length,
               baseline_captured, baseline_revision_id,
               baseline_revision_no, baseline_sha256
          FROM copy_session_entries
         WHERE session_id = $1 AND segment_no = $2
         ORDER BY ordinal
        """,
        session_id,
        segment_no,
    )
    return [_to_copy_entry(r) for r in rows]


async def segment_baseline_captured(
    conn: asyncpg.Connection, session_id: str, segment_no: int
) -> bool:
    """Whether every entry of the segment carries a revision baseline.

    Upgrade-era sessions (created before baselines existed) have rows
    with ``baseline_captured = FALSE``; their pending segments may not be
    acknowledged by digest comparison alone.
    """
    value = await conn.fetchval(
        """
        SELECT COALESCE(bool_and(baseline_captured), TRUE)
          FROM copy_session_entries
         WHERE session_id = $1 AND segment_no = $2
        """,
        session_id,
        segment_no,
    )
    return bool(value)


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
    conn: asyncpg.Connection, entries: list[CopyEntry]
) -> tuple[list[dict], list[dict]]:
    """Apply one segment to the target prefix inside the caller's transaction.

    Every entry must carry the revision-identity baseline captured when
    the session was created.  Target rows are locked in ascending path
    order (``FOR UPDATE``), so concurrent batches and publishes serialise
    without deadlocks.  Classification compares revision *identity*
    against the baseline — not content digests — because a same-bytes
    re-publish after the session was created still changes identity and
    must be reported rather than silently skipped:

    * **target absent at baseline, still absent** -> create revision 1;
    * **target present at baseline, identity unchanged** ->
      ``already_present`` (idempotent no-op; at baseline time the
      content already equalled the pinned content);
    * **any publish since the baseline** (identity changed, or a path
      that was absent now exists — even byte-identical) -> conflict
      ``published_after_session``;
    * **identity unchanged but the baseline content already diverged**
      from the pinned content -> conflict ``diverged_at_session``.

    Locking and classification finish for the whole batch before any
    revision is written, and the caller rolls back on any conflict, so
    there is never a partial application.
    """
    ordered = sorted(entries, key=lambda e: e.target_path)

    # Phase 1: materialise and lock every target row in path order, then
    # read its current revision identity under the FOR UPDATE lock.
    for entry in ordered:
        await conn.execute(
            "INSERT INTO objects (path) VALUES ($1)"
            " ON CONFLICT (path) DO NOTHING",
            entry.target_path,
        )
    currents: dict[str, asyncpg.Record | None] = {}
    for entry in ordered:
        currents[entry.target_path] = await conn.fetchrow(
            """
            SELECT r.id AS revision_id, r.revision_no, r.sha256
              FROM objects o
              JOIN revisions r ON r.id = o.current_id
             WHERE o.path = $1
             FOR UPDATE OF o
            """,
            entry.target_path,
        )

    # Phase 2: classify against the baseline before writing anything.
    conflicts: list[dict] = []
    create_entries: list[CopyEntry] = []   # absent -> create
    present_entries: list[CopyEntry] = []  # identity unchanged
    for entry in ordered:
        baseline = entry.baseline
        if baseline is None:
            # Defence in depth: the ack handler gates these out.
            raise RuntimeError(
                f"entry for {entry.target_path} has no revision baseline"
            )
        row = currents[entry.target_path]
        current_id = None if row is None else row["revision_id"]

        if baseline.revision_id is None and current_id is not None:
            conflicts.append(
                _conflict(
                    entry,
                    row,
                    reason="published_after_session",
                    detail="target path did not exist when the session"
                    " was created but a revision has been published since",
                )
            )
        elif (
            baseline.revision_id is not None
            and current_id != baseline.revision_id
        ):
            conflicts.append(
                _conflict(
                    entry,
                    row,
                    reason="published_after_session",
                    detail="target revision changed after the session"
                    " baseline was captured",
                )
            )
        elif current_id is None:
            create_entries.append(entry)  # absent then and now
        elif baseline.sha256 == entry.sha256:
            present_entries.append(entry)  # same content, same identity
        else:
            conflicts.append(
                _conflict(
                    entry,
                    row,
                    reason="diverged_at_session",
                    detail="target already held different content when"
                    " the session was created",
                )
            )

    if conflicts:
        return [], conflicts

    # Phase 3: all clear — create the missing targets; the rest are
    # idempotent no-ops.  Results are emitted in ascending target order.
    applied: list[dict] = []
    for entry in create_entries:
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
    for entry in present_entries:
        row = currents[entry.target_path]
        applied.append(
            {
                "target_path": entry.target_path,
                "source_path": entry.source_path,
                "revision_no": row["revision_no"],
                "sha256": entry.sha256,
                "action": "already_present",
            }
        )
    applied.sort(key=lambda a: a["target_path"])
    return applied, []


def _conflict(entry: CopyEntry, row: asyncpg.Record | None, *,
              reason: str, detail: str) -> dict:
    baseline = entry.baseline
    return {
        "target_path": entry.target_path,
        "source_path": entry.source_path,
        "reason": reason,
        "detail": detail,
        "expected_sha256": entry.sha256,
        "actual_sha256": None if row is None else row["sha256"],
        "actual_revision_no": None if row is None else row["revision_no"],
        "baseline": {
            "existed": baseline.revision_id is not None,
            "revision_id": baseline.revision_id,
            "revision_no": baseline.revision_no,
            "sha256": baseline.sha256,
        },
    }
