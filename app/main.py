"""FastAPI origin application.

Request flow for GET/HEAD/PUT on ``/objects/{path}``:

1. open a transaction and lock the object row (FOR SHARE for safe
   methods, FOR UPDATE for publishes);
2. evaluate RFC 9110 13.2.2 preconditions against the locked, current
   revision (304 / 412 / 404 short-circuit here);
3. only then parse Range / If-Range and decide 200 / 206 / 416;
4. stream the response body from the very revision selected in step 1,
   keeping the transaction (and its locks) open until the last byte.

Resumable copy sessions (``/copy-sessions``) add snapshot replication on
top: a session pins a manifest of source revisions in one database
snapshot, serves it in cursor-addressed segments that are byte-identical
on every pull, and applies each acknowledged segment to the target
prefix as a single atomic batch.
"""
from __future__ import annotations

import base64
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from . import db as database
from .config import COPY_MAX_SEGMENT_SIZE, COPY_SEGMENT_SIZE, DATABASE_URL
from .copy_sessions import (
    CopyCursor,
    decode_cursor,
    encode_cursor,
    manifest_hash,
    segment_digest,
    source_set_hash,
)
from .dates import http_date, parse_http_date
from .etags import make_etag
from .preconditions import (
    Outcome,
    RequestConditions,
    SelectedRepresentation,
    evaluate,
)
from .ranges import (
    ByteRange,
    MultipartPart,
    closing_delimiter,
    if_range_matches,
    multipart_boundary,
    multipart_content_length,
    multipart_content_type,
    parse_byte_ranges,
)

DEFAULT_MEDIA_TYPE = "application/octet-stream"

# HTTP status replayed alongside a stored copy-session batch result.
ACK_STATUS_CODES = {"applied": 200, "conflict": 409}


@dataclass(slots=True)
class Plan:
    status: int
    headers: dict[str, str]
    kind: str = "empty"            # empty | full | single | multipart
    ranges: list[ByteRange] | None = None
    boundary: str | None = None


def validator_headers(rev) -> dict[str, str]:
    return {
        "ETag": str(make_etag(rev.sha256)),
        "Last-Modified": http_date(rev.created_at),
    }


def build_plan(rev, request: Request) -> Plan:
    """Translate request + locked revision into a response plan.

    Precondition evaluation happens before this is called; Range /
    If-Range handling lives here (preconditions first, ranges second).
    """
    etag = make_etag(rev.sha256)
    headers = validator_headers(rev)

    def entity_headers() -> dict[str, str]:
        h = dict(headers)
        h["Accept-Ranges"] = "bytes"
        return h

    def full_plan() -> Plan:
        h = entity_headers()
        h["Content-Type"] = rev.media_type
        h["Content-Length"] = str(rev.length)
        return Plan(status=200, headers=h, kind="full")

    range_raw = request.headers.get("range")
    if not range_raw:
        return full_plan()

    ranges = parse_byte_ranges(range_raw, rev.length)
    if ranges is None:
        # Malformed Range: ignore it and send the whole representation.
        return full_plan()

    # If-Range: on mismatch, fall back to the full representation.
    if_range_raw = request.headers.get("if-range")
    if if_range_raw is not None:
        if not if_range_matches(if_range_raw, etag, rev.created_at):
            return full_plan()

    if not ranges:
        # Every requested range is unsatisfiable.
        h = entity_headers()
        h["Content-Range"] = f"bytes */{rev.length}"
        return Plan(status=416, headers=h)

    h = entity_headers()
    if len(ranges) == 1:
        rng = ranges[0]
        h["Content-Type"] = rev.media_type
        h["Content-Range"] = rng.content_range(rev.length)
        h["Content-Length"] = str(rng.length)
        return Plan(status=206, headers=h, kind="single", ranges=ranges)

    boundary = multipart_boundary(etag, ranges, rev.media_type)
    total = multipart_content_length(ranges, rev.length, rev.media_type, boundary)
    h["Content-Type"] = multipart_content_type(boundary)
    h["Content-Length"] = str(total)
    return Plan(
        status=206, headers=h, kind="multipart",
        ranges=ranges, boundary=boundary,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Read the DSN at startup so tests / containers can inject it via env.
    dsn = os.environ.get("DATABASE_URL", DATABASE_URL)
    app.state.pool = await database.create_pool(dsn)
    app.state.copy_secret = await database.ensure_copy_secret(app.state.pool)
    try:
        yield
    finally:
        await app.state.pool.close()


app = FastAPI(title="Conditional Request Object Origin", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


def _conditions_from_request(method: str, request: Request) -> RequestConditions:
    return RequestConditions(
        method=method,
        if_match_raw=request.headers.get("if-match"),
        if_none_match_raw=request.headers.get("if-none-match"),
        if_unmodified_since=parse_http_date(request.headers["if-unmodified-since"])
        if request.headers.get("if-unmodified-since")
        else None,
        if_modified_since=parse_http_date(request.headers["if-modified-since"])
        if request.headers.get("if-modified-since")
        else None,
    )


def _selected(rev) -> SelectedRepresentation:
    if rev is None:
        return SelectedRepresentation(exists=False)
    return SelectedRepresentation(
        exists=True,
        etag=make_etag(rev.sha256),
        last_modified=rev.created_at,
    )


@app.api_route("/objects/{path:path}", methods=["GET", "HEAD"])
async def read_object(path: str, request: Request):
    pool = request.app.state.pool
    conn = await pool.acquire()
    tx = conn.transaction()
    await tx.start()
    streaming = False
    try:
        rev = await database.current_revision(conn, path, for_update=False)
        conditions = _conditions_from_request(request.method, request)

        # Preconditions first (RFC 9110 13.2.2), ranges afterwards.
        outcome = evaluate(conditions, _selected(rev))
        if outcome is Outcome.PRECONDITION_FAILED:
            resp_headers = validator_headers(rev) if rev else {}
            await tx.rollback()
            return JSONResponse(
                {"detail": "Precondition Failed"},
                status_code=412, headers=resp_headers,
            )
        if rev is None:
            await tx.rollback()
            return JSONResponse({"detail": "Not Found"}, status_code=404)
        if outcome is Outcome.NOT_MODIFIED:
            headers = validator_headers(rev)
            await tx.commit()
            return Response(status_code=304, headers=headers)

        plan = build_plan(rev, request)
        if plan.status == 416:
            await tx.rollback()
            return Response(status_code=416, headers=plan.headers)

        if request.method == "HEAD":
            # Headers already describe the entity/part; no body is sent.
            await tx.commit()
            return Response(status_code=plan.status, headers=plan.headers)

        if plan.kind == "full":
            async def full_body() -> AsyncIterator[bytes]:
                try:
                    async for chunk in database.stream_revision(
                        conn, rev.id, 0, rev.length - 1
                    ):
                        yield chunk
                    await tx.commit()
                except Exception:
                    await tx.rollback()
                    raise
                finally:
                    await pool.release(conn)

            streaming = True
            return StreamingResponse(
                full_body(), status_code=plan.status, headers=plan.headers
            )

        if plan.kind == "single":
            rng = plan.ranges[0]

            async def single_body() -> AsyncIterator[bytes]:
                try:
                    async for chunk in database.stream_revision(
                        conn, rev.id, rng.first, rng.last
                    ):
                        yield chunk
                    await tx.commit()
                except Exception:
                    await tx.rollback()
                    raise
                finally:
                    await pool.release(conn)

            streaming = True
            return StreamingResponse(
                single_body(), status_code=206, headers=plan.headers
            )

        # multipart/byteranges
        ranges = plan.ranges
        boundary = plan.boundary

        async def multipart_body() -> AsyncIterator[bytes]:
            try:
                for rng in ranges:
                    part = MultipartPart(
                        rng.first, rng.last, rev.length, rev.media_type
                    )
                    yield part.header_block(boundary)
                    async for chunk in database.stream_revision(
                        conn, rev.id, rng.first, rng.last
                    ):
                        yield chunk
                    yield b"\r\n"
                yield closing_delimiter(boundary)
                await tx.commit()
            except Exception:
                await tx.rollback()
                raise
            finally:
                await pool.release(conn)

        streaming = True
        return StreamingResponse(
            multipart_body(), status_code=206, headers=plan.headers
        )
    finally:
        if not streaming:
            await pool.release(conn)


@app.put("/objects/{path:path}")
async def publish_object(path: str, request: Request):
    body = await request.body()
    media_type = request.headers.get("content-type", DEFAULT_MEDIA_TYPE)

    pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            # FOR UPDATE: serialise concurrent publishers on this path.
            rev = await database.lock_new_or_existing_object(conn, path)
            conditions = _conditions_from_request("PUT", request)
            outcome = evaluate(conditions, _selected(rev))
            if outcome is not Outcome.PASS:
                # PUT + If-None-Match match, or If-Match/I-U-S failure.
                resp_headers = validator_headers(rev) if rev else {}
                return JSONResponse(
                    {"detail": "Precondition Failed"},
                    status_code=412, headers=resp_headers,
                )

            digest, revision_no, created_at = await database.publish_revision(
                conn, path, body, media_type
            )

    headers = {
        "ETag": str(make_etag(digest)),
        "Last-Modified": http_date(created_at),
        "X-Revision-No": str(revision_no),
    }
    return Response(status_code=204, headers=headers)


# ---- resumable copy sessions -------------------------------------------------


def _mint_cursor(secret: str, session, segment: int, last_path: str) -> str:
    """Cursor identifying ``segment`` of ``session`` (deterministic)."""
    return encode_cursor(
        secret,
        CopyCursor(
            session_id=session.id,
            segment=segment,
            manifest_sha256=session.manifest_sha256,
            last_path=last_path,
            target_prefix=session.target_prefix,
            source_hash=session.source_hash,
        ),
    )


async def _resume_payload(conn, secret: str, session) -> dict:
    """Recovery position: first un-acked segment + last confirmed cursor.

    ``last_confirmed_cursor`` is the token to hand to ``POST
    /copy-sessions`` when a session must be abandoned (conflict,
    failure): the new session resumes right after that segment.
    """
    next_segment = session.next_segment
    last_confirmed = None
    if next_segment > 0:
        entries = await database.fetch_segment_entries(
            conn, session.id, next_segment - 1
        )
        if entries:
            last_confirmed = _mint_cursor(
                secret, session, next_segment - 1, entries[-1].source_path
            )
    return {"next_segment": next_segment, "last_confirmed_cursor": last_confirmed}


async def _cursor_expired(conn, secret: str, session, detail: str) -> JSONResponse:
    return JSONResponse(
        {
            "detail": detail,
            "status": "cursor_expired",
            "session": session.id,
            "conflicts": [],
            "resume": await _resume_payload(conn, secret, session),
        },
        status_code=409,
    )


def _bad_request(detail: str) -> JSONResponse:
    return JSONResponse({"detail": detail}, status_code=400)


def _valid_cursor_for(secret: str, token: str, session) -> CopyCursor | None:
    """Decode a token and bind it to this session's immutable manifest."""
    cur = decode_cursor(secret, token)
    if (
        cur is None
        or cur.session_id != session.id
        or cur.manifest_sha256 != session.manifest_sha256
        or cur.segment >= session.total_segments
    ):
        return None
    return cur


@app.post("/copy-sessions", status_code=201)
async def create_copy_session(request: Request):
    try:
        body = await request.json()
    except Exception:
        return _bad_request("request body must be valid JSON")
    if not isinstance(body, dict):
        return _bad_request("request body must be a JSON object")

    source_paths = body.get("source_paths")
    if (
        not isinstance(source_paths, list)
        or not source_paths
        or not all(isinstance(p, str) and p for p in source_paths)
    ):
        return _bad_request("source_paths must be a non-empty list of paths")

    target_prefix = body.get("target_prefix", "")
    if not isinstance(target_prefix, str):
        return _bad_request("target_prefix must be a string")

    segment_size = body.get("segment_size")
    if segment_size is None:
        segment_size = COPY_SEGMENT_SIZE
    if (
        not isinstance(segment_size, int)
        or isinstance(segment_size, bool)
        or not 1 <= segment_size <= COPY_MAX_SEGMENT_SIZE
    ):
        return _bad_request(
            f"segment_size must be an integer in [1, {COPY_MAX_SEGMENT_SIZE}]"
        )

    resume_token = body.get("cursor")
    if resume_token is not None and not isinstance(resume_token, str):
        return _bad_request("cursor must be a string")

    # De-duplicate; the snapshot query returns stable path order anyway.
    paths = sorted(set(source_paths))
    source_hash = source_set_hash(paths)
    secret = request.app.state.copy_secret

    # Optional resume: continue right after the last confirmed segment of
    # a previous session over the same source set and target prefix.
    resume_after = None
    if resume_token is not None:
        cur = decode_cursor(secret, resume_token)
        if (
            cur is None
            or cur.target_prefix != target_prefix
            or cur.source_hash != source_hash
        ):
            return JSONResponse(
                {
                    "detail": "cursor_expired",
                    "status": "cursor_expired",
                    "conflicts": [],
                    "resume": None,
                },
                status_code=409,
            )
        resume_after = cur.last_path

    pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Manifest shape depends only on request parameters, not the
            # database, so build it before taking locks.
            provisional = [
                (p, target_prefix + p)
                for p in paths
                if resume_after is None or p > resume_after
            ]
            target_paths = [t for _, t in provisional]

            # Freeze every involved path in one globally sorted lock set:
            # placeholders are created for targets without a revision, and
            # the same ascending order acknowledgements use for FOR UPDATE
            # guarantees creation cannot deadlock with a concurrent apply.
            await database.lock_path_set(
                conn, sorted(set(paths + target_paths))
            )

            # Both snapshots are now read inside the held locks, so the
            # pinned source revisions and the target baselines come from
            # one frozen boundary (a concurrent publish blocks until
            # commit and cannot sneak between the two reads).
            rows = await database.snapshot_source_revisions(conn, paths)
            found = {r["source_path"] for r in rows}
            missing = [p for p in paths if p not in found]
            if missing:
                return JSONResponse(
                    {"detail": "source_not_found", "missing": missing},
                    status_code=404,
                )

            rev_by_path = {r["source_path"]: r for r in rows}
            entries: list[database.CopyEntry] = []
            for source_path, target_path in provisional:
                row = rev_by_path[source_path]
                entries.append(
                    database.CopyEntry(
                        ordinal=len(entries),
                        segment_no=len(entries) // segment_size,
                        source_path=source_path,
                        target_path=target_path,
                        revision_id=row["revision_id"],
                        revision_no=row["revision_no"],
                        sha256=row["sha256"],
                        media_type=row["media_type"],
                        length=row["length"],
                    )
                )

            baseline_rows = await database.snapshot_target_baselines(
                conn, target_paths
            )
            baselines = [
                database.TargetBaseline(
                    ordinal=e.ordinal,
                    target_path=e.target_path,
                    revision_id=baseline_rows[e.target_path]["revision_id"],
                    revision_no=baseline_rows[e.target_path]["revision_no"],
                    sha256=baseline_rows[e.target_path]["sha256"],
                )
                for e in entries
            ]

            session_id = uuid.uuid4().hex
            mhash = manifest_hash(entries)
            await database.insert_copy_session(
                conn,
                session_id,
                target_prefix,
                source_hash,
                mhash,
                segment_size,
                entries,
                baselines,
            )

    total_segments = (
        (len(entries) + segment_size - 1) // segment_size if entries else 0
    )
    return JSONResponse(
        {
            "session": session_id,
            "target_prefix": target_prefix,
            "manifest_sha256": mhash,
            "source_hash": source_hash,
            "segment_size": segment_size,
            "total_entries": len(entries),
            "total_segments": total_segments,
            "resumed_after": resume_after,
            "manifest": [
                {
                    "source_path": e.source_path,
                    "target_path": e.target_path,
                    "revision_no": e.revision_no,
                    "sha256": e.sha256,
                    "media_type": e.media_type,
                    "length": e.length,
                }
                for e in entries
            ],
        },
        status_code=201,
    )


@app.get("/copy-sessions/{session_id}")
async def describe_copy_session(session_id: str, request: Request):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        session = await database.get_copy_session(conn, session_id)
        if session is None:
            return JSONResponse({"detail": "session not found"}, status_code=404)
        resume = await _resume_payload(conn, request.app.state.copy_secret, session)
    return {
        "session": session.id,
        "target_prefix": session.target_prefix,
        "manifest_sha256": session.manifest_sha256,
        "source_hash": session.source_hash,
        "segment_size": session.segment_size,
        "total_entries": session.total_entries,
        "total_segments": session.total_segments,
        "next_segment": session.next_segment,
        "status": (
            "complete"
            if session.next_segment >= session.total_segments
            else "open"
        ),
        "resume": resume,
        "created_at": session.created_at.isoformat(),
    }


@app.get("/copy-sessions/{session_id}/segments")
async def pull_copy_segment(
    session_id: str, request: Request, cursor: str | None = None
):
    """Fetch the segment *after* ``cursor`` (the first when omitted).

    Segments are served from the persisted manifest, so repeated pulls
    return identical bytes, digests and cursors even if the source
    objects have published newer revisions since the session was created.
    """
    pool = request.app.state.pool
    secret = request.app.state.copy_secret
    async with pool.acquire() as conn:
        session = await database.get_copy_session(conn, session_id)
        if session is None:
            return JSONResponse({"detail": "session not found"}, status_code=404)

        if cursor is None:
            segment = 0
        else:
            cur = _valid_cursor_for(secret, cursor, session)
            if cur is None:
                return await _cursor_expired(conn, secret, session, "cursor_expired")
            segment = cur.segment + 1

        if segment >= session.total_segments:
            return JSONResponse(
                {
                    "detail": "no_more_segments",
                    "session": session.id,
                    "total_segments": session.total_segments,
                },
                status_code=404,
            )

        entries = await database.fetch_segment_entries(conn, session.id, segment)
        out_entries = []
        for entry in entries:
            content = await database.fetch_revision_content(conn, entry.revision_id)
            out_entries.append(
                {
                    "source_path": entry.source_path,
                    "target_path": entry.target_path,
                    "revision_no": entry.revision_no,
                    "sha256": entry.sha256,
                    "media_type": entry.media_type,
                    "length": entry.length,
                    "content_b64": base64.b64encode(content).decode("ascii"),
                }
            )
        return {
            "session": session.id,
            "segment": segment,
            "total_segments": session.total_segments,
            "cursor": _mint_cursor(
                secret, session, segment, entries[-1].source_path
            ),
            "has_more": segment + 1 < session.total_segments,
            "segment_sha256": segment_digest(entries),
            "entries": out_entries,
        }


@app.post("/copy-sessions/{session_id}/acks")
async def ack_copy_segment(session_id: str, request: Request):
    """Acknowledge one segment: apply it to the target prefix atomically.

    Targets are checked against the revision baselines captured at
    session creation, so any publish after the baseline -- even of the
    identical bytes -- is a 409 conflict.  On conflict or failure
    nothing is applied and a structured result (conflicting objects +
    resume position) is returned.  The result is persisted, so repeated
    or replayed acknowledgements return the very same batch result.
    """
    try:
        body = await request.json()
    except Exception:
        return _bad_request("request body must be valid JSON")
    token = body.get("cursor") if isinstance(body, dict) else None
    if not isinstance(token, str) or not token:
        return _bad_request("cursor is required")

    pool = request.app.state.pool
    secret = request.app.state.copy_secret
    async with pool.acquire() as conn:
        session = await database.get_copy_session(conn, session_id)
        if session is None:
            return JSONResponse({"detail": "session not found"}, status_code=404)

        cur = _valid_cursor_for(secret, token, session)
        if cur is None:
            return await _cursor_expired(conn, secret, session, "cursor_expired")

        # Replay: an acknowledged segment always yields its stored result.
        stored = await database.fetch_ack(conn, session.id, cur.segment)
        if stored is not None:
            status, result = stored
            return JSONResponse(result, status_code=ACK_STATUS_CODES[status])

        # Segments are acknowledged strictly in order; anything else is
        # an expired cursor from the client's point of view.
        if cur.segment != session.next_segment:
            return await _cursor_expired(conn, secret, session, "cursor_expired")

        # Sessions created before baselines existed have no revision
        # identity for their targets: a digest-only check cannot tell
        # "unchanged" from "re-published identical bytes", so unfinished
        # segments must be re-pinned as a new session instead of being
        # let through on matching hashes.  (Already-persisted acks were
        # replayed above and keep their original result.)
        if not await database.session_has_baselines(conn, session.id):
            return JSONResponse(
                {
                    "detail": "baseline_missing",
                    "status": "baseline_missing",
                    "session": session.id,
                    "segment": cur.segment,
                    "cursor": token,
                    "applied": [],
                    "conflicts": [],
                    "resume": await _resume_payload(conn, secret, session),
                },
                status_code=409,
            )

        entries = await database.fetch_segment_entries(conn, session.id, cur.segment)
        baselines = await database.fetch_segment_baselines(
            conn, session.id, cur.segment
        )
        resume_before = await _resume_payload(conn, secret, session)

        tx = conn.transaction()
        await tx.start()
        try:
            applied, conflicts = await database.apply_copy_entries(
                conn, entries, baselines
            )
        except Exception:
            await tx.rollback()
            return JSONResponse(
                {
                    "detail": "batch apply failed; no changes were committed",
                    "status": "failed",
                    "session": session.id,
                    "segment": cur.segment,
                    "cursor": token,
                    "applied": [],
                    "conflicts": [],
                    "resume": resume_before,
                },
                status_code=500,
            )

        if conflicts:
            # Roll back the whole batch, then persist the conflict result
            # on its own so replays return it verbatim.
            await tx.rollback()
            result = {
                "status": "conflict",
                "session": session.id,
                "segment": cur.segment,
                "cursor": token,
                "applied": [],
                "conflicts": conflicts,
                "resume": resume_before,
            }
            async with conn.transaction():
                inserted = await database.insert_ack(
                    conn, session.id, cur.segment, token, "conflict", result
                )
            if not inserted:
                stored = await database.fetch_ack(conn, session.id, cur.segment)
                if stored is not None:
                    status, result = stored
                    return JSONResponse(
                        result, status_code=ACK_STATUS_CODES[status]
                    )
            return JSONResponse(result, status_code=409)

        result = {
            "status": "applied",
            "session": session.id,
            "segment": cur.segment,
            "cursor": token,
            "applied": applied,
            "conflicts": [],
            "resume": {
                "next_segment": cur.segment + 1,
                "last_confirmed_cursor": token,
            },
        }
        # The batch, its ack record and the resume position commit together.
        inserted = await database.insert_ack(
            conn, session.id, cur.segment, token, "applied", result
        )
        if not inserted:
            # A concurrent ack of the same segment won; replay its result.
            await tx.rollback()
            stored = await database.fetch_ack(conn, session.id, cur.segment)
            if stored is not None:
                status, result = stored
                return JSONResponse(result, status_code=ACK_STATUS_CODES[status])
            return await _cursor_expired(conn, secret, session, "cursor_expired")
        await database.advance_session(conn, session.id, cur.segment + 1)
        await tx.commit()
        return JSONResponse(result, status_code=200)
