"""End-to-end tests for resumable copy sessions (/copy-sessions)."""
from __future__ import annotations

import base64
import hashlib
import json
import uuid

SHA_OF = lambda body: hashlib.sha256(body).hexdigest()
ETAG_OF = lambda body: '"' + SHA_OF(body) + '"'
json_dumps_bytes = lambda obj: json.dumps(
    obj, ensure_ascii=False, separators=(",", ":")
).encode()


def _prefix(name: str) -> str:
    return f"csrc/{name}-{uuid.uuid4().hex[:12]}"


def _put_sources(client, prefix, bodies: dict[str, bytes]) -> list[str]:
    """Publish bodies under unique object paths; return the source paths."""
    paths = []
    for name, body in bodies.items():
        p = f"/objects/{prefix}/{name}"
        r = client.put(p, content=body, headers={"Content-Type": "text/plain"})
        assert r.status_code == 204
        paths.append(p[len("/objects/"):])
    return paths


def _create_session(client, paths, target_prefix, segment_size=2, cursor=None):
    body = {
        "source_paths": paths,
        "target_prefix": target_prefix,
        "segment_size": segment_size,
    }
    if cursor is not None:
        body["cursor"] = cursor
    return client.post("/copy-sessions", json=body)


def _pull(client, session, cursor=None):
    params = {} if cursor is None else {"cursor": cursor}
    return client.get(f"/copy-sessions/{session}/segments", params=params)


def _ack(client, session, cursor):
    return client.post(f"/copy-sessions/{session}/acks", json={"cursor": cursor})


# ---- creation / manifest ------------------------------------------------------

def test_create_session_manifest_sorted_with_digests(client):
    prefix = _prefix("manifest")
    bodies = {"b.txt": b"bbb", "a.txt": b"aaa", "c.txt": b"ccc"}
    paths = _put_sources(client, prefix, bodies)

    r = _create_session(client, paths, "backup/", segment_size=2)
    assert r.status_code == 201
    data = r.json()
    assert data["total_entries"] == 3
    assert data["total_segments"] == 2
    assert data["segment_size"] == 2
    assert data["resumed_after"] is None
    # Stable path order regardless of request order.
    manifest = data["manifest"]
    assert [e["source_path"] for e in manifest] == sorted(paths)
    for entry in manifest:
        name = entry["source_path"].rsplit("/", 1)[1]
        assert entry["sha256"] == SHA_OF(bodies[name])
        assert entry["target_path"] == "backup/" + entry["source_path"]
        assert entry["revision_no"] == 1
        assert entry["length"] == len(bodies[name])
    assert len(data["manifest_sha256"]) == 64


def test_create_session_missing_source_is_404(client):
    prefix = _prefix("missing")
    paths = _put_sources(client, prefix, {"a.txt": b"aaa"})
    missing = f"{prefix}/nope.txt"
    r = _create_session(client, paths + [missing], "backup/")
    assert r.status_code == 404
    assert r.json()["missing"] == [missing]


def test_create_session_validation(client):
    assert client.post("/copy-sessions", json={}).status_code == 400
    assert (
        client.post("/copy-sessions", json={"source_paths": []}).status_code == 400
    )
    assert (
        client.post(
            "/copy-sessions",
            json={"source_paths": ["a"], "segment_size": 0},
        ).status_code
        == 400
    )
    assert client.post("/copy-sessions", content=b"not json").status_code == 400


def test_describe_session_and_404(client):
    prefix = _prefix("describe")
    paths = _put_sources(client, prefix, {"a.txt": b"aaa"})
    sid = _create_session(client, paths, "backup/").json()["session"]
    r = client.get(f"/copy-sessions/{sid}")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "open"
    assert data["next_segment"] == 0
    assert data["resume"] == {"next_segment": 0, "last_confirmed_cursor": None}
    assert client.get("/copy-sessions/" + "0" * 32).status_code == 404


# ---- segment pulls --------------------------------------------------------------

def test_pull_segments_repeatable_and_pinned(client):
    prefix = _prefix("pull")
    bodies = {"a.txt": b"alpha-v1", "b.txt": b"beta-v1"}
    paths = _put_sources(client, prefix, bodies)
    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]

    r1 = _pull(client, sid)
    assert r1.status_code == 200
    seg0 = r1.json()
    assert seg0["segment"] == 0
    assert seg0["has_more"] is True
    assert len(seg0["entries"]) == 1
    assert base64.b64decode(seg0["entries"][0]["content_b64"]) == b"alpha-v1"
    assert seg0["entries"][0]["sha256"] == SHA_OF(b"alpha-v1")

    # Source publishes a new revision after the session was created.
    client.put(f"/objects/{sorted(paths)[0]}", content=b"alpha-v2-CHANGED")

    # Repeated pull: identical bytes, digest and cursor (pinned snapshot).
    r2 = _pull(client, sid)
    assert r2.status_code == 200
    assert r2.content == r1.content
    assert r2.json()["cursor"] == seg0["cursor"]
    assert r2.json()["segment_sha256"] == seg0["segment_sha256"]

    # Next segment via cursor, also repeatable.
    r3 = _pull(client, sid, cursor=seg0["cursor"])
    assert r3.status_code == 200
    seg1 = r3.json()
    assert seg1["segment"] == 1
    assert seg1["has_more"] is False
    assert base64.b64decode(seg1["entries"][0]["content_b64"]) == b"beta-v1"
    assert _pull(client, sid, cursor=seg0["cursor"]).content == r3.content

    # Past the end.
    r4 = _pull(client, sid, cursor=seg1["cursor"])
    assert r4.status_code == 404
    assert r4.json()["detail"] == "no_more_segments"


def test_pull_with_foreign_or_tampered_cursor_is_409(client):
    prefix = _prefix("foreign")
    paths = _put_sources(client, prefix, {"a.txt": b"aaa", "b.txt": b"bbb"})
    sid1 = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    sid2 = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    cursor_of_s1 = _pull(client, sid1).json()["cursor"]

    # A cursor minted for another session is expired here.
    r = _pull(client, sid2, cursor=cursor_of_s1)
    assert r.status_code == 409
    assert r.json()["status"] == "cursor_expired"
    assert r.json()["resume"]["next_segment"] == 0

    assert _pull(client, sid2, cursor="garbage.token").status_code == 409


# ---- acknowledge: atomic apply + replay -----------------------------------------

def test_ack_applies_batch_and_replays(client):
    prefix = _prefix("apply")
    bodies = {"a.txt": b"alpha", "b.txt": b"beta", "c.txt": b"gamma"}
    paths = _put_sources(client, prefix, bodies)
    sid = _create_session(client, paths, "backup/", segment_size=3).json()["session"]

    seg = _pull(client, sid).json()
    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 200
    result = r.json()
    assert result["status"] == "applied"
    assert result["segment"] == 0
    assert len(result["applied"]) == 3
    assert all(a["action"] == "created" for a in result["applied"])
    assert result["resume"]["next_segment"] == 1
    assert result["resume"]["last_confirmed_cursor"] == seg["cursor"]

    # Targets are ordinary objects: same content, same strong ETag.
    for name, body in sorted(bodies.items()):
        g = client.get(f"/objects/backup/{prefix}/{name}")
        assert g.status_code == 200
        assert g.content == body
        assert g.headers["etag"] == ETAG_OF(body)

    # Repeated acknowledgement replays the identical batch result.
    r2 = _ack(client, sid, seg["cursor"])
    assert r2.status_code == 200
    assert r2.json() == result

    # Session is complete.
    desc = client.get(f"/copy-sessions/{sid}").json()
    assert desc["status"] == "complete"
    assert desc["resume"]["last_confirmed_cursor"] == seg["cursor"]


def test_ack_conflict_rolls_back_and_replays(client):
    prefix = _prefix("conflict")
    bodies = {"a.txt": b"alpha", "b.txt": b"beta"}
    paths = _put_sources(client, prefix, bodies)
    # Pre-existing divergent content at one target.
    client.put(f"/objects/backup/{prefix}/b.txt", content=b"divergent")

    sid = _create_session(client, paths, "backup/", segment_size=2).json()["session"]
    seg = _pull(client, sid).json()
    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 409
    result = r.json()
    assert result["status"] == "conflict"
    assert result["applied"] == []
    assert len(result["conflicts"]) == 1
    conflict = result["conflicts"][0]
    assert conflict["target_path"] == f"backup/{prefix}/b.txt"
    assert conflict["expected_sha256"] == SHA_OF(b"beta")
    assert conflict["actual_sha256"] == SHA_OF(b"divergent")
    # Resume position points at the failed segment; nothing confirmed yet.
    assert result["resume"]["next_segment"] == 0
    assert result["resume"]["last_confirmed_cursor"] is None

    # No partial application: the other target must not exist.
    assert client.get(f"/objects/backup/{prefix}/a.txt").status_code == 404
    # The conflicting target is untouched.
    assert client.get(f"/objects/backup/{prefix}/b.txt").content == b"divergent"

    # Replay returns the identical conflict result.
    r2 = _ack(client, sid, seg["cursor"])
    assert r2.status_code == 409
    assert r2.json() == result


def test_ack_out_of_order_is_rejected(client):
    prefix = _prefix("order")
    paths = _put_sources(client, prefix, {"a.txt": b"a", "b.txt": b"b"})
    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg0 = _pull(client, sid).json()
    seg1 = _pull(client, sid, cursor=seg0["cursor"]).json()

    # Acknowledging segment 1 before segment 0: expired cursor + resume.
    r = _ack(client, sid, seg1["cursor"])
    assert r.status_code == 409
    assert r.json()["status"] == "cursor_expired"
    assert r.json()["resume"]["next_segment"] == 0

    # In order works, then the next one too.
    assert _ack(client, sid, seg0["cursor"]).status_code == 200
    assert _ack(client, sid, seg1["cursor"]).status_code == 200


def test_ack_unknown_session_and_bad_body(client):
    assert _ack(client, "0" * 32, "whatever").status_code == 404
    prefix = _prefix("badbody")
    paths = _put_sources(client, prefix, {"a.txt": b"a"})
    sid = _create_session(client, paths, "backup/").json()["session"]
    assert client.post(f"/copy-sessions/{sid}/acks", json={}).status_code == 400
    assert (
        client.post(f"/copy-sessions/{sid}/acks", content=b"nope").status_code
        == 400
    )


def test_ack_idempotent_when_content_already_at_target(client):
    prefix = _prefix("idempotent")
    paths = _put_sources(client, prefix, {"a.txt": b"same-bytes"})
    # Target already holds identical content (e.g. copied earlier).
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"same-bytes")

    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg = _pull(client, sid).json()
    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 200
    assert r.json()["applied"][0]["action"] == "already_present"


def test_same_bytes_republish_at_absent_target_is_conflict(client):
    """The fixed miss: target absent at session creation, then the very
    same bytes are published before acknowledgement.  Digest equality
    must not mask the post-baseline publish."""
    prefix = _prefix("samebytes-new")
    paths = _put_sources(client, prefix, {"a.txt": b"same-bytes"})
    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg = _pull(client, sid).json()

    # Independently publish byte-identical content at the target.
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"same-bytes")

    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 409
    result = r.json()
    assert result["status"] == "conflict"
    assert result["applied"] == []
    assert len(result["conflicts"]) == 1
    conflict = result["conflicts"][0]
    assert conflict["target_path"] == f"backup/{prefix}/a.txt"
    assert conflict["reason"] == "published_after_session"
    # Both digests are identical — this is what the digest-only check
    # used to mistake for already_present.
    assert conflict["expected_sha256"] == SHA_OF(b"same-bytes")
    assert conflict["actual_sha256"] == SHA_OF(b"same-bytes")
    assert conflict["actual_revision_no"] == 1
    assert conflict["baseline"] == {
        "existed": False,
        "revision_id": None,
        "revision_no": None,
        "sha256": None,
    }
    # The batch was not applied and the cursor did not advance.
    assert result["resume"]["next_segment"] == 0
    assert result["resume"]["last_confirmed_cursor"] is None
    desc = client.get(f"/copy-sessions/{sid}").json()
    assert desc["status"] == "open"
    assert desc["next_segment"] == 0

    # The independently published content is left untouched.
    assert client.get(f"/objects/backup/{prefix}/a.txt").content == b"same-bytes"

    # Replay returns the very first persisted result.
    r2 = _ack(client, sid, seg["cursor"])
    assert r2.status_code == 409
    assert r2.json() == result


def test_same_bytes_republish_at_existing_target_is_conflict(client):
    """Target held identical content before the session; after creation
    it is published again with identical bytes.  Same revision identity
    is what matters, not digest equality."""
    prefix = _prefix("samebytes-old")
    paths = _put_sources(client, prefix, {"a.txt": b"same-bytes"})
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"same-bytes")

    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg = _pull(client, sid).json()

    # Re-publish identical bytes at the target (revision_no -> 2).
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"same-bytes")

    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 409
    conflict = r.json()["conflicts"][0]
    assert conflict["reason"] == "published_after_session"
    assert conflict["expected_sha256"] == conflict["actual_sha256"]
    assert conflict["actual_revision_no"] == 2
    assert conflict["baseline"]["existed"] is True
    assert conflict["baseline"]["revision_no"] == 1


def test_same_bytes_republish_in_batch_rolls_back_whole_segment(client):
    """One post-baseline publish among several entries rejects the whole
    batch, including entries that would otherwise have been created."""
    prefix = _prefix("samebytes-batch")
    bodies = {"a.txt": b"alpha", "b.txt": b"beta", "c.txt": b"gamma"}
    paths = _put_sources(client, prefix, bodies)
    sid = _create_session(client, paths, "backup/", segment_size=3).json()["session"]
    seg = _pull(client, sid).json()

    # Only b.txt is touched, with identical bytes.
    client.put(f"/objects/backup/{prefix}/b.txt", content=b"beta")

    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 409
    result = r.json()
    assert result["applied"] == []
    assert [c["target_path"] for c in result["conflicts"]] == [
        f"backup/{prefix}/b.txt"
    ]
    # No partial application: the untouched targets were not created.
    assert client.get(f"/objects/backup/{prefix}/a.txt").status_code == 404
    assert client.get(f"/objects/backup/{prefix}/c.txt").status_code == 404

    # Re-acking keeps returning the stored conflict; cursor never moves.
    assert _ack(client, sid, seg["cursor"]).json() == result
    assert client.get(f"/copy-sessions/{sid}").json()["next_segment"] == 0


def test_preexisting_divergence_reports_diverged_at_session(client):
    """Content already different at baseline time: identity is unchanged
    at ack, but the baseline digest differs from the pinned digest — a
    regular conflict, distinct from a post-baseline publish."""
    prefix = _prefix("diverged-at")
    paths = _put_sources(client, prefix, {"a.txt": b"alpha"})
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"different")

    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg = _pull(client, sid).json()
    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 409
    conflict = r.json()["conflicts"][0]
    assert conflict["reason"] == "diverged_at_session"
    assert conflict["baseline"]["existed"] is True
    assert conflict["baseline"]["sha256"] == SHA_OF(b"different")
    assert conflict["actual_sha256"] == SHA_OF(b"different")
    assert client.get(f"/objects/backup/{prefix}/a.txt").content == b"different"


def test_baseline_unchanged_same_bytes_still_applies_and_replays(client):
    """Target already holds identical bytes at baseline and nobody
    publishes afterwards: idempotent skip still works, repeated acks
    replay the stored applied result."""
    prefix = _prefix("unchanged")
    bodies = {"a.txt": b"alpha", "b.txt": b"beta"}
    paths = _put_sources(client, prefix, bodies)
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"alpha")
    sid = _create_session(client, paths, "backup/", segment_size=2).json()["session"]
    seg = _pull(client, sid).json()

    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 200
    result = r.json()
    actions = {a["target_path"]: a["action"] for a in result["applied"]}
    assert actions[f"backup/{prefix}/a.txt"] == "already_present"
    assert actions[f"backup/{prefix}/b.txt"] == "created"

    assert _ack(client, sid, seg["cursor"]).json() == result
    assert client.get(f"/objects/backup/{prefix}/a.txt").content == b"alpha"
    assert client.get(f"/objects/backup/{prefix}/b.txt").content == b"beta"


def test_legacy_session_without_baseline_is_not_allowed_by_digest(client):
    """Upgrade scenario: an unfinished session whose manifest rows were
    written before baselines existed must not be acknowledged just
    because current digests happen to match."""
    import asyncio

    prefix = _prefix("legacy")
    paths = _put_sources(client, prefix, {"a.txt": b"alpha"})
    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg = _pull(client, sid).json()

    # Simulate rows created by the pre-upgrade schema: erase baselines
    # via a connection independent of the server thread's pool.
    async def erase_baselines(dsn: str):
        import asyncpg

        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(
                "UPDATE copy_session_entries SET baseline_captured = FALSE,"
                " baseline_revision_id = NULL, baseline_revision_no = NULL,"
                " baseline_sha256 = NULL WHERE session_id = $1",
                sid,
            )
        finally:
            await conn.close()

    import asyncio
    import os

    asyncio.new_event_loop().run_until_complete(
        erase_baselines(os.environ["DATABASE_URL"])
    )

    # Even with identical bytes at the target, no digest-based pass.
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"alpha")
    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 409
    result = r.json()
    assert result["status"] == "baseline_missing"
    assert result["resume"]["next_segment"] == 0
    assert result["applied"] == []
    # Cursor unchanged: the session still cannot advance.
    assert client.get(f"/copy-sessions/{sid}").json()["next_segment"] == 0
    # Replay stays stable.
    assert _ack(client, sid, seg["cursor"]).json() == result

    # A fresh session captures a baseline and applies idempotently.
    sid2 = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg2 = _pull(client, sid2).json()
    r2 = _ack(client, sid2, seg2["cursor"])
    assert r2.status_code == 200
    assert r2.json()["applied"][0]["action"] == "already_present"


def test_legacy_session_resume_points_at_last_confirmed(client):
    """A pre-upgrade session whose early segments were already applied:
    the baseline-less segment's 409 still offers last_confirmed_cursor
    of the preceding segment for resuming."""
    import asyncio
    import os

    import asyncpg

    prefix = _prefix("legacy-resume")
    paths = _put_sources(client, prefix, {"a.txt": b"alpha", "b.txt": b"beta"})
    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg0 = _pull(client, sid).json()
    seg1 = _pull(client, sid, cursor=seg0["cursor"]).json()
    # Segment 0 applies while baselines still exist.
    assert _ack(client, sid, seg0["cursor"]).status_code == 200

    # Then the deployment is "downgraded": erase baselines everywhere.
    async def erase(dsn: str):
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(
                "UPDATE copy_session_entries SET baseline_captured = FALSE,"
                " baseline_revision_id = NULL, baseline_revision_no = NULL,"
                " baseline_sha256 = NULL WHERE session_id = $1",
                sid,
            )
        finally:
            await conn.close()

    asyncio.new_event_loop().run_until_complete(erase(os.environ["DATABASE_URL"]))

    r = _ack(client, sid, seg1["cursor"])
    assert r.status_code == 409
    resume = r.json()["resume"]
    assert resume["next_segment"] == 1
    assert resume["last_confirmed_cursor"] == seg0["cursor"]

    # The offered cursor resumes a fresh session right after segment 0.
    r2 = _create_session(
        client, paths, "backup/", segment_size=1,
        cursor=resume["last_confirmed_cursor"],
    )
    assert r2.status_code == 201
    assert [e["source_path"] for e in r2.json()["manifest"]] == sorted(paths)[1:]


def test_conflict_recovery_new_session_captures_fresh_baseline(client):
    """After a same-bytes post-baseline conflict, resuming via a new
    session (fresh snapshot/baseline) applies idempotently."""
    prefix = _prefix("recover-same")
    paths = _put_sources(client, prefix, {"a.txt": b"alpha", "b.txt": b"beta"})

    sid1 = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg0 = _pull(client, sid1).json()
    r0 = _ack(client, sid1, seg0["cursor"])
    assert r0.status_code == 200
    last_confirmed = r0.json()["resume"]["last_confirmed_cursor"]

    # Target of segment 1 receives identical bytes independently.
    client.put(f"/objects/backup/{prefix}/b.txt", content=b"beta")
    seg1 = _pull(client, sid1, cursor=seg0["cursor"]).json()
    r1 = _ack(client, sid1, seg1["cursor"])
    assert r1.status_code == 409
    assert r1.json()["conflicts"][0]["reason"] == "published_after_session"

    # New session resumed after the last confirmed cursor: fresh
    # baseline sees beta already present -> idempotent skip, progress.
    r = _create_session(client, paths, "backup/", segment_size=1,
                        cursor=last_confirmed)
    assert r.status_code == 201
    sid2 = r.json()["session"]
    seg = _pull(client, sid2).json()
    r2 = _ack(client, sid2, seg["cursor"])
    assert r2.status_code == 200
    assert r2.json()["applied"][0]["action"] == "already_present"
    assert client.get(f"/objects/backup/{prefix}/b.txt").content == b"beta"


# ---- concurrent acknowledgements -----------------------------------------------

def test_ack_serialises_against_concurrent_same_bytes_publish(client):
    """The ack takes FOR UPDATE locks on targets in path order.  A
    concurrent publisher of identical bytes is strictly serialised:

    * publish first  -> the ack sees a post-baseline publish and 409s,
      nothing applied, cursor not advanced;
    * ack first      -> the ack applies and the publisher waits, then
      wins (revision 2 with identical bytes); both outcomes keep the
      stored result stable on replay.
    """
    import threading

    prefix = _prefix("race-lock")
    paths = _put_sources(client, prefix, {"a.txt": b"alpha"})
    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    cursor = _pull(client, sid).json()["cursor"]

    outcomes: list = []
    barrier = threading.Barrier(2)

    def ack():
        barrier.wait()
        r = _ack(client, sid, cursor)
        outcomes.append(("ack", r.status_code, r.content))

    def publish():
        barrier.wait()
        r = client.put(f"/objects/backup/{prefix}/a.txt", content=b"alpha")
        outcomes.append(("put", r.status_code))

    threads = [threading.Thread(target=ack), threading.Thread(target=publish)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ack_code = next(payload[1] for payload in outcomes if payload[0] == "ack")
    put_code = next(payload[1] for payload in outcomes if payload[0] == "put")
    assert put_code == 204
    if ack_code == 200:
        # Ack committed first (created rev 1); publisher then created rev 2.
        g = client.get(f"/objects/backup/{prefix}/a.txt")
        assert g.content == b"alpha"
        assert g.headers["x-revision-no"] == "2"
        assert client.get(f"/copy-sessions/{sid}").json()["status"] == "complete"
    else:
        # Publisher committed first: post-baseline publish detected.
        assert ack_code == 409
        stored = _ack(client, sid, cursor).json()
        assert stored["status"] == "conflict"
        assert stored["conflicts"][0]["reason"] == "published_after_session"
        assert client.get(f"/copy-sessions/{sid}").json()["next_segment"] == 0
        # Replay is byte-identical.
        assert _ack(client, sid, cursor).content == json_dumps_bytes(stored)


def test_concurrent_acks_of_same_segment_converge(client):
    import threading

    prefix = _prefix("race-ack")
    paths = _put_sources(client, prefix, {"a.txt": b"a", "b.txt": b"b"})
    sid = _create_session(client, paths, "backup/", segment_size=2).json()["session"]
    cursor = _pull(client, sid).json()["cursor"]

    results = []

    def ack():
        r = _ack(client, sid, cursor)
        results.append((r.status_code, r.content))

    threads = [threading.Thread(target=ack) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Every concurrent acknowledgement observes the same single batch.
    assert all(code == 200 for code, _ in results)
    assert len({body for _, body in results}) == 1
    assert client.get(f"/objects/backup/{prefix}/a.txt").content == b"a"


# ---- resume across sessions ------------------------------------------------------

def test_resume_with_last_confirmed_cursor(client):
    prefix = _prefix("resume")
    bodies = {f"{c}.txt": f"content-{c}".encode() for c in "abcd"}
    paths = _put_sources(client, prefix, bodies)

    # Session 1: confirm only the first segment (paths a, b).
    sid1 = _create_session(client, paths, "backup/", segment_size=2).json()["session"]
    seg0 = _pull(client, sid1).json()
    assert _ack(client, sid1, seg0["cursor"]).status_code == 200
    last_confirmed = seg0["cursor"]

    # Session 2 resumes after the last confirmed cursor.
    r = _create_session(client, paths, "backup/", segment_size=2,
                       cursor=last_confirmed)
    assert r.status_code == 201
    data = r.json()
    assert data["resumed_after"] == sorted(paths)[1]  # last path of segment 0
    assert [e["source_path"] for e in data["manifest"]] == sorted(paths)[2:]

    # Drain session 2.
    seg = _pull(client, data["session"]).json()
    assert _ack(client, data["session"], seg["cursor"]).status_code == 200
    for name, body in bodies.items():
        assert client.get(f"/objects/backup/{prefix}/{name}").content == body


def test_conflict_recovery_via_new_session(client):
    prefix = _prefix("recover")
    bodies = {"a.txt": b"alpha", "b.txt": b"beta"}
    paths = _put_sources(client, prefix, bodies)
    client.put(f"/objects/backup/{prefix}/b.txt", content=b"divergent")

    sid1 = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg0 = _pull(client, sid1).json()
    # Segment 0 (a.txt) applies fine.
    r0 = _ack(client, sid1, seg0["cursor"])
    assert r0.status_code == 200
    last_confirmed = r0.json()["resume"]["last_confirmed_cursor"]

    # Segment 1 (b.txt) conflicts; the session is stuck on it.
    seg1 = _pull(client, sid1, cursor=seg0["cursor"]).json()
    assert _ack(client, sid1, seg1["cursor"]).status_code == 409

    # Operator resolves the divergence out-of-band, then a new session
    # resumes from the last confirmed cursor with a fresh snapshot.
    client.put(f"/objects/backup/{prefix}/b.txt", content=b"beta")
    r = _create_session(client, paths, "backup/", segment_size=1,
                        cursor=last_confirmed)
    assert r.status_code == 201
    sid2 = r.json()["session"]
    assert [e["source_path"] for e in r.json()["manifest"]] == sorted(paths)[1:]

    seg = _pull(client, sid2).json()
    r2 = _ack(client, sid2, seg["cursor"])
    assert r2.status_code == 200
    assert r2.json()["applied"][0]["action"] == "already_present"
    assert client.get(f"/objects/backup/{prefix}/b.txt").content == b"beta"


def test_resume_cursor_rejected_for_different_set_or_prefix(client):
    prefix = _prefix("resumecheck")
    paths = _put_sources(client, prefix, {"a.txt": b"a", "b.txt": b"b"})
    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    cursor = _pull(client, sid).json()["cursor"]

    # Different target prefix.
    assert (
        _create_session(client, paths, "other/", cursor=cursor).status_code == 409
    )
    # Different source set.
    assert (
        _create_session(client, paths[:1], "backup/", cursor=cursor).status_code
        == 409
    )
    # Garbage cursor.
    assert (
        _create_session(client, paths, "backup/", cursor="x.y").status_code == 409
    )


def test_full_walkthrough_multiple_segments(client):
    prefix = _prefix("walkthrough")
    bodies = {f"f{i}.txt": f"payload-{i}".encode() * (i + 1) for i in range(5)}
    paths = _put_sources(client, prefix, bodies)
    sid = _create_session(client, paths, "replica/", segment_size=2).json()["session"]

    cursor = None
    confirmed = 0
    while True:
        seg = _pull(client, sid, cursor=cursor)
        if seg.status_code == 404:
            break
        assert seg.status_code == 200
        data = seg.json()
        # Every pull of this segment is byte-identical.
        assert _pull(client, sid, cursor=cursor).content == seg.content
        r = _ack(client, sid, data["cursor"])
        assert r.status_code == 200
        confirmed += 1
        cursor = data["cursor"]
    assert confirmed == 3  # ceil(5 / 2)

    for name, body in bodies.items():
        g = client.get(f"/objects/replica/{prefix}/{name}")
        assert g.content == body
        assert g.headers["etag"] == ETAG_OF(body)
    assert client.get(f"/copy-sessions/{sid}").json()["status"] == "complete"
