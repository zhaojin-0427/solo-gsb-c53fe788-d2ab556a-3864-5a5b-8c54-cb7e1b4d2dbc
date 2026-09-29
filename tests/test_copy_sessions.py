"""End-to-end tests for resumable copy sessions (/copy-sessions)."""
from __future__ import annotations

import base64
import hashlib
import uuid

SHA_OF = lambda body: hashlib.sha256(body).hexdigest()
ETAG_OF = lambda body: '"' + SHA_OF(body) + '"'


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


# ---- baseline: identical re-publish after creation is a conflict ----------------

def test_ack_same_content_republished_after_baseline_conflicts(client):
    """The bug: identical bytes published after session creation must not
    be mistaken for an idempotent already_present skip."""
    prefix = _prefix("repub-same")
    paths = _put_sources(client, prefix, {"a.txt": b"same-bytes"})
    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg = _pull(client, sid).json()

    # After the session pinned its (absent) baseline, publish the exact
    # same bytes to the target independently.
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"same-bytes")

    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 409
    result = r.json()
    assert result["status"] == "conflict"
    assert result["applied"] == []
    assert len(result["conflicts"]) == 1
    conflict = result["conflicts"][0]
    assert conflict["reason"] == "target_republished_same_content"
    assert conflict["target_path"] == f"backup/{prefix}/a.txt"
    assert conflict["expected_sha256"] == SHA_OF(b"same-bytes")
    assert conflict["actual_sha256"] == SHA_OF(b"same-bytes")
    assert conflict["actual_revision_no"] == 1
    assert conflict["baseline_revision_no"] is None
    # Cursor does not advance; recovery points back at this segment.
    assert result["resume"]["next_segment"] == 0
    assert result["resume"]["last_confirmed_cursor"] is None
    desc = client.get(f"/copy-sessions/{sid}").json()
    assert desc["status"] == "open"
    assert desc["next_segment"] == 0

    # Replay returns the identical stored conflict result.
    assert _ack(client, sid, seg["cursor"]).content == r.content


def test_ack_same_content_new_revision_on_existing_target_conflicts(client):
    """Baseline pinned rev 1 (identical content); rev 2 of the same bytes
    after session creation is still a change of revision identity."""
    prefix = _prefix("repub-exist")
    paths = _put_sources(client, prefix, {"a.txt": b"same-bytes"})
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"same-bytes")

    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg = _pull(client, sid).json()

    # Re-publish byte-identical content as a new revision.
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"same-bytes")

    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 409
    conflict = r.json()["conflicts"][0]
    assert conflict["reason"] == "target_republished_same_content"
    assert conflict["actual_revision_no"] == 2
    assert conflict["baseline_revision_no"] == 1

    # The cursor is still not advanced and the session stays resumable.
    desc = client.get(f"/copy-sessions/{sid}").json()
    assert desc["next_segment"] == 0


def test_ack_different_content_published_after_baseline_conflicts(client):
    prefix = _prefix("repub-diff")
    paths = _put_sources(client, prefix, {"a.txt": b"alpha"})
    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg = _pull(client, sid).json()

    client.put(f"/objects/backup/{prefix}/a.txt", content=b"different")
    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 409
    conflict = r.json()["conflicts"][0]
    assert conflict["reason"] == "content_diverged"
    assert conflict["actual_sha256"] == SHA_OF(b"different")
    assert conflict["baseline_revision_no"] is None


def test_same_content_republish_rolls_back_whole_batch(client):
    """One republished target aborts the entire segment; sibling targets
    that were absent at baseline must not be created."""
    prefix = _prefix("repub-batch")
    bodies = {"a.txt": b"alpha", "b.txt": b"beta"}
    paths = _put_sources(client, prefix, bodies)
    sid = _create_session(client, paths, "backup/", segment_size=2).json()["session"]
    cursor = _pull(client, sid).json()["cursor"]

    client.put(f"/objects/backup/{prefix}/a.txt", content=b"alpha")

    r = _ack(client, sid, cursor)
    assert r.status_code == 409
    assert len(r.json()["conflicts"]) == 1
    assert r.json()["conflicts"][0]["target_path"] == f"backup/{prefix}/a.txt"
    # No partial application.
    assert client.get(f"/objects/backup/{prefix}/a.txt").content == b"alpha"
    assert client.get(f"/objects/backup/{prefix}/b.txt").status_code == 404
    # Not advanced: re-acking after fixing the target still cannot happen
    # on the old session (its baseline still says absent -> conflict), so
    # recovery is a fresh session.
    assert client.get(f"/copy-sessions/{sid}").json()["next_segment"] == 0


def test_recover_after_same_content_republish_via_new_session(client):
    prefix = _prefix("repub-recover")
    paths = _put_sources(client, prefix, {"a.txt": b"same-bytes"})
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"same-bytes")
    sid1 = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    cursor = _pull(client, sid1).json()["cursor"]
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"same-bytes")
    assert _ack(client, sid1, cursor).status_code == 409

    # A fresh session re-pins the baseline at the new revision identity;
    # identical content is now a legitimate idempotent skip.
    sid2 = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg = _pull(client, sid2).json()
    r = _ack(client, sid2, seg["cursor"])
    assert r.status_code == 200
    assert r.json()["applied"][0]["action"] == "already_present"


def test_legacy_session_without_baseline_is_not_allowed_on_digest(client):
    """An unfinished session created before the baseline upgrade has no
    revision identity: matching digests alone must never let it apply."""
    import asyncio
    import asyncpg
    import os

    prefix = _prefix("legacy")
    paths = _put_sources(client, prefix, {"a.txt": b"same-bytes"})
    sid = _create_session(client, paths, "backup/", segment_size=1).json()["session"]
    seg = _pull(client, sid).json()

    async def strip_baselines():
        conn = await asyncpg.connect(os.environ["DATABASE_URL"])
        try:
            await conn.execute(
                "DELETE FROM copy_session_target_baselines WHERE session_id = $1",
                sid,
            )
        finally:
            await conn.close()

    asyncio.run(strip_baselines())

    # Identical content at the target would previously pass on digest; it
    # must now be refused with baseline_missing.
    client.put(f"/objects/backup/{prefix}/a.txt", content=b"same-bytes")
    r = _ack(client, sid, seg["cursor"])
    assert r.status_code == 409
    result = r.json()
    assert result["status"] == "baseline_missing"
    assert result["conflicts"] == []
    assert result["resume"]["next_segment"] == 0
    assert client.get(f"/copy-sessions/{sid}").json()["next_segment"] == 0

    # Pulls of the legacy session still work so a replacement session can
    # be built from a fresh snapshot.
    assert _pull(client, sid).status_code == 200


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
