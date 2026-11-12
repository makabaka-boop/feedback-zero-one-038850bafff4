"""Backend batch service.

A single-file HTTP front over a SQLite database.  It owns:

* the job catalogue (submission, idempotent re-submission, rejection);
* the persistent logical clock ``now`` (advanced only through the test
  interface) and per-job lease epochs;
* the atomic publication of labels together with the terminal state;
* crash hooks used by the acceptance tests to kill the process at exact
  moments of the commit protocol.

Run with::

    python3 -I server.py --db /tmp/batch.db --port 8080 --crash-hook /tmp/hook.json
"""

import argparse
import json
import os
import sqlite3
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (  # noqa: E402
    HttpError,
    ValidationError,
    fingerprint,
    require_nonneg_int,
    validate_submission,
)

LEASE_LENGTH = 5


def connect_db(path):
    conn = sqlite3.connect(path, timeout=10, isolation_level=None)
    # WAL lets the worker processes and the HTTP front read while a writer
    # is mid-transaction; busy_timeout serialises the actual writers.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta(
            key   TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS jobs(
            id            TEXT PRIMARY KEY,
            fingerprint   TEXT NOT NULL,
            status        TEXT NOT NULL CHECK(status IN
                          ('queued','running','succeeded','failed')),
            epoch         INTEGER NOT NULL DEFAULT 0,
            lease_expiry  INTEGER,
            ticket        TEXT,
            progress      INTEGER NOT NULL DEFAULT 0,
            created_seq   INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS job_rows(
            job_id       TEXT NOT NULL REFERENCES jobs(id),
            row_idx      INTEGER NOT NULL,
            row_id       TEXT NOT NULL,
            text         TEXT NOT NULL,
            label        TEXT,
            label_is_set INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (job_id, row_idx)
        );
        """
    )
    conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('clock', 0)")
    conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('created_seq', 0)")


class CrashHook:
    """File-driven one-shot crash hooks.

    The hook file maps a rule name (``worker_pause`` / ``server_exit`` ...)
    to a rule object.  ``maybe_exit`` is called at named points in the
    commit protocol; on a match the *whole rule file is deleted first*
    (so a restarted process never re-fires the same rule) and the process
    terminates with SIGKILL-style ``os._exit(2)`` without running any
    ``finally`` block -- simulating a hard crash.
    """

    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()

    def _load(self):
        if not self.path or not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (ValueError, OSError):
            return {}

    def maybe_exit(self, rule_name, context):
        if not self.path:
            return
        with self.lock:
            rules = self._load()
            rule = rules.get(rule_name)
            if not rule:
                return
            if rule.get("job") not in (None, context.get("job")):
                return
            # Delete first so that after restart the rule cannot re-fire.
            try:
                os.remove(self.path)
            except OSError:
                pass
            sys.stderr.write(
                "[server] CRASH HOOK %s fired for job=%s\n"
                % (rule_name, context.get("job"))
            )
            sys.stderr.flush()
            os._exit(2)


class Server:
    def __init__(self, db_path, hook_path):
        self.db_path = db_path
        self.init_lock = threading.Lock()
        self.hook = CrashHook(hook_path)

    def conn(self):
        c = connect_db(self.db_path)
        # One connection per request/thread: SQLite connections must not be
        # shared across threads, and separate processes get separate handles.
        return c

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _clock(conn):
        row = conn.execute("SELECT value FROM meta WHERE key='clock'").fetchone()
        return int(row[0])

    @staticmethod
    def _job_or_404(conn, job_id):
        row = conn.execute(
            "SELECT id, status, epoch, lease_expiry, ticket, progress "
            "FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        if row is None:
            raise HttpError(404, b'{"error":"unknown job"}')
        return row

    def _write_guard(self, conn, job_id, epoch):
        """Common precondition for every lease-scoped write.

        Writes are only legal while the caller still holds the lease:
        epoch must match *and* ``now`` must be strictly before expiry.
        Eligibility is checked inside the same transaction that performs
        the update, so a racing clock advance / claim cannot slip in.
        """
        row = conn.execute(
            "SELECT status, epoch, lease_expiry FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
        if row is None:
            raise HttpError(404, b'{"error":"unknown job"}')
        status, current_epoch, expiry = row
        if status in ("succeeded", "failed"):
            raise HttpError(409, b'{"error":"job already terminal"}')
        if current_epoch != epoch:
            raise HttpError(409, b'{"error":"epoch superseded"}')
        if expiry is None or self._clock(conn) >= expiry:
            raise HttpError(409, b'{"error":"lease expired"}')

    # -- public endpoints --------------------------------------------------

    def submit(self, body):
        payload = body  # parse errors are handled by the HTTP layer
        job_id, rows = validate_submission(payload)
        fp = fingerprint(rows)

        conn = self.conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT fingerprint FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if existing is not None:
                if existing[0] == fp:
                    # Same content: idempotent return of the original job;
                    # nothing is written.
                    conn.execute("COMMIT")
                    return 200, self._job_view(conn, job_id, replayed=True)
                # Different content under the same id: reject and leave the
                # original job completely untouched.  Resolve the response
                # while the transaction is still open, commit, then raise
                # outside the try/rollback block.
                conn.execute("COMMIT")
                conflict = HttpError(
                    409,
                    json.dumps(
                        {"error": "job id already exists with different content",
                         "path": ""}
                    ).encode("utf-8"),
                )
            else:
                seq = conn.execute(
                    "UPDATE meta SET value=value+1 WHERE key='created_seq' "
                    "RETURNING value"
                ).fetchone()[0]
                conn.execute(
                    "INSERT INTO jobs(id, fingerprint, status, epoch, "
                    "progress, created_seq) "
                    "VALUES(?,?, 'queued', 0, 0, ?)",
                    (job_id, fp, seq),
                )
                conn.executemany(
                    "INSERT INTO job_rows(job_id, row_idx, row_id, text) "
                    "VALUES(?,?,?,?)",
                    [(job_id, i, r["id"], r["text"])
                     for i, r in enumerate(rows)],
                )
                conn.execute("COMMIT")
                return 201, self._job_view(conn, job_id, replayed=False)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        # Post-commit rejection: no transaction is open here.
        raise conflict

    def get_job(self, job_id):
        conn = self.conn()
        try:
            self._job_or_404(conn, job_id)
            return 200, self._job_view(conn, job_id)
        finally:
            conn.close()

    def _job_view(self, conn, job_id, replayed=None):
        row = conn.execute(
            "SELECT status, epoch, lease_expiry, ticket, progress "
            "FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        status, epoch, expiry, ticket, progress = row
        view = {
            "job": job_id,
            "status": status,
            "epoch": epoch,
            "lease_expiry": expiry,
            "ticket": ticket,
            "progress": progress,
        }
        if replayed is not None:
            view["replayed"] = replayed
        if status == "succeeded":
            # Full label set, sorted by id; null labels are preserved.
            labels = conn.execute(
                "SELECT row_id, label, label_is_set FROM job_rows "
                "WHERE job_id=? ORDER BY row_id",
                (job_id,),
            ).fetchall()
            view["labels"] = [
                {"id": rid, "label": label if is_set else None}
                for rid, label, is_set in labels
            ]
        else:
            # Unfinished or failed jobs expose no labels at all.
            view["labels"] = None
        return view

    def advance_clock(self, body):
        delta = body.get("delta") if isinstance(body, dict) else None
        require_nonneg_int(delta, "/delta")
        conn = self.conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE meta SET value=value+? WHERE key='clock'", (delta,)
            )
            now = self._clock(conn)
            conn.execute("COMMIT")
            return 200, {"now": now}
        except Exception:
            # ROLLBACK is a no-op-safe statement once the transaction has
            # committed; only a genuinely failed ROLLBACK is swallowed so
            # the original exception always propagates.
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def inspect(self):
        conn = self.conn()
        try:
            now = self._clock(conn)
            jobs = [
                {
                    "job": j[0],
                    "status": j[1],
                    "epoch": j[2],
                    "lease_expiry": j[3],
                    "ticket": j[4],
                    "progress": j[5],
                }
                for j in conn.execute(
                    "SELECT id, status, epoch, lease_expiry, ticket, progress "
                    "FROM jobs ORDER BY created_seq"
                )
            ]
            return 200, {"now": now, "jobs": jobs}
        finally:
            conn.close()

    # -- worker-facing lease protocol --------------------------------------

    def claim(self, body):
        conn = self.conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = self._clock(conn)
            row = conn.execute(
                "SELECT id, status, epoch, lease_expiry FROM jobs "
                "WHERE status IN ('queued','running') "
                "ORDER BY created_seq"
            ).fetchall()
            candidate = None
            for job_id, status, epoch, expiry in row:
                # Claimable when there is no holder (queued has never been
                # claimed; running with NULL expiry cannot normally happen)
                # or the current lease has reached its expiry value.
                if status == "queued" or expiry is None or now >= expiry:
                    candidate = (job_id, epoch)
                    break
            if candidate is None:
                conn.execute("COMMIT")
                return 204, None
            job_id, old_epoch = candidate
            new_epoch = old_epoch + 1
            expiry = now + LEASE_LENGTH
            conn.execute(
                "UPDATE jobs SET status='running', epoch=?, lease_expiry=? "
                "WHERE id=?",
                (new_epoch, expiry, job_id),
            )
            conn.execute("COMMIT")
            rows = conn.execute(
                "SELECT row_id, text FROM job_rows WHERE job_id=? ORDER BY row_idx",
                (job_id,),
            ).fetchall()
            return 200, {
                "job": job_id,
                "epoch": new_epoch,
                "lease_expiry": expiry,
                "rows": [{"id": r, "text": t} for r, t in rows],
            }
        except Exception:
            # ROLLBACK is a no-op-safe statement once the transaction has
            # committed; only a genuinely failed ROLLBACK is swallowed so
            # the original exception always propagates.
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def renew(self, body):
        job_id, epoch = body["job"], body["epoch"]
        conn = self.conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._write_guard(conn, job_id, epoch)
            now = self._clock(conn)
            expiry = now + LEASE_LENGTH
            conn.execute(
                "UPDATE jobs SET lease_expiry=? WHERE id=? AND epoch=?",
                (expiry, job_id, epoch),
            )
            conn.execute("COMMIT")
            return 200, {"job": job_id, "epoch": epoch, "lease_expiry": expiry}
        except Exception:
            # ROLLBACK is a no-op-safe statement once the transaction has
            # committed; only a genuinely failed ROLLBACK is swallowed so
            # the original exception always propagates.
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def progress(self, body):
        job_id, epoch = body["job"], body["epoch"]
        amount = body.get("amount", 1)
        require_nonneg_int(amount, "/amount")
        conn = self.conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._write_guard(conn, job_id, epoch)
            conn.execute(
                "UPDATE jobs SET progress=progress+? WHERE id=? AND epoch=?",
                (amount, job_id, epoch),
            )
            row = conn.execute(
                "SELECT progress FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            conn.execute("COMMIT")
            return 200, {"job": job_id, "epoch": epoch, "progress": row[0]}
        except Exception:
            # ROLLBACK is a no-op-safe statement once the transaction has
            # committed; only a genuinely failed ROLLBACK is swallowed so
            # the original exception always propagates.
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def attach_ticket(self, body):
        """Persist the remote ticket after the source accepted the task."""
        job_id, epoch, ticket = body["job"], body["epoch"], body["ticket"]
        if not isinstance(ticket, str) or not ticket:
            raise ValidationError("/ticket", "must be a non-empty string")
        conn = self.conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._write_guard(conn, job_id, epoch)
            conn.execute(
                "UPDATE jobs SET ticket=? WHERE id=? AND epoch=?",
                (ticket, job_id, epoch),
            )
            conn.execute("COMMIT")
            return 200, self._job_view(conn, job_id)
        except Exception:
            # ROLLBACK is a no-op-safe statement once the transaction has
            # committed; only a genuinely failed ROLLBACK is swallowed so
            # the original exception always propagates.
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def complete(self, body, crash_hook=None):
        """Publish labels and the succeeded terminal state atomically."""
        job_id, epoch = body["job"], body["epoch"]
        labels = body["labels"]
        if not isinstance(labels, list):
            raise ValidationError("/labels", "expected an array")
        conn = self.conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._write_guard(conn, job_id, epoch)
            input_rows = conn.execute(
                "SELECT row_id FROM job_rows WHERE job_id=? ORDER BY row_id",
                (job_id,),
            ).fetchall()
            input_ids = [r[0] for r in input_rows]
            label_ids = []
            for i, item in enumerate(labels):
                base = "/labels/%d" % i
                if not isinstance(item, dict):
                    raise ValidationError(base, "expected an object")
                rid = item.get("id")
                if not isinstance(rid, str):
                    raise ValidationError(base + "/id", "must be a string")
                if "label" not in item:
                    raise ValidationError(base + "/label", "field is required")
                lab = item["label"]
                if lab is not None and not isinstance(lab, str):
                    raise ValidationError(base + "/label",
                                          "must be a string or null")
                label_ids.append(rid)
            # One-to-one correspondence with the input, order-insensitive.
            if sorted(label_ids) != input_ids:
                raise ValidationError(
                    "/labels",
                    "label ids must correspond one-to-one with input row ids",
                )

            # All label writes and the terminal flip happen in this one
            # transaction: a crash before COMMIT leaves no partial labels
            # and the job remains claimable under a new epoch.
            for item in labels:
                conn.execute(
                    "UPDATE job_rows SET label=?, label_is_set=1 "
                    "WHERE job_id=? AND row_id=?",
                    (item["label"], job_id, item["id"]),
                )
            conn.execute(
                "UPDATE jobs SET status='succeeded', lease_expiry=NULL "
                "WHERE id=? AND epoch=?",
                (job_id, epoch),
            )
            if crash_hook is not None:
                # Point 2 of the acceptance protocol: labels written inside
                # the open transaction, COMMIT not yet issued.
                crash_hook.maybe_exit(
                    "server_exit",
                    {"job": job_id, "when": "before_commit_complete"},
                )
            conn.execute("COMMIT")
            return 200, self._job_view(conn, job_id)
        except Exception:
            # ROLLBACK is a no-op-safe statement once the transaction has
            # committed; only a genuinely failed ROLLBACK is swallowed so
            # the original exception always propagates.
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def fail(self, body):
        job_id, epoch, reason = body["job"], body["epoch"], body.get("reason", "")
        conn = self.conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._write_guard(conn, job_id, epoch)
            conn.execute(
                "UPDATE jobs SET status='failed', lease_expiry=NULL "
                "WHERE id=? AND epoch=?",
                (job_id, epoch),
            )
            conn.execute("COMMIT")
            return 200, self._job_view(conn, job_id)
        except Exception:
            # ROLLBACK is a no-op-safe statement once the transaction has
            # committed; only a genuinely failed ROLLBACK is swallowed so
            # the original exception always propagates.
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# HTTP wiring
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "BatchLabel/1.0"

    def log_message(self, fmt, *args):  # quiet by default
        if getattr(self.server, "verbose", False):
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _send(self, status, obj):
        if obj is None:
            payload = b""
        else:
            payload = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if not raw:
            raise ValidationError("", "request body is required")
        try:
            return json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise ValidationError("", "body is not valid JSON")

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method):
        svc = self.server.service
        path = self.path.split("?", 1)[0]
        try:
            if method == "POST" and path == "/jobs":
                status, obj = svc.submit(self._read_json())
            elif method == "GET" and path.startswith("/jobs/"):
                status, obj = svc.get_job(path[len("/jobs/"):])
            elif method == "POST" and path == "/test/advance":
                status, obj = svc.advance_clock(self._read_json())
            elif method == "GET" and path == "/test/inspect":
                status, obj = svc.inspect()
            elif method == "POST" and path == "/internal/claim":
                self._read_json()  # currently no body fields
                status, obj = svc.claim({})
            elif method == "POST" and path == "/internal/renew":
                status, obj = svc.renew(self._read_json())
            elif method == "POST" and path == "/internal/progress":
                status, obj = svc.progress(self._read_json())
            elif method == "POST" and path == "/internal/ticket":
                status, obj = svc.attach_ticket(self._read_json())
            elif method == "POST" and path == "/internal/complete":
                status, obj = svc.complete(self._read_json(),
                                           crash_hook=svc.hook)
            elif method == "POST" and path == "/internal/fail":
                status, obj = svc.fail(self._read_json())
            else:
                self._send(404, {"error": "unknown route"})
                return
            self._send(status, obj)
        except ValidationError as exc:
            self._send(400, {"error": exc.message, "path": exc.path})
        except HttpError as exc:
            self._send(exc.status,
                       json.loads(exc.body) if exc.body else {"error": "error"})
        except Exception as exc:  # pragma: no cover - defensive
            sys.stderr.write("[server] unhandled: %r\n" % (exc,))
            self._send(500, {"error": "internal error"})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--crash-hook", default="")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    service = Server(args.db, args.crash_hook or None)
    conn = connect_db(args.db)
    with service.init_lock:
        init_db(conn)
    conn.close()

    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    httpd.service = service
    httpd.verbose = args.verbose
    httpd.allow_reuse_address = True
    sys.stderr.write("[server] listening on 127.0.0.1:%d db=%s\n"
                     % (args.port, args.db))
    sys.stderr.flush()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
