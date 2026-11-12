"""Simulated remote labelling source.

Implements the remote contract used by the worker:

* ``POST /tasks`` is idempotent per *job*: an identical body always yields
  the same ticket and creates exactly one task; a different body under the
  same job id is rejected with 409.  Per-job fault injections can make the
  source drop the connection or answer 503 (both retryable, and the body
  of such responses is deliberately not valid JSON).
* ``GET /tasks/{ticket}`` answers pending / failed / ready.  A ready body
  carries the full ``{id, label}`` set (labels may be JSON null) taken
  from the fixture file.  Several "malformed body" modes let acceptance
  tests prove that only 200 responses are validated.
* Test-only routes under ``/test/...`` resolve tasks, inspect the store and
  reset the fault schedule.

Run with::

    python3 -I mock_source.py --fixture fixtures.json --port 9090
"""

import argparse
import json
import os
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import is_ascii_alnum  # noqa: E402


def canonical(body):
    """Canonical content key of a task submission (order-insensitive)."""
    rows = body.get("rows", [])
    return json.dumps(
        sorted((r["id"], r["text"]) for r in rows),
        separators=(",", ":"),
        ensure_ascii=False,
    )


# Ready bodies that violate the wire contract; used to prove that an
# illegal 200 body fails the job while retryable faults never get parsed.
BAD_BODIES = {
    "bad_json": b"this is not json {{{",
    "bad_shape": b'{"status":"ready"}',  # missing job / results
    "bad_job": None,                     # rendered dynamically (wrong job)
    "bad_ids": None,                     # rendered dynamically (id mismatch)
    "bad_label_type": None,              # label that is neither string/null
}


class MockSource:
    def __init__(self, fixture_path):
        self.lock = threading.Lock()
        with open(fixture_path, "r", encoding="utf-8") as fh:
            fixture = json.load(fh)
        # job name -> {"labels": {id: label-or-None}, "status": "...",
        #              "faults": {"post_drop": n, "post_503": n,
        #                        "bad_body": "mode"}}
        self.fixture = fixture["jobs"]
        # job -> {"ticket", "content", "remaining_faults": {...}}
        self.tasks = {}
        # ticket -> job
        self.ticket_index = {}

    def _job_cfg(self, job):
        return self.fixture.get(job)

    # -- POST /tasks --------------------------------------------------------

    def create_task(self, body, raw_writer):
        job = body.get("job")
        if not is_ascii_alnum(job or ""):
            return 400, {"error": "invalid job id"}
        if not isinstance(body.get("rows"), list):
            return 400, {"error": "rows must be an array"}
        for r in body["rows"]:
            if not isinstance(r, dict) or not is_ascii_alnum(r.get("id", "")) \
                    or not isinstance(r.get("text"), str):
                return 400, {"error": "invalid row"}
        if self._job_cfg(job) is None:
            return 404, {"error": "no fixture for job %r" % job}

        content = canonical(body)
        with self.lock:
            existing = self.tasks.get(job)
            created = False
            if existing is not None:
                # Idempotency key is the job; content must match.
                if existing["content"] != content:
                    return 409, {"error": "job already submitted with "
                                          "different content"}
                faults = existing["remaining_faults"]
            else:
                # The task is accepted and durably recorded *before* any
                # fault is simulated: a drop means "accepted then
                # disconnected", and the fault budget lives on the task so
                # retries consume it instead of re-seeding forever.
                ticket = "ticket-%s" % uuid.uuid4().hex[:12]
                initial = self._job_cfg(job).get("initial_status", "ready")
                faults = dict(self._job_cfg(job).get("faults", {}))
                existing = {
                    "ticket": ticket,
                    "content": content,
                    "status": initial,
                    "remaining_faults": faults,
                }
                self.tasks[job] = existing
                self.ticket_index[ticket] = job
                created = True

            mode = self._consume_post_fault(faults, raw_writer)
            if mode == "drop":
                return None  # connection was dropped; no response at all
            if mode == "503":
                # Deliberately unparseable: retryable faults must never be
                # body-validated.
                return 503, b"service unavailable, not json"

            return (201 if created else 200), {
                "ticket": existing["ticket"], "created": created}

    @staticmethod
    def _consume_post_fault(faults, raw_writer):
        if faults.get("post_drop", 0) > 0:
            faults["post_drop"] -= 1
            # Hard disconnect: write a partial line and close the socket.
            try:
                raw_writer(b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\npartial")
            except OSError:
                pass
            # The HTTP handler sets close_connection=True when we return
            # the sentinel, which tears down the socket mid-response.
            return "drop"
        if faults.get("post_503", 0) > 0:
            faults["post_503"] -= 1
            return "503"
        return None

    # -- GET /tasks/{ticket} ------------------------------------------------

    def get_task(self, ticket):
        with self.lock:
            job = self.ticket_index.get(ticket)
            if job is None:
                return 404, {"error": "unknown ticket"}
            task = self.tasks[job]
            cfg = self._job_cfg(job)
            status = task["status"]
            if status != "ready":
                # pending: no result field; failed: terminal failure.
                return 200, {"ticket": ticket, "status": status}

            bad = cfg.get("faults", {}).get("bad_body")
            if bad:
                if bad == "bad_json":
                    # Raw, deliberately unparseable 200 body.
                    return 200, b"this is not json {{{"
                return 200, self._bad_ready_body(bad, job, cfg, ticket)
            results = [
                {"id": rid, "label": cfg["labels"].get(rid)}
                for rid in sorted(cfg["labels"])
            ]
            return 200, {"ticket": ticket, "status": "ready",
                         "job": job, "results": results}

    @staticmethod
    def _bad_ready_body(mode, job, cfg, ticket):
        good = [{"id": rid, "label": cfg["labels"].get(rid)}
                for rid in sorted(cfg["labels"])]
        if mode == "bad_shape":
            return {"ticket": ticket, "status": "ready"}
        if mode == "bad_job":
            return {"ticket": ticket, "status": "ready",
                    "job": job + "-WRONG", "results": good}
        if mode == "bad_ids":
            broken = [dict(x, id=x["id"] + "X") for x in good]
            return {"ticket": ticket, "status": "ready",
                    "job": job, "results": broken}
        if mode == "bad_label_type":
            broken = [dict(x, label=123) for x in good]
            return {"ticket": ticket, "status": "ready",
                    "job": job, "results": broken}
        raise ValueError("unknown bad_body mode %r" % mode)

    # -- test control -------------------------------------------------------

    def resolve(self, body):
        job = body.get("job")
        ticket = body.get("ticket")
        status = body.get("status")
        if status not in ("ready", "failed", "pending"):
            return 400, {"error": "status must be pending|failed|ready"}
        with self.lock:
            if job is None and ticket is not None:
                job = self.ticket_index.get(ticket)
            task = self.tasks.get(job or "")
            if task is None:
                return 404, {"error": "unknown task"}
            # Terminal immutability: ready and failed are end states.  An
            # identical re-resolution is an idempotent no-op; any change is
            # refused with 409.
            if task["status"] in ("ready", "failed"):
                if task["status"] == status:
                    return 200, {"job": job, "ticket": task["ticket"],
                                 "status": status}
                return 409, {"error": "task already terminal",
                             "current": task["status"]}
            task["status"] = status
            return 200, {"job": job, "ticket": task["ticket"],
                         "status": status}

    def inspect(self):
        with self.lock:
            return 200, {
                "task_count": len(self.tasks),
                "tasks": [
                    {"job": job, "ticket": t["ticket"], "status": t["status"]}
                    for job, t in sorted(self.tasks.items())
                ],
            }


class Handler(BaseHTTPRequestHandler):
    server_version = "MockSource/1.0"

    def log_message(self, fmt, *args):
        if getattr(self.server, "verbose", False):
            sys.stderr.write("[mock] %s\n" % (fmt % args))

    def _send_json(self, status, obj):
        payload = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_raw(self, status, payload):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            return json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        src = self.server.source
        try:
            if path.startswith("/tasks/"):
                ticket = path[len("/tasks/"):]
                result = src.get_task(ticket)
                if result is None:
                    return
                status, obj = result
                if isinstance(obj, (bytes, bytearray)):
                    self._send_raw(status, bytes(obj))
                else:
                    self._send_json(status, obj)
            elif path == "/test/inspect":
                status, obj = src.inspect()
                self._send_json(status, obj)
            else:
                self._send_json(404, {"error": "unknown route"})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:
            # A handler bug must never look like a transport drop: emit a
            # real HTTP 500 so failures stay diagnosable.
            try:
                self._send_json(500, {"error": "handler failure: %r" % (exc,)})
            except Exception:
                self.close_connection = True

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        src = self.server.source
        try:
            if path == "/tasks":
                body = self._read_json()
                if body is None:
                    self._send_json(400, {"error": "invalid JSON"})
                    return

                def raw_writer(payload):
                    # Bypass the normal response path so we can then close.
                    self.wfile.write(payload)
                    self.wfile.flush()

                result = src.create_task(body, raw_writer)
                if result is None:
                    # Fault already wrote a partial response; kill socket.
                    self.close_connection = True
                    try:
                        self.wfile.flush()
                    except OSError:
                        pass
                    return
                status, obj = result
                if isinstance(obj, (bytes, bytearray)):
                    self._send_raw(status, bytes(obj))
                else:
                    self._send_json(status, obj)
            elif path == "/test/resolve":
                body = self._read_json() or {}
                status, obj = src.resolve(body)
                self._send_json(status, obj)
            else:
                self._send_json(404, {"error": "unknown route"})
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception as exc:
            try:
                self._send_json(500, {"error": "handler failure: %r" % (exc,)})
            except Exception:
                self.close_connection = True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixture", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    source = MockSource(args.fixture)
    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    httpd.source = source
    httpd.verbose = args.verbose
    httpd.allow_reuse_address = True
    sys.stderr.write("[mock] listening on 127.0.0.1:%d fixture=%s\n"
                     % (args.port, args.fixture))
    sys.stderr.flush()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
