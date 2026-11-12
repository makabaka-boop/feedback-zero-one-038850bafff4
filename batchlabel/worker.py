"""Worker process.

One worker is one OS process; the acceptance tests run two of them against
the same database to prove cross-process lease takeover.  Per loop the
worker:

1. claims an eligible job (no holder, or ``now`` has reached the lease
   expiry) and receives a fresh epoch;
2. submits the task to the remote source, retrying *only* on disconnects
   and 503 responses (whose bodies are never inspected) -- the remote
   idempotency key makes retries safe;
3. records the ticket locally, then polls the source, renewing the lease
   on every poll and persisting progress;
4. validates a ``ready`` 200 body strictly (job match, one-to-one ids,
   string/null labels) and either publishes atomically via the backend or
   fails the job.  Any 409 from the backend means the epoch was superseded
   or the lease expired: the worker silently abandons the job.

Crash hooks (file driven):

* ``worker_pause`` -- block before the remote POST until the hook file is
  removed by the test, keeping the old holder alive while the clock moves;
* ``worker_exit_after_accept`` -- hard-exit after the source returns a
  ticket but before it is persisted locally.
"""

import argparse
import json
import os
import sys
import time
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import http_json, is_ascii_alnum  # noqa: E402


class Worker:
    def __init__(self, server_url, source_url, hook_path, name, interval):
        self.server = server_url.rstrip("/")
        self.source = source_url.rstrip("/")
        self.hook_path = hook_path or None
        self.name = name
        self.interval = interval

    def log(self, msg):
        sys.stderr.write("[worker %s] %s\n" % (self.name, msg))
        sys.stderr.flush()

    # -- hook file ----------------------------------------------------------

    def _hook_rules(self):
        if not self.hook_path or not os.path.exists(self.hook_path):
            return {}
        try:
            with open(self.hook_path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (ValueError, OSError):
            return {}

    def wait_if_paused(self, job_id):
        """Block while a ``worker_pause`` rule for this job is present."""
        while True:
            rule = self._hook_rules().get("worker_pause")
            if not rule or rule.get("job") != job_id:
                return
            self.log("paused by hook for job=%s" % job_id)
            time.sleep(0.1)

    def maybe_exit_after_accept(self, job_id):
        rules = self._hook_rules()
        rule = rules.get("worker_exit_after_accept")
        if rule and rule.get("job") == job_id:
            # Remove first so a restarted worker never re-fires it.
            try:
                os.remove(self.hook_path)
            except OSError:
                pass
            self.log("CRASH after remote accept, before ticket record "
                     "(job=%s)" % job_id)
            sys.stderr.flush()
            os._exit(2)

    # -- backend calls ------------------------------------------------------

    def _post(self, path, body):
        """POST to the backend.

        Returns ``(status, body)``; a 409 signals a lost lease / superseded
        epoch / terminal job and is handled by the caller by abandoning.
        """
        try:
            return http_json("POST", self.server + path, body, timeout=5.0)
        except urllib.error.URLError as exc:
            self.log("backend unreachable (%s); retrying" % exc)
            time.sleep(self.interval)
            return None, None

    @staticmethod
    def _classify(status):
        """Classify an internal backend response.

        ``ok``       -- the guarded write landed;
        ``abandon``  -- 409: epoch superseded, lease lost or terminal;
        ``retry``    -- transport failure (None) or a transient 5xx.
        Anything else is a permanent, non-retryable protocol error.
        """
        if status is None:
            return "retry"
        if 200 <= status < 300:
            return "ok"
        if status == 409:
            return "abandon"
        if 500 <= status < 600:
            return "retry"
        return "error"

    def claim(self):
        status, body = self._post("/internal/claim", {})
        if status == 204 or body is None:
            return None
        if status != 200:
            self.log("unexpected claim status %s" % status)
            time.sleep(self.interval)
            return None
        return body

    # -- main loop -----------------------------------------------------------

    def run(self):
        self.log("started server=%s source=%s" % (self.server, self.source))
        while True:
            try:
                claimed = self.claim()
            except Exception as exc:  # pragma: no cover - defensive
                self.log("claim error %r" % (exc,))
                time.sleep(self.interval)
                continue
            if claimed is None:
                time.sleep(self.interval)
                continue
            self.process(claimed)

    def process(self, claimed):
        job_id = claimed["job"]
        epoch = claimed["epoch"]
        rows = claimed["rows"]
        input_ids = sorted(r["id"] for r in rows)
        self.log("claimed job=%s epoch=%d rows=%d"
                 % (job_id, epoch, len(rows)))

        # Hold here while the test wants this owner kept alive (stale-epoch
        # takeover scenario).
        self.wait_if_paused(job_id)

        ticket = self.submit_remote(job_id, epoch, rows)
        if ticket is None:
            return  # lease lost while submitting
        self.log("job=%s ticket=%s" % (job_id, ticket))

        # Poll until terminal.
        while True:
            status, body = self.poll_once(job_id, epoch, ticket, input_ids)
            if status == "abandon":
                self.log("abandoning job=%s (lease/epoch lost)" % job_id)
                return
            if status == "terminal":
                return
            time.sleep(self.interval)

    # -- remote submission ---------------------------------------------------

    def submit_remote(self, job_id, epoch, rows):
        """Return the ticket, or None if the job must be abandoned."""
        url = self.source + "/tasks"
        body = {"job": job_id, "rows": rows}
        while True:
            try:
                status, parsed = http_json("POST", url, body, timeout=5.0)
            except urllib.error.URLError as exc:
                # Disconnect after possible acceptance: the POST is
                # idempotent per job, so retrying cannot create a duplicate.
                self.log("source disconnect on POST (%s); retry" % exc)
                time.sleep(self.interval)
                continue

            if status == 503:
                self.log("source 503 on POST; retry without parsing body")
                time.sleep(self.interval)
                continue
            if status == 409:
                # Content conflict under a reused job id is a permanent
                # protocol violation -> fail through the backend guard.
                self.backend_fail(job_id, epoch, "remote 409 content conflict")
                return None
            if status != 200 and status != 201:
                self.backend_fail(job_id, epoch,
                                  "remote POST status %s" % status)
                return None

            ticket = parsed.get("ticket") if isinstance(parsed, dict) else None
            if not isinstance(ticket, str) or not ticket:
                # Malformed accept body: safe to retry an idempotent POST.
                self.log("malformed accept body; retry")
                time.sleep(self.interval)
                continue

            # Remote has accepted; crash point #1 lives here, before the
            # ticket is durably recorded in the backend database.
            self.maybe_exit_after_accept(job_id)

            st, _ = self._post("/internal/ticket",
                               {"job": job_id, "epoch": epoch,
                                "ticket": ticket})
            kind = self._classify(st)
            if kind == "abandon":
                return None
            if kind == "retry":
                # Backend transiently unreachable.  Re-report the same
                # ticket (idempotent column write) until accepted.
                time.sleep(self.interval)
                continue
            if kind == "error":
                self.log("ticket write rejected with status %s" % st)
                return None
            return ticket

    # -- polling --------------------------------------------------------------

    def poll_once(self, job_id, epoch, ticket, input_ids):
        # Renew first; every poll is also a lease liveness signal.
        st, _ = self._post("/internal/renew",
                           {"job": job_id, "epoch": epoch})
        kind = self._classify(st)
        if kind == "abandon":
            return "abandon", None
        if kind == "retry":
            return "retry", None
        if kind == "error":
            self.log("renew rejected with status %s; abandoning" % st)
            return "abandon", None

        try:
            status, parsed = http_json(
                "GET", self.source + "/tasks/" + ticket, timeout=5.0)
        except urllib.error.URLError as exc:
            # Disconnect: no body, nothing to validate, just retry.
            self.log("source disconnect on poll (%s)" % exc)
            return "retry", None

        if status == 503:
            self.log("source 503 on poll; no body validation")
            return "retry", None
        if status != 200:
            # Any other status is a permanent failure of the task.
            self.backend_fail(job_id, epoch,
                              "remote poll status %s" % status)
            return "terminal", None

        if not isinstance(parsed, dict):
            # A 200 with an unparseable/non-object body is an illegal body:
            # only a valid 200 body is ever validated, and this one fails.
            self.backend_fail(job_id, epoch,
                              "illegal body: response is not a JSON object")
            return "terminal", None

        remote_status = parsed.get("status")
        if remote_status == "pending":
            st, _ = self._post("/internal/progress",
                               {"job": job_id, "epoch": epoch, "amount": 1})
            kind = self._classify(st)
            if kind == "abandon":
                return "abandon", None
            if kind == "retry":
                return "retry", None
            if kind == "error":
                self.log("progress rejected with status %s; abandoning" % st)
                return "abandon", None
            return "retry", None
        if remote_status == "failed":
            self.backend_fail(job_id, epoch, "remote terminal failure")
            return "terminal", None
        if remote_status != "ready":
            # Missing/garbage status on a 200: illegal body -> job fails.
            self.backend_fail(job_id, epoch,
                              "illegal body: bad status %r" % (remote_status,))
            return "terminal", None

        labels, error = self.validate_ready(parsed, job_id, input_ids)
        if error is not None:
            self.log("illegal ready body: %s" % error)
            self.backend_fail(job_id, epoch, "illegal ready body: " + error)
            return "terminal", None

        st, view = self._post("/internal/complete",
                              {"job": job_id, "epoch": epoch, "labels": labels})
        kind = self._classify(st)
        if kind == "abandon":
            # A stale holder's success publish is rejected: state untouched.
            self.log("complete rejected (epoch superseded/expired)")
            return "abandon", None
        if kind == "retry":
            return "retry", None
        if kind == "error":
            self.log("complete rejected with status %s; abandoning" % st)
            return "abandon", None
        self.log("job=%s published with %d labels" % (job_id, len(labels)))
        return "terminal", None

    @staticmethod
    def validate_ready(parsed, job_id, input_ids):
        """Strict 200-body validation; returns (labels, error)."""
        if not isinstance(parsed, dict):
            return None, "body is not an object"
        if parsed.get("job") != job_id:
            return None, "ready job does not match claimed job"
        results = parsed.get("results")
        if not isinstance(results, list):
            return None, "results is not an array"
        labels = []
        seen = set()
        for i, item in enumerate(results):
            if not isinstance(item, dict):
                return None, "results/%d is not an object" % i
            rid = item.get("id")
            if not isinstance(rid, str) or not is_ascii_alnum(rid):
                return None, "results/%d has invalid id" % i
            if rid in seen:
                return None, "duplicate result id %r" % rid
            seen.add(rid)
            if "label" not in item:
                return None, "results/%d missing label" % i
            label = item["label"]
            if label is not None and not isinstance(label, str):
                return None, "results/%d label must be string or null" % i
            labels.append({"id": rid, "label": label})
        if seen != set(input_ids):
            return None, "result ids do not correspond one-to-one with input"
        return labels, None

    def backend_fail(self, job_id, epoch, reason):
        # Bounded retries: the lease guard turns into a 409 once the lease
        # lapses, at which point a takeover owner is authoritative.
        for _ in range(100):
            st, body = self._post("/internal/fail",
                                  {"job": job_id, "epoch": epoch,
                                   "reason": reason})
            kind = self._classify(st)
            if kind == "abandon":
                # A stale holder's failure must not change takeover state.
                self.log("fail write rejected for job=%s (stale holder); "
                         "state unchanged" % job_id)
                return
            if kind == "ok":
                self.log("job=%s marked failed: %s" % (job_id, reason))
                return
            if kind == "error":
                self.log("fail write rejected with status %s for job=%s"
                         % (st, job_id))
                return
            time.sleep(self.interval)
        self.log("fail write gave up after retries for job=%s" % job_id)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--source", required=True)
    ap.add_argument("--crash-hook", default="")
    ap.add_argument("--name", default="w1")
    ap.add_argument("--interval", type=float, default=0.1)
    args = ap.parse_args()
    Worker(args.server, args.source, args.crash_hook,
           args.name, args.interval).run()


if __name__ == "__main__":
    main()
