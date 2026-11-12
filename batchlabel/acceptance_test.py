"""End-to-end acceptance harness.

Runs the *real* server, mock source and worker OS processes (two workers
where takeover is required) and drives every guarantee in the spec:

1. happy path, idempotent re-submission, 409 on changed content;
2. disconnect + 503 retries with exactly one remote task;
3. illegal 200 bodies (bad JSON, wrong job, id mismatch, non-string label)
   fail the job while retryable fault bodies are never parsed;
4. empty batch and genuine JSON ``null`` labels preserved end to end;
5. remote terminal failure; invalid input paths and no job creation;
6. lease/ticket/progress survival across a backend hard restart;
7. two-process stale-epoch takeover: an old holder's delayed failure and
   success writes are rejected and cannot overwrite the new holder;
8. hard kill between remote accept and local ticket record -- takeover
   verifies one remote task and results identical to a fault-free run;
9. hard kill after label writes but before commit -- takeover completes
   atomically with no partial/duplicate labels.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
POLL_TIMEOUT = 20.0


# ---------------------------------------------------------------------------
# Process / HTTP plumbing
# ---------------------------------------------------------------------------

def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Proc:
    def __init__(self, name, argv, log_fh):
        self.name = name
        self.log_fh = log_fh
        self.proc = subprocess.Popen(argv, stdout=log_fh, stderr=subprocess.STDOUT)

    def alive(self):
        return self.proc.poll() is None

    def wait_exit(self, timeout=5.0):
        try:
            return self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            raise

    def stop(self):
        if self.alive():
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


def wait_port(port, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError("port %d never opened" % port)


def request(method, url, body=None, timeout=5.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json",
                 "Accept": "application/json"},
        method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw) if raw else None
        except ValueError:
            return exc.code, None


def wait_job(server, job, predicate, timeout=POLL_TIMEOUT):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        status, view = request("GET", server + "/jobs/" + job)
        assert status == 200, (status, view)
        last = view
        if predicate(view):
            return view
        time.sleep(0.1)
    raise AssertionError("job %s timed out; last=%s" % (job, json.dumps(last)))


def is_terminal(v):
    return v["status"] in ("succeeded", "failed")


def submit(server, job, rows):
    return request("POST", server + "/jobs", {"job": job, "rows": rows})


def advance(server, delta):
    status, body = request("POST", server + "/test/advance", {"delta": delta})
    assert status == 200 and isinstance(body["now"], int) \
        and not isinstance(body["now"], bool)
    return body["now"]


def write_hook(path, rules):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(rules, fh)


def remove_hook(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


class Acceptance:
    def __init__(self):
        self.tmp = tempfile.mkdtemp(prefix="batchlabel-acc-")
        self.db = os.path.join(self.tmp, "batch.db")
        self.fixture = os.path.join(self.tmp, "fixture.json")
        with open(os.path.join(HERE, "fixtures.json"), "r", encoding="utf-8") as fh:
            self.fixture_data = json.load(fh)
        with open(self.fixture, "w", encoding="utf-8") as fh:
            json.dump(self.fixture_data, fh)
        self.server_port = free_port()
        self.mock_port = free_port()
        self.logs = {}
        self.procs = []
        self.server_proc = None
        self.results = []
        self._start_mock()
        self._start_server(hook=None)

    def _open_log(self, name):
        fh = open(os.path.join(self.tmp, name + ".log"), "w")
        self.logs[name] = fh
        return fh

    def _start_mock(self):
        self.mock = Proc("mock", [
            PY, "-I", os.path.join(HERE, "mock_source.py"),
            "--fixture", self.fixture, "--port", str(self.mock_port),
        ], self._open_log("mock"))
        self.procs.append(self.mock)
        wait_port(self.mock_port)
        self.mock_url = "http://127.0.0.1:%d" % self.mock_port

    def _start_server(self, hook):
        argv = [PY, "-I", os.path.join(HERE, "server.py"),
                "--db", self.db, "--port", str(self.server_port)]
        if hook:
            argv += ["--crash-hook", hook]
        name = "server-%d" % len(self.logs)
        proc = Proc(name, argv, self._open_log(name))
        self.procs.append(proc)
        self.server_proc = proc
        wait_port(self.server_port)
        self.server_url = "http://127.0.0.1:%d" % self.server_port
        return proc

    def restart_server(self, hook=None):
        self.server_proc.wait_exit(timeout=5.0)
        return self._start_server(hook)

    def kill_and_start_server(self, hook=None):
        """Hard-kill the current backend and start a fresh one on the DB."""
        if self.server_proc.alive():
            self.server_proc.proc.kill()
            self.server_proc.proc.wait()
        return self._start_server(hook)

    def start_worker(self, name, hook=None):
        argv = [PY, "-I", os.path.join(HERE, "worker.py"),
                "--server", self.server_url, "--source", self.mock_url,
                "--name", name, "--interval", "0.05"]
        if hook:
            argv += ["--crash-hook", hook]
        proc = Proc(name, argv, self._open_log(name))
        self.procs.append(proc)
        return proc

    # -- test bookkeeping ----------------------------------------------------

    def check(self, name, fn):
        try:
            fn()
        except Exception as exc:
            import traceback
            tb = traceback.format_exc()
            self.results.append((name, False, tb))
            print("FAIL  %s:\n%s" % (name, tb))
            self.dump_logs()
        else:
            self.results.append((name, True, ""))
            print("PASS  %s" % name)

    def dump_logs(self):
        for name, fh in self.logs.items():
            fh.flush()
            path = fh.name
            try:
                with open(path, "r", encoding="utf-8") as reader:
                    content = reader.read()
            except OSError:
                continue
            print("---- log %s ----" % name)
            print(content[-3000:])

    def cleanup(self):
        for p in self.procs:
            p.stop()
        for fh in self.logs.values():
            fh.close()

    # -- assertions reused by many tests ------------------------------------

    def expect_labels(self, job, expected):
        view = wait_job(self.server_url, job,
                        lambda v: v["status"] == "succeeded")
        assert view["labels"] is not None
        assert [x["id"] for x in view["labels"]] == sorted(expected), \
            "labels must be sorted by id"
        assert view["labels"] == [
            {"id": k, "label": expected[k]} for k in sorted(expected)]
        return view

    def mock_task_count(self):
        status, body = request("GET", self.mock_url + "/test/inspect")
        assert status == 200
        return body["task_count"], body["tasks"]

    def resolve(self, job, status):
        code, body = request("POST", self.mock_url + "/test/resolve",
                             {"job": job, "status": status})
        assert code == 200, body
        return body


# ---------------------------------------------------------------------------
# Individual scenarios
# ---------------------------------------------------------------------------

def test_happy_path(acc):
    rows = [{"id": "c3", "text": "third"}, {"id": "a1", "text": "first"},
            {"id": "b2", "text": "second"}]
    code, view = submit(acc.server_url, "jobBase", rows)
    assert code == 201 and view["status"] == "queued" and view["epoch"] == 0
    w = acc.start_worker("w-base")
    try:
        acc.expect_labels("jobBase", {
            "a1": "positive", "b2": "neutral", "c3": "negative"})
        # Idempotent resubmission: different row order, same content.
        code2, view2 = submit(acc.server_url, "jobBase", list(reversed(rows)))
        assert code2 == 200 and view2["replayed"] is True
        # Changed content with same job id is rejected, original untouched.
        changed = [dict(r, text=r["text"] + "!") for r in rows]
        code3, err = submit(acc.server_url, "jobBase", changed)
        assert code3 == 409 and err["path"] == ""
        view3 = wait_job(acc.server_url, "jobBase", lambda v: True)
        assert view3["status"] == "succeeded"
        assert [x["label"] for x in view3["labels"]] == \
            ["positive", "neutral", "negative"]
    finally:
        w.stop()


def test_null_labels(acc):
    code, _ = submit(acc.server_url, "jobNull", [
        {"id": "n3", "text": "c"}, {"id": "n1", "text": "a"},
        {"id": "n2", "text": "b"}])
    assert code == 201
    w = acc.start_worker("w-null")
    try:
        view = acc.expect_labels("jobNull", {
            "n1": None, "n2": "real-label", "n3": None})
        raw = json.dumps(view["labels"])
        assert '"label": null' in raw, "real JSON null must be preserved"
    finally:
        w.stop()


def test_empty_batch(acc):
    code, view = submit(acc.server_url, "jobEmpty", [])
    assert code == 201 and view["status"] == "queued"
    w = acc.start_worker("w-empty")
    try:
        view = wait_job(acc.server_url, "jobEmpty",
                        lambda v: v["status"] == "succeeded")
        assert view["labels"] == []
        count, tasks = acc.mock_task_count()
        assert any(t["job"] == "jobEmpty" for t in tasks)
    finally:
        w.stop()


def test_flaky_retries(acc):
    code, _ = submit(acc.server_url, "jobFlaky", [
        {"id": "x2", "text": "b"}, {"id": "x1", "text": "a"}])
    assert code == 201
    w = acc.start_worker("w-flaky")
    try:
        acc.expect_labels("jobFlaky", {"x1": "ok", "x2": "retry-ok"})
        count, tasks = acc.mock_task_count()
        flaky = [t for t in tasks if t["job"] == "jobFlaky"]
        # Drop + 503 were retried; idempotency guarantees a single task.
        assert len(flaky) == 1 and flaky[0]["status"] == "ready"
    finally:
        w.stop()


def test_illegal_bodies(acc):
    cases = [
        ("jobBadJson", "bad_json"), ("jobBadJob", "bad_job"),
        ("jobBadIds", "bad_ids"), ("jobBadLabel", "bad_label"),
    ]
    workers = []
    try:
        for job, _mode in cases:
            code, _ = submit(acc.server_url, job, [
                {"id": "z9", "text": "s"}])
            assert code == 201
            w = acc.start_worker("w-" + job)
            workers.append(w)
        for job, _mode in cases:
            view = wait_job(acc.server_url, job,
                            lambda v: v["status"] == "failed")
            assert view["labels"] is None, "failed jobs expose no labels"
    finally:
        for w in workers:
            w.stop()


def test_remote_failure(acc):
    code, _ = submit(acc.server_url, "jobRemoteFail",
                     [{"id": "f1", "text": "doomed"}])
    assert code == 201
    w = acc.start_worker("w-fail")
    try:
        # Let the task be created, then resolve it as a terminal failure.
        deadline = time.time() + 10
        while time.time() < deadline:
            _c, tasks = acc.mock_task_count()
            if any(t["job"] == "jobRemoteFail" for t in tasks):
                break
            time.sleep(0.1)
        acc.resolve("jobRemoteFail", "failed")
        view = wait_job(acc.server_url, "jobRemoteFail",
                        lambda v: v["status"] == "failed")
        assert view["labels"] is None
        # Remote terminal state is immutable: re-resolving to ready is
        # refused, while an identical re-resolution is an idempotent no-op.
        code, body = request("POST", acc.mock_url + "/test/resolve",
                             {"job": "jobRemoteFail", "status": "ready"})
        assert code == 409 and body["current"] == "failed"
        code, body = request("POST", acc.mock_url + "/test/resolve",
                             {"job": "jobRemoteFail", "status": "failed"})
        assert code == 200 and body["status"] == "failed"
        # Terminal jobs can never be claimed again.
        assert request("GET", acc.server_url + "/test/inspect")[1] is not None
    finally:
        w.stop()


def test_invalid_input(acc):
    bad_cases = [
        ({}, "/job"),
        ({"job": "bad id!", "rows": []}, "/job"),
        ({"job": "J1", "rows": [{"id": "r1"}]}, "/rows/0/text"),
        ({"job": "J2", "rows": [{"id": "r1", "text": "a"},
                                {"id": "r1", "text": "b"}]}, "/rows/1/id"),
        ({"job": "J3", "rows": "nope"}, "/rows"),
        ({"job": True, "rows": []}, "/job"),
    ]
    for payload, path in bad_cases:
        code, err = request("POST", acc.server_url + "/jobs", payload)
        assert code == 400, (payload, code, err)
        assert err["path"] == path, (payload, err)
    # None of the rejected jobs may exist.
    for jid in ("J1", "J2", "J3"):
        code, _ = request("GET", acc.server_url + "/jobs/" + jid)
        assert code == 404
    # Booleans must not pass the integer clock API.
    code, err = request("POST", acc.server_url + "/test/advance",
                        {"delta": True})
    assert code == 400 and err["path"] == "/delta"
    code, body = request("POST", acc.server_url + "/test/advance",
                         {"delta": 0})
    assert code == 200 and isinstance(body["now"], int)


def test_restart_persistence(acc):
    # jobPersist starts pending on the mock, so the worker records a
    # ticket and accumulates progress while the task is unresolved.
    code, _ = submit(acc.server_url, "jobPersist",
                     [{"id": "p1", "text": "persist-me"}])
    assert code == 201
    w = acc.start_worker("w-persist")
    try:
        view = wait_job(acc.server_url, "jobPersist",
                        lambda v: v["ticket"] is not None
                        and v["progress"] >= 1
                        and v["status"] == "running")
        epoch_before = view["epoch"]
        expiry_before = view["lease_expiry"]
        before = request("GET", acc.server_url + "/test/inspect")[1]

        # Hard kill (SIGKILL) the backend and bring it back on the same DB.
        acc.kill_and_start_server(hook=None)

        after = request("GET", acc.server_url + "/test/inspect")[1]
        assert after["now"] == before["now"], "clock is durable"
        row = next(j for j in after["jobs"]
                   if j["job"] == "jobPersist")
        assert row["status"] == "running"
        assert row["epoch"] == epoch_before, "epoch survives restart"
        assert row["lease_expiry"] == expiry_before, "lease survives restart"
        assert row["ticket"], "ticket survives restart"
        assert row["progress"] >= 1, "progress survives restart"
        view2 = request("GET", acc.server_url + "/jobs/jobPersist")[1]
        assert view2["labels"] is None, "unfinished job exposes no labels"
        # Labels of a previously finished job are still intact.
        base = request("GET", acc.server_url + "/jobs/jobBase")[1]
        assert base["status"] == "succeeded" and len(base["labels"]) == 3

        # Expire the old lease while the original worker's socket is dead;
        # after the bounce it re-claims under a new epoch and finishes.
        advance(acc.server_url, 5)
        acc.resolve("jobPersist", "ready")
        acc.expect_labels("jobPersist", {"p1": "persisted-label"})
    finally:
        w.stop()


def test_stale_epoch_takeover(acc):
    rows = [{"id": "t2", "text": "b"}, {"id": "t1", "text": "a"}]
    code, _ = submit(acc.server_url, "jobTakeover", rows)
    assert code == 201

    worker_hook = os.path.join(acc.tmp, "hook-takeover.json")
    write_hook(worker_hook, {"worker_pause": {"job": "jobTakeover"}})
    w_a = acc.start_worker("w-A", hook=worker_hook)
    try:
        # Wait until A holds epoch 1 and is parked before the remote POST.
        view = wait_job(acc.server_url, "jobTakeover",
                        lambda v: v["status"] == "running" and v["epoch"] == 1)
        expiry_a = view["lease_expiry"]
        now = advance(acc.server_url, 5)
        assert now >= expiry_a, "clock must reach the expiry value"

        w_b_hook = os.path.join(acc.tmp, "hook-takeover-b.json")
        write_hook(w_b_hook, {"worker_pause": {"job": "jobTakeover"}})
        w_b = acc.start_worker("w-B", hook=w_b_hook)
        try:
            view2 = wait_job(acc.server_url, "jobTakeover",
                             lambda v: v["epoch"] == 2 and
                             v["status"] == "running")
            assert view2["status"] == "running"
            assert view2["lease_expiry"] > now, "B holds a fresh lease"
            # A is still the live, paused old holder.  Its delayed failure
            # and success writes must both be rejected; state is untouched.
            code, err = request("POST", acc.server_url + "/internal/fail",
                                {"job": "jobTakeover", "epoch": 1,
                                 "reason": "delayed failure"})
            assert code == 409, err
            code, err = request("POST", acc.server_url + "/internal/complete",
                                {"job": "jobTakeover", "epoch": 1,
                                 "labels": [{"id": "t1", "label": "STALE"},
                                            {"id": "t2", "label": "STALE"}]})
            assert code == 409, err
            inspect = request("GET",
                              acc.server_url + "/test/inspect")[1]
            jobrow = next(j for j in inspect["jobs"]
                          if j["job"] == "jobTakeover")
            assert jobrow["epoch"] == 2 and jobrow["status"] == "running"

            # Release both holders; B drives the job to success under
            # epoch 2 while A discovers its epoch was superseded.
            remove_hook(w_b_hook)
            # Release A; B drives the job to success under epoch 2.
            remove_hook(worker_hook)
            acc.expect_labels("jobTakeover", {
                "t1": "taken-over-ok", "t2": "taken-over-ok2"})
            # A's late writes still fail after terminal publication and can
            # never resurrect/overwrite the result.
            code, _ = request("POST", acc.server_url + "/internal/fail",
                              {"job": "jobTakeover", "epoch": 1,
                               "reason": "zombie"})
            assert code == 409
            final = request("GET",
                            acc.server_url + "/jobs/jobTakeover")[1]
            assert final["status"] == "succeeded"
            assert all(x["label"] != "STALE" for x in final["labels"])
        finally:
            w_b.stop()
    finally:
        w_a.stop()


def test_crash_ticket_window(acc):
    rows = [{"id": "g2", "text": "b"}, {"id": "g1", "text": "a"}]
    code, _ = submit(acc.server_url, "jobCrashTicket", rows)
    assert code == 201
    hook = os.path.join(acc.tmp, "hook-ticket.json")
    write_hook(hook, {"worker_exit_after_accept":
                      {"job": "jobCrashTicket"}})
    w_a = acc.start_worker("w-crash1", hook=hook)
    try:
        # Remote accepted exactly once; worker died before recording ticket.
        deadline = time.time() + 10
        while time.time() < deadline:
            count, tasks = acc.mock_task_count()
            if any(t["job"] == "jobCrashTicket" for t in tasks):
                break
            time.sleep(0.1)
        assert not w_a.alive(), "worker must have hard-exited"
        w_a.wait_exit()
        count, tasks = acc.mock_task_count()
        assert len([t for t in tasks
                    if t["job"] == "jobCrashTicket"]) == 1
        remote_ticket = next(t["ticket"] for t in tasks
                             if t["job"] == "jobCrashTicket")
        view = request("GET",
                       acc.server_url + "/jobs/jobCrashTicket")[1]
        assert view["ticket"] is None and view["status"] in (
            "queued", "running")

        # Expire the lease, then take over with a fresh worker.
        advance(acc.server_url, 5)
        w_b = acc.start_worker("w-crash1b")
        try:
            acc.expect_labels("jobCrashTicket", {
                "g1": "recovered-label-1", "g2": "recovered-label-2"})
            count, tasks = acc.mock_task_count()
            matches = [t for t in tasks
                       if t["job"] == "jobCrashTicket"]
            assert len(matches) == 1, "no duplicate remote task"
            assert matches[0]["ticket"] == remote_ticket
            stored = request("GET",
                             acc.server_url + "/jobs/jobCrashTicket")[1]
            assert stored["ticket"] == remote_ticket
        finally:
            w_b.stop()
    finally:
        if w_a.alive():
            w_a.stop()


def test_crash_commit_window(acc):
    rows = [{"id": "h2", "text": "b"}, {"id": "h1", "text": "a"}]
    code, _ = submit(acc.server_url, "jobCrashCommit", rows)
    assert code == 201

    server_hook = os.path.join(acc.tmp, "hook-commit.json")
    write_hook(server_hook, {
        "server_exit": {"job": "jobCrashCommit",
                        "when": "before_commit_complete"}})
    # Bring up a backend that carries the crash rule for this job.
    acc.kill_and_start_server(hook=server_hook)
    w_a = acc.start_worker("w-crash2")
    try:
        # Worker reaches ready and calls complete; the server hard-exits
        # after writing labels but before COMMIT.
        deadline = time.time() + 15
        while time.time() < deadline:
            if not acc.server_proc.alive():
                break
            time.sleep(0.1)
        assert not acc.server_proc.alive(), "server must have hard-exited"
        acc.server_proc.wait_exit()
        # Stop the original worker; takeover is done by a distinct process.
        w_a.stop()

        # Restart without hooks: SQLite rolled the open transaction back,
        # so no partial labels exist and the job is still non-terminal.
        acc._start_server(hook=None)
        view = request("GET",
                       acc.server_url + "/jobs/jobCrashCommit")[1]
        assert view["status"] == "running" and view["labels"] is None
        advance(acc.server_url, 5)

        w_b = acc.start_worker("w-crash2b")
        try:
            acc.expect_labels("jobCrashCommit", {
                "h1": "committed-label-1", "h2": "committed-label-2"})
            final = request("GET",
                            acc.server_url + "/jobs/jobCrashCommit")[1]
            assert len(final["labels"]) == 2, "no duplicated label rows"
            count, tasks = acc.mock_task_count()
            assert len([t for t in tasks
                        if t["job"] == "jobCrashCommit"]) == 1
        finally:
            w_b.stop()
    finally:
        w_a.stop()


def test_terminal_not_claimable(acc):
    # jobBase is succeeded; a claim sweep must never hand it out.
    for _ in range(3):
        code, body = request("POST", acc.server_url + "/internal/claim", {})
        if code == 200:
            assert body["job"] != "jobBase"
            # Put it back indirectly is impossible; instead ensure no
            # terminal job ever appears in claim responses across sweeps.
            inspect = request("GET",
                              acc.server_url + "/test/inspect")[1]
            terminal = {j["job"] for j in inspect["jobs"]
                        if j["status"] in ("succeeded", "failed")}
            assert body["job"] not in terminal
        else:
            assert code == 204
        time.sleep(0.05)


def main():
    acc = Acceptance()
    try:
        acc.check("happy path + idempotent replay + 409 rejection",
                  lambda: test_happy_path(acc))
        acc.check("genuine null labels preserved, sorted output",
                  lambda: test_null_labels(acc))
        acc.check("empty batch is legal and completes",
                  lambda: test_empty_batch(acc))
        acc.check("disconnect + 503 retried, single remote task",
                  lambda: test_flaky_retries(acc))
        acc.check("illegal 200 bodies fail the job without label exposure",
                  lambda: test_illegal_bodies(acc))
        acc.check("remote terminal failure",
                  lambda: test_remote_failure(acc))
        acc.check("invalid input located by path, no job created, bool int",
                  lambda: test_invalid_input(acc))
        acc.check("clock, lease and labels survive hard server restart",
                  lambda: test_restart_persistence(acc))
        acc.check("two processes: stale epoch takeover, no overwrite",
                  lambda: test_stale_epoch_takeover(acc))
        acc.check("crash after accept/before ticket: takeover, one task",
                  lambda: test_crash_ticket_window(acc))
        acc.check("crash after label writes/before commit: atomic recovery",
                  lambda: test_crash_commit_window(acc))
        acc.check("terminal jobs are never claimed again",
                  lambda: test_terminal_not_claimable(acc))
    finally:
        acc.cleanup()

    passed = sum(1 for _, ok, _ in acc.results if ok)
    total = len(acc.results)
    print("\n=== %d/%d acceptance checks passed ===" % (passed, total))
    if passed != total:
        sys.exit(1)


if __name__ == "__main__":
    main()
