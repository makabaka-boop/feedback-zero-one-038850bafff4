"""Shared primitives for the batch labelling service.

Everything here uses only the Python standard library so the service can
run with ``python3 -I`` in hardened environments.
"""

import http.client
import json
import urllib.error
import urllib.request


class TransportError(urllib.error.URLError):
    """A connection-level failure (reset, drop, timeout, refused).

    Distinct from an HTTP status: nothing about a response body can be
    assumed, so callers treat this as retryable without validation.  It
    subclasses URLError so existing ``except urllib.error.URLError``
    handlers keep working.
    """


class ValidationError(Exception):
    """Raised when an HTTP request body is malformed.

    ``path`` uses JSON-Pointer style segments (``""`` is the document root)
    so callers can report exactly which field was illegal.
    """

    def __init__(self, path, message):
        super().__init__(message if not path else "%s: %s" % (path, message))
        self.path = path
        self.message = message


class HttpError(Exception):
    """An HTTP response with a non-2xx status from an outbound call."""

    def __init__(self, status, body=b""):
        super().__init__("HTTP %s" % status)
        self.status = status
        self.body = body


# ---------------------------------------------------------------------------
# Strict validation helpers
# ---------------------------------------------------------------------------

def is_ascii_alnum(value):
    """A non-empty string of ASCII letters/digits only."""
    return (
        isinstance(value, str)
        and len(value) > 0
        and all(("a" <= ch <= "z") or ("A" <= ch <= "Z") or ("0" <= ch <= "9")
                for ch in value)
    )


def require_object(value, path):
    if not isinstance(value, dict):
        raise ValidationError(path, "expected a JSON object")
    return value


def require_string(value, path):
    if not isinstance(value, bool) and isinstance(value, str):
        return value
    raise ValidationError(path, "expected a string")


def require_nonneg_int(value, path):
    """Non-negative *integer*: booleans are explicitly rejected.

    In Python ``bool`` is a subclass of ``int``; the spec forbids using a
    boolean where an integer clock/epoch/progress value is required, so the
    bool check has to come first everywhere.
    """
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationError(path, "expected a non-negative integer")
    return value


def validate_submission(payload):
    """Validate a ``POST /jobs`` body and return the canonical row list.

    Rows are returned sorted by id so fingerprinting and later label queries
    always observe one canonical order, regardless of the submitted order.
    """
    require_object(payload, "")
    if "job" not in payload:
        raise ValidationError("/job", "field is required")
    job = require_string(payload["job"], "/job")
    if not is_ascii_alnum(job):
        raise ValidationError("/job", "must be a non-empty ASCII alphanumeric string")

    if "rows" not in payload:
        raise ValidationError("/rows", "field is required")
    rows = payload["rows"]
    if not isinstance(rows, list):
        raise ValidationError("/rows", "expected an array")

    seen = set()
    cleaned = []
    for i, row in enumerate(rows):
        base = "/rows/%d" % i
        require_object(row, base)
        if "id" not in row:
            raise ValidationError(base + "/id", "field is required")
        rid = require_string(row["id"], base + "/id")
        if not is_ascii_alnum(rid):
            raise ValidationError(base + "/id",
                                  "must be a non-empty ASCII alphanumeric string")
        if rid in seen:
            raise ValidationError(base + "/id", "duplicate row id %r" % rid)
        seen.add(rid)
        if "text" not in row:
            raise ValidationError(base + "/text", "field is required")
        text = require_string(row["text"], base + "/text")
        cleaned.append({"id": rid, "text": text})

    cleaned.sort(key=lambda r: r["id"])
    return job, cleaned


def fingerprint(rows):
    """Stable content hash of the canonical (id, text) sequence."""
    return json.dumps(
        [[r["id"], r["text"]] for r in rows],
        separators=(",", ":"),
        ensure_ascii=False,
        sort_keys=True,
    )


# ---------------------------------------------------------------------------
# Small HTTP client used by the worker / acceptance harness
# ---------------------------------------------------------------------------

def http_json(method, url, body=None, timeout=5.0):
    """Call a JSON endpoint.

    Returns ``(status, parsed_body_or_None)``. Network failures (reset,
    refused connection, timeout) raise the underlying URLError so callers
    can distinguish a *disconnect* — which is retryable — from an HTTP
    status response.
    """
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            status = resp.status
    except urllib.error.HTTPError as exc:
        # An HTTP status (even 5xx) is a *response*, not a transport drop.
        raw = exc.read()
        status = exc.code
    except (urllib.error.URLError, http.client.HTTPException,
            ConnectionError, OSError) as exc:
        # RemoteDisconnected / BadStatusLine are HTTPExceptions, not
        # URLErrors; refused connections and timeouts arrive as URLError
        # or OSError.  All of them mean "no usable response": retryable.
        raise TransportError(exc)
    try:
        parsed = json.loads(raw.decode("utf-8")) if raw else None
    except (ValueError, UnicodeDecodeError):
        parsed = None
    return status, parsed
