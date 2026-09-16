"""Bounded, read-only usage and latency report over this product's private metric rows.

The whole report is produced by SQL over one explicit selection: a time window plus a row
cap. Counts, sums and nearest-rank percentiles are computed by the database over exactly
those rows, so a report never loads a whole table into memory, never scans message bodies
and never addresses another subject's rows. Each key space keeps its own tables and its own
identity columns, and the two spaces are never mixed in one selection.

Only normalized ``input_tokens``/``output_tokens`` are aggregated. The vendor-native usage
structure stays in the receipt, is never summed, and a value that was not reported stays
``null`` with an explicit missing count instead of becoming zero.
"""

import math
from datetime import datetime, timedelta, timezone

from .contracts import Rejected
from .diagnostics import NATIVE_IDENTITY_FIELDS

SCHEMA_VERSION = 1
ALLOWED_PARAMETERS = ("view", "since", "until", "limit", "offset")
VIEWS = ("summary", "attempts")
DEFAULT_LIMIT = 500
MAX_LIMIT = 5000
MAX_OFFSET = 100_000
DEFAULT_WINDOW = timedelta(hours=24)
MAX_WINDOW = timedelta(days=366)

# table, receipt table, identity columns
KEY_SPACES = {
    "chat": ("request_metrics", "requests", ("service",)),
    "native": ("native_request_metrics", "native_requests", NATIVE_IDENTITY_FIELDS),
}
METRIC_COLUMNS = (
    "request_id",
    "completed_at_ms",
    "first_upstream_byte_ms",
    "first_event_ms",
    "first_output_ms",
    "usage_source",
    "input_tokens",
    "output_tokens",
    "usage_complete",
)
# Reported name -> column. ``elapsed_ms`` is read from the existing receipt row.
LATENCY_COLUMNS = (
    ("request_total_ms", "elapsed_ms"),
    ("first_upstream_byte_ms", "first_upstream_byte_ms"),
    ("first_event_ms", "first_event_ms"),
    ("first_output_ms", "first_output_ms"),
)
SUCCEEDED_REASONS = frozenset({"completed", "response_completed"})
FAILED_REASONS = frozenset({"response_failed"})
CANCELLED_REASONS = frozenset({"cancelled_unknown"})


def parse_instant(value):
    """A UTC instant written the way the receipts write one: ISO-8601 ending in Z."""
    if not isinstance(value, str) or not value.endswith("Z") or len(value) < 20:
        raise Rejected()
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise Rejected() from None
    if moment.tzinfo is None or moment.utcoffset() != timedelta(0):
        raise Rejected()
    return int(moment.timestamp() * 1000)


def instant_text(milliseconds):
    return (
        datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat().replace("+00:00", "Z")
    )


def parse_integer(value, low, high):
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
        raise Rejected()
    number = int(value)
    if str(number) != value or not low <= number <= high:
        raise Rejected()
    return number


def parse_selection(values, now=None):
    """Validate one report selector from single-valued string parameters."""
    if not isinstance(values, dict) or set(values) - set(ALLOWED_PARAMETERS):
        raise Rejected()
    view = values.get("view", "summary")
    if view not in VIEWS:
        raise Rejected()
    now = now or datetime.now(timezone.utc)
    until = parse_instant(values["until"]) if "until" in values else int(now.timestamp() * 1000)
    since = (
        parse_instant(values["since"])
        if "since" in values
        else until - int(DEFAULT_WINDOW.total_seconds() * 1000)
    )
    limit = parse_integer(values["limit"], 1, MAX_LIMIT) if "limit" in values else DEFAULT_LIMIT
    offset = parse_integer(values["offset"], 0, MAX_OFFSET) if "offset" in values else 0
    if not since < until or until - since > int(MAX_WINDOW.total_seconds() * 1000):
        raise Rejected()
    return view, since, until, limit, offset


def outcome_bucket(reason, upstream_status):
    """Success/failure/cancellation/unknown, from the recorded reason and HTTP status.

    ``upstream_http_error`` is the only reason whose verdict depends on the status: a
    provider 4xx is a failed attempt and anything else stays unknown. An unrecognized
    reason is reported as unknown and still listed under ``reasons``.
    """
    if reason in CANCELLED_REASONS:
        return "cancelled"
    if reason in SUCCEEDED_REASONS:
        return "succeeded"
    if reason in FAILED_REASONS:
        return "failed"
    if reason == "upstream_http_error" and type(upstream_status) is int:
        if 400 <= upstream_status < 500:
            return "failed"
    return "unknown"


class Selection:
    """One time window, row cap and identity; every query runs over exactly this set."""

    def __init__(self, key_space, identity, since, until, limit, offset):
        try:
            self.table, self.receipts, self.columns = KEY_SPACES[key_space]
        except KeyError:
            raise ValueError("unknown key space") from None
        if not isinstance(identity, dict) or set(identity) != set(self.columns):
            raise ValueError("identity must match the key space columns")
        values = []
        for name in self.columns:
            value = identity[name]
            if not isinstance(value, str) or not value:
                raise ValueError("identity values must be non-empty strings")
            values.append(value)
        self.key_space, self.identity = key_space, tuple(values)
        self.since, self.until, self.limit, self.offset = since, until, limit, offset

    @property
    def parameters(self):
        return (*self.identity, self.since, self.until, self.limit, self.offset)

    def _filter(self, alias):
        return " AND ".join(f"{alias}.{name}=?" for name in self.columns)

    @property
    def window(self):
        """The bounded selection: the newest ``limit`` attempts of the window, newest first."""
        columns = ", ".join(f"m.{name}" for name in METRIC_COLUMNS)
        # The receipt primary key is the identity columns plus the request id; joining on
        # the identity alone would multiply one attempt by every sibling of its subject.
        join = " AND ".join(f"r.{name}=m.{name}" for name in (*self.columns, "request_id"))
        return (
            f"(SELECT {columns}, r.elapsed_ms, r.reason, r.upstream_status FROM {self.table} m "
            f"JOIN {self.receipts} r ON {join} "
            f"WHERE {self._filter('m')} AND m.completed_at_ms>=? AND m.completed_at_ms<? "
            "ORDER BY m.completed_at_ms DESC, m.request_id DESC LIMIT ? OFFSET ?)"
        )

    def scalar(self, connection, sql, parameters):
        return connection.execute(sql, parameters).fetchone()

    def scanned(self, connection):
        return self.scalar(connection, f"SELECT COUNT(*) FROM {self.window}", self.parameters)[0]

    def matching(self, connection):
        return self.scalar(
            connection,
            f"SELECT COUNT(*) FROM {self.table} m "
            f"WHERE {self._filter('m')} AND m.completed_at_ms>=? AND m.completed_at_ms<?",
            (*self.identity, self.since, self.until),
        )[0]

    def unmetered(self, connection):
        """Attempts that predate this metric table or never reached a terminal record."""
        return self.scalar(
            connection,
            f"SELECT (SELECT COUNT(*) FROM {self.receipts} r WHERE {self._filter('r')})"
            f" - (SELECT COUNT(*) FROM {self.table} m WHERE {self._filter('m')})",
            (*self.identity, *self.identity),
        )[0]

    def latency(self, connection):
        fields = ["COUNT(*)"]
        for _, column in LATENCY_COLUMNS:
            fields.extend(
                (
                    f"SUM({column} IS NOT NULL)",
                    f"MIN({column})",
                    f"MAX({column})",
                    f"AVG({column})",
                )
            )
        row = self.scalar(
            connection, f"SELECT {', '.join(fields)} FROM {self.window}", self.parameters
        )
        result = {}
        for position, (name, column) in enumerate(LATENCY_COLUMNS):
            observed, low, high, average = row[1 + position * 4 : 5 + position * 4]
            observed = observed or 0
            result[name] = {
                "observed": observed,
                "missing": row[0] - observed,
                "min": low,
                "max": high,
                "avg": None if average is None else round(average, 3),
                "p50": self.percentile(connection, column, 0.50, observed),
                "p95": self.percentile(connection, column, 0.95, observed),
            }
        return result

    def percentile(self, connection, column, fraction, observed):
        """Exact nearest-rank percentile; a bounded sort inside the selection, not a scan."""
        if observed <= 0:
            return None
        rank = max(1, min(observed, math.ceil(fraction * observed)))
        row = self.scalar(
            connection,
            f"SELECT {column} FROM {self.window} WHERE {column} IS NOT NULL "
            f"ORDER BY {column} LIMIT 1 OFFSET ?",
            (*self.parameters, rank - 1),
        )
        return row[0] if row else None

    def counts(self, connection):
        # Only rows whose reason maps to a definite verdict are counted directly; the
        # remainder is computed by subtraction so the four buckets always sum to the
        # scanned total even when an attempt carries no upstream status at all.
        fields = (
            ("succeeded", "reason IN ('completed','response_completed')"),
            (
                "failed",
                "reason='response_failed' OR (reason='upstream_http_error' "
                "AND upstream_status>=400 AND upstream_status<500)",
            ),
            ("cancelled", "reason='cancelled_unknown'"),
        )
        select = ", ".join(f"SUM({sql}) AS {name}" for name, sql in fields)
        row = self.scalar(
            connection, f"SELECT COUNT(*), {select} FROM {self.window}", self.parameters
        )
        total = row[0]
        counts = {"total": total}
        for position, (name, _) in enumerate(fields):
            counts[name] = row[1 + position] or 0
        counts["unknown"] = total - counts["succeeded"] - counts["failed"] - counts["cancelled"]
        reasons = [
            {"reason": reason, "upstream_status": status, "attempts": attempts}
            for reason, status, attempts in connection.execute(
                f"SELECT reason, upstream_status, COUNT(*) FROM {self.window} "
                "GROUP BY reason, upstream_status ORDER BY COUNT(*) DESC, reason",
                self.parameters,
            )
        ]
        return counts, reasons

    def usage(self, connection):
        select = (
            "COUNT(*), COUNT(input_tokens), COALESCE(SUM(input_tokens),0), "
            "COUNT(output_tokens), COALESCE(SUM(output_tokens),0), "
            "COALESCE(SUM(usage_complete),0), "
            "COALESCE(SUM(input_tokens IS NULL AND output_tokens IS NULL),0)"
        )
        sources = {}
        for source, attempts, fed, fed_sum, oed, oed_sum, complete, absent in connection.execute(
            f"SELECT usage_source, {select} FROM {self.window} GROUP BY usage_source "
            "ORDER BY usage_source",
            self.parameters,
        ):
            sources[source] = {
                "attempts": attempts,
                "complete": complete,
                "input_tokens": {"sum": fed_sum, "reported": fed, "missing": attempts - fed},
                "output_tokens": {"sum": oed_sum, "reported": oed, "missing": attempts - oed},
                "no_usage_reported": absent,
            }
        for source in (
            "upstream_json_usage",
            "upstream_stream_usage",
            "not_reported",
            "unobserved",
        ):
            sources.setdefault(
                source,
                {
                    "attempts": 0,
                    "complete": 0,
                    "input_tokens": {"sum": 0, "reported": 0, "missing": 0},
                    "output_tokens": {"sum": 0, "reported": 0, "missing": 0},
                    "no_usage_reported": 0,
                },
            )
        row = self.scalar(connection, f"SELECT {select} FROM {self.window}", self.parameters)
        total, fed, fed_sum, oed, oed_sum, complete, absent = row
        return {
            "attempts": total,
            "complete": complete,
            "partial": total - complete - absent,
            "missing": absent,
            "normalized": {
                "input_tokens": {
                    "sum": fed_sum,
                    "reported": fed,
                    "missing": total - fed,
                },
                "output_tokens": {
                    "sum": oed_sum,
                    "reported": oed,
                    "missing": total - oed,
                },
            },
            "by_source": sources,
            "vendor_fields_aggregated": False,
        }

    def attempts(self, connection):
        rows = connection.execute(
            "SELECT request_id, completed_at_ms, elapsed_ms, reason, upstream_status, "
            "first_upstream_byte_ms, first_event_ms, first_output_ms, usage_source, "
            f"input_tokens, output_tokens, usage_complete FROM {self.window}",
            self.parameters,
        )
        return [
            {
                "request_id": row[0],
                "completed_at": instant_text(row[1]),
                "request_total_ms": row[2],
                "reason": row[3],
                "upstream_status": row[4],
                "outcome": outcome_bucket(row[3], row[4]),
                "first_upstream_byte_ms": row[5],
                "first_event_ms": row[6],
                "first_output_ms": row[7],
                "usage_source": row[8],
                "input_tokens": row[9],
                "output_tokens": row[10],
                "usage_complete": bool(row[11]),
            }
            for row in rows
        ]


def build_report(connection, key_space, identity, values):
    """One bounded report for one authenticated identity in one key space."""
    view, since, until, limit, offset = parse_selection(values)
    selection = Selection(key_space, identity, since, until, limit, offset)
    scanned = selection.scanned(connection)
    matching = selection.matching(connection)
    counts, reasons = selection.counts(connection)
    document = {
        "schema_version": SCHEMA_VERSION,
        "key_space": key_space,
        "identity": dict(zip(selection.columns, selection.identity, strict=True)),
        "window": {
            "since": instant_text(since),
            "until": instant_text(until),
            "since_ms": since,
            "until_ms": until,
            "limit": limit,
            "offset": offset,
        },
        "coverage": {
            "scanned": scanned,
            "matching": matching,
            "truncated": matching > scanned,
            "unmetered_total": selection.unmetered(connection),
        },
        "counts": counts,
        "reasons": reasons,
        "latency_ms": selection.latency(connection),
        "usage": selection.usage(connection),
        "notes": [
            "Timings are monotonic milliseconds since the attempt started; null means the "
            "value was not observed and is never reported as zero.",
            "request_total_ms covers reading, upstream transfer and downstream backpressure; "
            "it is not model generation time.",
            "Only normalized input_tokens/output_tokens are aggregated; the vendor-native "
            "usage structure stays in the receipt.",
        ],
    }
    if view == "attempts":
        document["attempts"] = selection.attempts(connection)
    return document
