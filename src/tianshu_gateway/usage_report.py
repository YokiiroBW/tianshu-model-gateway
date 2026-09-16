"""Local operator CLI for the read-only usage and latency report.

This entry point never listens, never talks to the platform or a provider and never writes:
it opens the private ledger read-only and prints one bounded report. The requested identity
must still exist in the deployment document, be currently authorized, and still resolve its
credential, so a revoked or expired caller cannot read its history from the command line
either. There is no unscoped "show everything" mode.
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .config import EnvSecrets
from .contracts import Rejected
from .diagnostics import NATIVE_IDENTITY_FIELDS, PRIVATE_TABLES
from .native import authorize
from .server import load_settings
from .usage import build_report, parse_selection

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_REFUSED = 3
NATIVE_SELECTORS = ("principal_id", "caller_service", "credential_namespace")


class Refused(Exception):
    """The requested identity is not a usable registration of this deployment."""


class LedgerUnavailable(Exception):
    """The private ledger is missing, unreadable or not migrated yet."""


def build_parser():
    parser = argparse.ArgumentParser(
        prog="tianshu-model-gateway usage-report",
        description="Read one bounded usage/latency report from the local private ledger.",
    )
    parser.add_argument(
        "--settings", required=True, help="deployment JSON (references, not secrets)"
    )
    parser.add_argument("--database", help="override diagnostics_path for this read")
    parser.add_argument("--view", choices=("summary", "attempts"), default="summary")
    parser.add_argument("--since", help="UTC start instant, ISO-8601 ending in Z")
    parser.add_argument("--until", help="UTC end instant, ISO-8601 ending in Z")
    parser.add_argument("--limit", help="row cap for this window (1..5000)")
    parser.add_argument("--offset", help="row offset inside the window (0..100000)")
    parser.add_argument("--service", help="chat caller service registered in this deployment")
    parser.add_argument("--principal-id", help="native principal registered in this deployment")
    parser.add_argument("--caller-service", help="native caller service of that registration")
    parser.add_argument("--credential-namespace", help="native credential namespace")
    parser.add_argument("--compact", action="store_true", help="print single-line JSON")
    return parser


def live_credential(settings, reference):
    """A registration whose credential no longer resolves is not a usable identity."""
    try:
        EnvSecrets(settings.secret_references).resolve(reference)
    except Rejected:
        raise Refused("the registered credential is not currently resolvable") from None


def select_scope(settings, arguments):
    """Exactly one key space, chosen by an explicit registration of this deployment."""
    native = tuple(getattr(arguments, name) for name in NATIVE_SELECTORS)
    if arguments.service is not None and any(native):
        raise Refused("select either --service or the native identity, not both")
    if arguments.service is not None:
        grant = next((c for c in settings.clients if c.service == arguments.service), None)
        if grant is None:
            raise Refused("no chat registration matches --service")
        live_credential(settings, grant.credential_ref)
        return "chat", {"service": grant.service}
    if not all(native):
        raise Refused("native reads need --principal-id, --caller-service and --namespace")
    grant = next(
        (
            g
            for g in settings.native_clients
            if g.principal_id == arguments.principal_id
            and g.service == arguments.caller_service
            and g.credential_namespace == arguments.credential_namespace
        ),
        None,
    )
    if grant is None:
        raise Refused("no native registration matches the requested identity")
    try:
        authorize(grant)
    except Rejected:
        raise Refused("the native registration is revoked, expired or unauthorized") from None
    live_credential(settings, grant.credential_ref)
    return "native", dict(zip(NATIVE_IDENTITY_FIELDS, grant.identity(), strict=True))


def open_ledger(path):
    """Read-only handle; the migration itself belongs to the running service."""
    try:
        connection = sqlite3.connect(str(Path(path).resolve().as_uri()) + "?mode=ro", uri=True)
    except sqlite3.Error:
        raise LedgerUnavailable("the private ledger could not be opened read-only") from None
    try:
        names = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    except sqlite3.Error:
        connection.close()
        raise LedgerUnavailable("the private ledger could not be read") from None
    if not set(PRIVATE_TABLES) <= names:
        connection.close()
        raise LedgerUnavailable(
            "the ledger has no metric tables yet; start the gateway once to migrate it"
        )
    return connection


def parameters(arguments):
    values = {"view": arguments.view}
    for name in ("since", "until", "limit", "offset"):
        value = getattr(arguments, name)
        if value is not None:
            values[name] = value
    return values


def main(argv):
    """Both entries pass an explicit argument list; nothing is read from a global."""
    arguments = build_parser().parse_args(list(argv))
    try:
        settings = load_settings(Path(arguments.settings).read_bytes())
        settings.validate()
    except Exception:
        print("usage-report: invalid deployment settings document", file=sys.stderr)
        return EXIT_USAGE
    try:
        key_space, identity = select_scope(settings, arguments)
    except Refused as exc:
        print(f"usage-report: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    values = parameters(arguments)
    try:
        parse_selection(values)
    except Rejected:
        print("usage-report: invalid report parameters", file=sys.stderr)
        return EXIT_USAGE
    connection = None
    try:
        connection = open_ledger(arguments.database or settings.diagnostics_path)
        report = build_report(connection, key_space, identity, values)
    except LedgerUnavailable as exc:
        print(f"usage-report: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except (Rejected, sqlite3.Error, ValueError):
        print("usage-report: the report could not be built", file=sys.stderr)
        return EXIT_USAGE
    finally:
        if connection is not None:
            connection.close()
    indent = None if arguments.compact else 2
    print(json.dumps(report, ensure_ascii=False, indent=indent))
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
