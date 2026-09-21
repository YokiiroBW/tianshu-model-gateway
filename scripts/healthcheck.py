#!/usr/bin/env python3
"""Container liveness check: one unauthenticated loopback ``GET /health/live`` over TLS.

This is the check the image declares in its ``HEALTHCHECK``. It answers exactly one question --
"is this process still able to answer an HTTP request?" -- and it deliberately does not answer
any other:

* it never touches ``/health/ready``, because readiness needs an independent credential and a
  credential must not be baked into an image;
* it never checks a dependency, because a liveness check that fails on a dependency outage
  makes an orchestrator kill a process that was working;
* it sends no business header, no correlation id and no bearer token, so the probe it performs
  is the same read-only probe an operator would perform by hand;
* it writes nothing: no file, no log line, no state.

TLS is verified. ``http://`` is refused unless ``--allow-http`` is passed explicitly, and the
image never passes it: an unverified loopback check would hide a broken certificate chain.

Only the standard library is used, so the check cannot fail because a dependency is missing.
"""

import argparse
import json
import socket
import ssl
import sys
from http.client import HTTPSConnection, HTTPConnection
from urllib.parse import urlsplit

MAX_BODY_BYTES = 4096
DEFAULT_TIMEOUT = 3.0


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Tianshu gateway container liveness check")
    parser.add_argument(
        "--url",
        default="https://127.0.0.1:8443/health/live",
        help="liveness URL; must be https unless --allow-http is explicit",
    )
    parser.add_argument("--cacert", help="PEM bundle used to verify the server certificate")
    parser.add_argument("--server-hostname", help="name to verify instead of the URL host")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument(
        "--allow-http",
        action="store_true",
        help="explicit opt-out of TLS verification; not used by the image",
    )
    return parser.parse_args(argv)


def build_connection(parts, args):
    host = parts.hostname
    port = parts.port or (443 if parts.scheme == "https" else 80)
    timeout = max(args.timeout, 0.1)
    if parts.scheme == "https":
        context = ssl.create_default_context(cafile=args.cacert)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        return HTTPSConnection(host, port, context=context, timeout=timeout)
    return HTTPConnection(host, port, timeout=timeout)


def check(url, args):
    """Return ``(ok, reason)``. Never raises: the reason is a fixed word, never an exception."""
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        return False, "invalid_url"
    if parts.scheme == "http" and not args.allow_http:
        # An unverified loopback check would hide a broken certificate chain, so plaintext is
        # refused unless the operator asked for it explicitly. The image never asks.
        return False, "plaintext_refused"
    path = parts.path or "/health/live"
    connection = None
    try:
        connection = build_connection(parts, args)
        connection.request("GET", path, headers={"Accept": "application/json"})
        response = connection.getresponse()
        body = response.read(MAX_BODY_BYTES)
        status = response.status
    except (OSError, ValueError, ssl.SSLError, socket.timeout):
        return False, "unreachable"
    except Exception:
        return False, "unreachable"
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
    if status != 200:
        return False, "unexpected_status"
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return False, "invalid_body"
    if not isinstance(document, dict) or document.get("status") != "alive":
        return False, "unexpected_body"
    return True, "alive"


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    ok, reason = check(args.url, args)
    # One bounded line, no URL, no body, no certificate detail and no exception text.
    print(json.dumps({"check": "liveness", "result": reason}))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
