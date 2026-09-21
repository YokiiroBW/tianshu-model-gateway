"""Deployment input is explicit; no default tokens, URLs, databases or model config.

This module is the only place that assembles the process: it reads the deployment document,
builds the independent observation adapter and hands both to :func:`create_app`. The server
module never constructs a sink, a directory or a segment of its own, and the adapter never
reaches back into a business rule.
"""

import argparse
import ipaddress
import json
import logging
import ssl
import sys
from pathlib import Path
from urllib.parse import urlsplit

from aiohttp import web

from .observability import Observability, load_contract, sink
from .server import create_app, load_settings

USAGE_REPORT = "usage-report"
LOG_RECOVERY_CHECK = "log-recovery-check"
SUBCOMMANDS = (USAGE_REPORT, LOG_RECOVERY_CHECK)
LOG = logging.getLogger("tianshu.gateway")


def build_observability(settings):
    """Assemble the adapter from deployment input alone; ``None`` keeps the old behaviour.

    A deployment that configures no observation block gets no adapter at all, which is the
    pre-TS-103 behaviour: no runtime event is emitted and the readiness surface reports every
    check as unverified instead of inventing a green light.
    """
    if settings.observability is None:
        return None
    contract = None
    if settings.diagnostics_contract_directory is not None:
        # Read-only load with manifest hash verification, before the server binds.
        contract = load_contract(settings.diagnostics_contract_directory)
    return Observability(settings.observability, contract=contract)


def log_recovery_check(settings):
    """Explicit maintenance action: replace a *check* with a real, successful write.

    A readiness probe is read-only and can never prove recovery, so recovery is proved here,
    by an operator-invoked command that writes one registered record and reports whether that
    write and its ``fsync`` actually succeeded. It touches no business state, no ledger row and
    no upstream; the result is one line on stdout and a non-zero exit when the sink is still
    unusable.
    """
    observability = build_observability(settings)
    if observability is None or observability.settings.log_directory is None:
        print(
            json.dumps(
                {"check": "log-recovery-check", "result": "not_configured", "state": "unavailable"}
            )
        )
        return 2
    observability.open()
    ok = observability.log.probe_write()
    print(
        json.dumps(
            {
                "check": "log-recovery-check",
                "result": "recovered" if ok else "failed",
                "state": observability.state,
                "reason": observability.reason,
            }
        )
    )
    observability.close_sync()
    return 0 if ok else 1


def main():
    # A fixed leading word selects a local utility; everything else keeps the existing server
    # interface byte for byte.
    if sys.argv[1:2] == [USAGE_REPORT]:
        from .usage_report import main as report_main

        sys.exit(report_main(sys.argv[2:]))
    if sys.argv[1:2] == [LOG_RECOVERY_CHECK]:
        parser = argparse.ArgumentParser(prog="python -m tianshu_gateway log-recovery-check")
        parser.add_argument("--settings", required=True, help="deployment JSON")
        args = parser.parse_args(sys.argv[2:])
        try:
            settings = load_settings(Path(args.settings).read_bytes())
        except Exception:
            print(json.dumps({"check": "log-recovery-check", "result": "invalid_settings"}))
            sys.exit(1)
        sys.exit(log_recovery_check(settings))
    parser = argparse.ArgumentParser(
        description="TS-041 native model gateway (subcommands: usage-report, log-recovery-check)"
    )
    parser.add_argument(
        "--settings", required=True, help="deployment JSON (references, not secrets)"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--tls-cert")
    parser.add_argument("--tls-key")
    parser.add_argument(
        "--local-test", action="store_true", help="explicit loopback-only HTTP fixture mode"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        tls = None
        if args.local_test:
            if not ipaddress.ip_address(args.host).is_loopback:
                raise ValueError()
        else:
            if not args.tls_cert or not args.tls_key:
                parser.error(
                    "TLS certificate and key required; HTTP is only for explicit loopback tests"
                )
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.minimum_version = ssl.TLSVersion.TLSv1_2
            tls.load_cert_chain(args.tls_cert, args.tls_key)
        settings = load_settings(Path(args.settings).read_bytes())
        if args.local_test:
            if not all(
                ipaddress.ip_address(ip).is_loopback
                for target in settings.targets
                for ip in target["addresses"]
            ):
                raise ValueError()
        elif urlsplit(settings.platform_base_url).scheme != "https":
            raise ValueError()
        observability = build_observability(settings)
        app = create_app(settings, observability)
    except Exception:
        # Nothing is serving and no sink may exist yet, so the failure is reported on the
        # process channel. No event is invented for a runtime that never assembled.
        LOG.critical("runtime_startup_failed")
        parser.error("invalid or unavailable deployment settings/contract/TLS material")
    # Cancellation releases the upstream connection even when it is currently idle. The
    # shutdown timeout is the bounded drain of an in-flight request on SIGTERM/SIGINT.
    web.run_app(
        app,
        host=args.host,
        port=args.port,
        ssl_context=tls,
        handler_cancellation=True,
        shutdown_timeout=sink.SHUTDOWN_TIMEOUT_SECONDS,
        access_log=None,
        print=None,
    )


if __name__ == "__main__":
    main()
