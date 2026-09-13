"""Deployment input is explicit; no default tokens, URLs, databases or model config."""

import argparse
import ipaddress
import logging
import ssl
from pathlib import Path
from urllib.parse import urlsplit

from aiohttp import web

from .config import ClientGrant
from .contracts import loads
from .server import Settings, create_app


def main():
    parser = argparse.ArgumentParser(description="TS-041 native model gateway")
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
        data = loads(Path(args.settings).read_bytes())
        data["clients"] = [ClientGrant(**client) for client in data["clients"]]
        settings = Settings(**data)
        if args.local_test:
            if not all(
                ipaddress.ip_address(ip).is_loopback
                for target in settings.targets
                for ip in target["addresses"]
            ):
                raise ValueError()
        elif urlsplit(settings.platform_base_url).scheme != "https":
            raise ValueError()
        app = create_app(settings)
    except Exception:
        parser.error("invalid or unavailable deployment settings/contract/TLS material")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    # Cancellation releases the upstream connection even when it is currently idle.
    web.run_app(
        app,
        host=args.host,
        port=args.port,
        ssl_context=tls,
        handler_cancellation=True,
        access_log=None,
        print=None,
    )


if __name__ == "__main__":
    main()
