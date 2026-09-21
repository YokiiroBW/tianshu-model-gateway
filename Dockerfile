# TS-103 container base for the Tianshu model gateway.
#
# Scope of this file: a reproducible runtime for the gateway process and nothing else. It
# installs the exact locked dependency set, runs the server as an unprivileged user, keeps the
# secret material out of the image, and gives the orchestrator one read-only liveness check.
#
# It is deliberately NOT a deployment definition. Every piece of deployment input -- the
# settings document, the credentials, the platform origin assertion, the published contract
# packages, the TLS material and the writable log/state directories -- is supplied at run time
# by an explicit mount or environment variable. No default token, URL, database, model config,
# contract directory or diagnostics token exists anywhere in this image.
#
# NOT VERIFIED: this image has never been built or run. No container runtime was available in
# the task environment, so "the Dockerfile is written" is the whole claim. See
# docs/deployment.md and docs/handoffs/TS-103.md for the exact unverified items.

# --- stage 1: resolve and install the locked dependency set -----------------------------
FROM python:3.12.12-slim-bookworm AS builder

# The uv version is pinned so the resolver that produced uv.lock is the resolver that consumes
# it. `--locked` then makes the build fail rather than silently updating the lock.
ARG UV_VERSION=0.9.5
ENV UV_PROJECT_ENVIRONMENT=/opt/tianshu-venv \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_NO_CACHE=1

RUN pip install --no-cache-dir "uv==${UV_VERSION}"

WORKDIR /build
# Only what the resolution needs. The project is installed from source with --no-editable, so
# the runtime stage carries an installed package rather than a source tree to import from.
COPY pyproject.toml uv.lock ./
COPY src/ ./src/
RUN python -m uv sync --locked --no-dev --no-editable --python /usr/local/bin/python3.12

# --- stage 2: the runtime image ---------------------------------------------------------
FROM python:3.12.12-slim-bookworm AS runtime

# No build toolchain, no shell utilities for debugging, no package manager cache: the runtime
# stage is the interpreter plus the installed package plus the liveness check.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/opt/tianshu-venv/bin:$PATH

# One unprivileged, password-less account owns the process. It owns nothing in the image:
# every writable path below is an explicit mount the operator provides.
RUN groupadd --gid 10001 tianshu \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin tianshu

COPY --from=builder /opt/tianshu-venv /opt/tianshu-venv
COPY scripts/healthcheck.py /opt/tianshu/healthcheck.py

# Convention, not configuration: these are the paths docs/deployment.md tells the operator to
# mount. Creating them here means the image is *capable* of a read-only root filesystem, and
# nothing in the image writes anywhere else.
#   /etc/tianshu/settings.json              deployment document (read-only mount)
#   /etc/tianshu/contracts/diagnostics/v1   published diagnostics package (read-only mount)
#   /etc/tianshu/tls/                       server certificate, key and CA (read-only mount)
#   /var/log/tianshu                        runtime event segments (writable mount)
#   /var/lib/tianshu                        private ledger (writable mount)
RUN mkdir -p /etc/tianshu /var/log/tianshu /var/lib/tianshu \
    && chown 10001:10001 /var/log/tianshu /var/lib/tianshu \
    && chmod 0750 /var/log/tianshu /var/lib/tianshu

USER 10001:10001
EXPOSE 8443

# Liveness only: an unauthenticated loopback request over TLS against the certificate this
# container serves. The readiness surface is never probed by the image, because readiness needs
# an independent credential and a credential must not be baked into an image.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["python", "/opt/tianshu/healthcheck.py", "--url", "https://127.0.0.1:8443/health/live", "--cacert", "/etc/tianshu/tls/ca.pem"]

# Graceful stop: aiohttp drains in-flight requests for a bounded time, then the observation
# sink flushes its bounded queue. SIGKILL is never the first signal.
STOPSIGNAL SIGTERM

# The server refuses to start without --settings and without TLS material; there is no
# implicit configuration to fall back on and no HTTP mode in this image.
ENTRYPOINT ["python", "-m", "tianshu_gateway"]
CMD ["--settings", "/etc/tianshu/settings.json", "--host", "0.0.0.0", "--port", "8443", "--tls-cert", "/etc/tianshu/tls/server.crt", "--tls-key", "/etc/tianshu/tls/server.key"]
