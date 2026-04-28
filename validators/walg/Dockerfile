# arete-validator-walg
#
# Bundles wal-g (Postgres flavor) for use as a Job-spawned validator by the
# arete operator. Per ADR-023's "always-latest pinned per arete release"
# strategy, the WALG_VERSION is bumped here on each arete release; customers
# never specify a version. wal-g maintains backward compat across versions,
# and using the latest pinned binary doubles as a forward-compat canary.
#
# Build args:
#   WALG_VERSION — wal-g release tag without the leading 'v' (e.g. 3.0.5)
#
# Runtime contract: the arete controller spawns this image as a Job with
# `command: ["wal-g", ...]` and credentials/config injected as env vars
# (AWS_*, WALG_S3_PREFIX, optional WALG_LIBSODIUM_KEY for encrypted repos).

ARG WALG_VERSION=3.0.5

FROM ubuntu:22.04 AS download
ARG WALG_VERSION
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    curl -fsSL -o /tmp/wal-g.tar.gz \
        "https://github.com/wal-g/wal-g/releases/download/v${WALG_VERSION}/wal-g-pg-ubuntu-22.04-amd64.tar.gz" && \
    tar -xzf /tmp/wal-g.tar.gz -C /tmp && \
    mv /tmp/wal-g-pg-ubuntu-22.04-amd64 /tmp/wal-g && \
    chmod +x /tmp/wal-g

FROM ubuntu:22.04
ARG WALG_VERSION
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libsodium23 && \
    rm -rf /var/lib/apt/lists/*
COPY --from=download /tmp/wal-g /usr/local/bin/wal-g

# Embed version metadata so `docker inspect` (and the controller, if it ever
# wants to verify) can confirm what's inside without running the binary.
LABEL org.opencontainers.image.source="https://github.com/TobiasHofmaenner/arete"
LABEL org.opencontainers.image.title="arete-validator-walg"
LABEL org.opencontainers.image.description="wal-g binary bundled for use by the arete operator's E2-E4 validators"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL net.thfops.arete.walg-version="${WALG_VERSION}"

ENTRYPOINT ["/usr/local/bin/wal-g"]
