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

# Use the exact wal-g binary that cnpg-plugin-wal-g ships — same source
# commit (94bf839) as the v3.0.5 GitHub release tarball, but built fresh
# in their pipeline with current libsodium / Go toolchain. Binaries from
# the GH release tarball (Jan 2025) and the cnpg-plugin build (Oct 2025)
# diverged enough that decryption differs in subtle ways even though
# both report the same version. Pinning to the producer's image
# eliminates that drift.
#
# Note: this slightly rebrands the "always latest pinned" strategy from
# ADR-023 — we track the cnpg-plugin-wal-g release rather than wal-g
# upstream releases. Practically, since cnpg-plugin-wal-g IS what writes
# the data we validate, matching it is the right choice. The forward-
# compat canary still fires when the plugin bumps wal-g and the new
# binary can't read repos written by older plugin versions.
FROM ghcr.io/wal-g/cnpg-plugin-wal-g:0.2.2 AS walg-source

FROM ubuntu:22.04
ARG WALG_VERSION
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libsodium23 && \
    rm -rf /var/lib/apt/lists/* && \
    # wal-g calls user.Lookup() at startup and crashes if the running UID
    # has no /etc/passwd entry. arete spawns validator pods with
    # runAsUser=65532 (PSS-restricted compliance), so we provision a
    # matching nonroot account here. Group/home-dir match common K8s
    # convention (distroless nonroot user).
    groupadd --gid 65532 nonroot && \
    useradd --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin nonroot
COPY --from=walg-source /usr/local/bin/wal-g /usr/local/bin/wal-g

# Embed version metadata so `docker inspect` (and the controller, if it ever
# wants to verify) can confirm what's inside without running the binary.
LABEL org.opencontainers.image.source="https://github.com/TobiasHofmaenner/arete"
LABEL org.opencontainers.image.title="arete-validator-walg"
LABEL org.opencontainers.image.description="wal-g binary bundled for use by the arete operator's E2-E4 validators"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL net.thfops.arete.walg-version="${WALG_VERSION}"

ENTRYPOINT ["/usr/local/bin/wal-g"]
