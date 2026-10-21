# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Build stage: native platform, static musl binary.
#
# Build with the TLS feature by default:
#   docker build -t ha-ch-ingestor .
#
# Plaintext-only (smaller, faster to build):
#   docker build --build-arg FEATURES=none -t ha-ch-ingestor .
#
# The same Dockerfile builds the fake HA server used by docker-compose:
#   docker build --build-arg FEATURES=mock --build-arg BIN=mock_ha .
# ---------------------------------------------------------------------------
FROM rust:1.99.1-alpine AS builder

ARG FEATURES=tls
ARG BIN=ha-ch-ingestor

# aws-lc-rs (rustls' default provider) needs a C toolchain and cmake. These
# stay in the builder; the runtime image has no libc at all.
RUN apk add --no-cache build-base cmake perl linux-headers ca-certificates

WORKDIR /src

# Dependency layer: build a stub binary so the expensive dependency graph is
# cached even when only application sources change.
COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    set -eux; \
    case "$FEATURES" in \
        tls) FLAGS="--features tls" ;; \
        none|"") FLAGS="" ;; \
        *) FLAGS="--features $FEATURES" ;; \
    esac; \
    mkdir -p src/bin; \
    echo 'fn main() {}' > src/main.rs; \
    echo 'fn main() {}' > src/bin/mock_ha.rs; \
    cargo build --release --locked --no-default-features $FLAGS --bin ha-ch-ingestor; \
    rm -rf src target/release/ha-ch-ingestor target/release/deps/ha_ch_ingestor*

COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    set -eux; \
    case "$FEATURES" in \
        tls) FLAGS="--features tls" ;; \
        none|"") FLAGS="" ;; \
        *) FLAGS="--features $FEATURES" ;; \
    esac; \
    cargo build --release --locked --no-default-features $FLAGS --bin "$BIN"; \
    cp "target/release/$BIN" /out-binary

# ---------------------------------------------------------------------------
# Runtime stage: no shell, no libc, no package manager.
# ---------------------------------------------------------------------------
FROM scratch

# Only needed for wss:// and https://; harmless in the plaintext build.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out-binary /ingestor

USER 65532:65532
ENTRYPOINT ["/ingestor"]
