# MiniVectorDB judge image.
#
# Runtime contract (see STDIO_PROTOCOL.md): the process speaks JSON Lines on
# stdin/stdout, prints exactly {"type":"ready"} before the first request and
# stores all persistent state under /data. The default command therefore runs
# the binary with no arguments: no shell wrapper, no SQL prompt, no listener.

ARG RUST_IMAGE=rust:1.90-slim-bookworm
FROM ${RUST_IMAGE} AS builder

ENV CARGO_INCREMENTAL=0 \
    CARGO_TERM_COLOR=never

WORKDIR /build

# The crate has no third-party dependencies, so the build is fully offline and
# reproducible once the base image is present.
COPY minivectordb/Cargo.toml minivectordb/Cargo.lock ./
COPY minivectordb/src ./src

# Strip symbols without touching the shared Cargo.toml.
ENV RUSTFLAGS="-C strip=symbols"
# --locked: the committed Cargo.lock is authoritative, so the build is
# reproducible and cannot silently pick different (or new) dependency versions.
RUN cargo build --release --locked --bin minivectordb

# Ownership of this directory is copied into a fresh named volume mounted at
# /data, which is what lets the unprivileged uid below write there.
RUN install -d -o 10001 -g 10001 /build/data

ARG RUNTIME_IMAGE=debian:bookworm-slim
FROM ${RUNTIME_IMAGE} AS runtime

# Dedicated unprivileged account. The judge always provides /data (named
# volume or bind mount); the image only guarantees it exists and is writable.
RUN set -eux; \
    groupadd --gid 10001 vectordb; \
    useradd --uid 10001 --gid vectordb --no-create-home \
            --home-dir /nonexistent --shell /usr/sbin/nologin vectordb

COPY --from=builder /build/target/release/minivectordb /usr/local/bin/minivectordb
COPY --from=builder --chown=10001:10001 /build/data /data

ENV VECTORDB_DATA_DIR=/data
WORKDIR /
USER 10001:10001

# Empty default argv: main() defaults the data directory to /data.
ENTRYPOINT ["/usr/local/bin/minivectordb"]
