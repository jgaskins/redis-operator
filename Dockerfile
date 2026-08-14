# syntax=docker/dockerfile:1.7

FROM rust:1.95-slim-bookworm AS builder
WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked && \
    cp target/release/redis-operator /usr/local/bin/redis-operator

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /usr/local/bin/redis-operator /usr/local/bin/redis-operator
ENTRYPOINT ["/usr/local/bin/redis-operator"]
CMD ["run"]
