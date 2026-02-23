FROM rust:1.88-alpine

RUN apk update && apk add --no-cache \
    musl-dev \
    git \
    bash \
    curl \
    protobuf-dev \
    nodejs \
    npm \
    make net-tools tcpdump binutils netcat-openbsd htop \
 && rustup component add clippy rustfmt

# Dynamic linking for cdylib plugins
ENV RUSTFLAGS="-C target-feature=-crt-static"

# Install cargo-watch for dev workflow
RUN cargo install cargo-watch

WORKDIR /workspace
