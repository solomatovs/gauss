FROM rust:1.88-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    bash \
    curl \
    protobuf-compiler \
    nodejs \
    npm \
    locales \
    musl-tools \
    make net-tools tcpdump binutils netcat-openbsd htop \
 && sed -i 's/# ru_RU.UTF-8 UTF-8/ru_RU.UTF-8 UTF-8/' /etc/locale.gen \
 && locale-gen \
 && rm -rf /var/lib/apt/lists/* \
 && rustup component add clippy rustfmt \
 && rustup target add x86_64-unknown-linux-musl

ENV LANG=ru_RU.UTF-8
ENV LC_ALL=ru_RU.UTF-8

# Install cargo-watch for dev workflow
RUN cargo install cargo-watch

WORKDIR /workspace
