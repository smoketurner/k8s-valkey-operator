# Build stage - using Alpine for musl toolchain
FROM rust:1.92-alpine AS builder

WORKDIR /app

# Install build dependencies (cmake, perl, go needed for aws-lc-rs)
RUN apk add --no-cache musl-dev cmake make perl go clang

COPY Cargo.toml Cargo.lock ./

# Cache dependencies - create dummy src matching project structure
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "// dummy" > src/lib.rs && \
    cargo build --release && \
    rm -rf src

COPY src ./src

# Build the actual application
RUN cargo build --release

# Runtime stage - scratch for minimal image
FROM scratch

# Copy CA certificates for TLS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the statically-linked binary
COPY --from=builder /app/target/release/my-operator /my-operator

# Run as non-root (numeric UID since scratch has no /etc/passwd)
USER 1000:1000

ENTRYPOINT ["/my-operator"]
