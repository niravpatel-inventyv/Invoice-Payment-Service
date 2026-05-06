FROM rust:1-bookworm AS builder

WORKDIR /app
COPY services/mock-psp/ /app/
RUN cargo build --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/mock-psp /usr/local/bin/mock-psp

EXPOSE 8081
CMD ["mock-psp"]
