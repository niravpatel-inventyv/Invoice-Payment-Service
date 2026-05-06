FROM rust:1-bookworm AS builder

WORKDIR /app
COPY services/invoice-api/ /app/
RUN cargo build --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/invoice-api /usr/local/bin/invoice-api

EXPOSE 8080
CMD ["invoice-api"]
