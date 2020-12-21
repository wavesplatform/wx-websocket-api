FROM rust:1.46 as builder
WORKDIR /usr/src/service

RUN rustup component add rustfmt

RUN echo "fn main() {}" > dummy.rs
COPY Cargo.* ./
RUN sed -i 's#src/main.rs#dummy.rs#' Cargo.toml
RUN cargo build --release
RUN sed -i 's#dummy.rs#src/main.rs#' Cargo.toml
COPY ./src ./src

RUN cargo install --path .

FROM debian:stretch
WORKDIR /usr/www/app
RUN apt-get update && apt-get install -y curl openssl libssl-dev
# RUN curl -ks 'https://cert.host.server/ssl_certs/EnterpriseRootCA.crt' -o '/usr/local/share/ca-certificates/EnterpriseRootCA.crt'
RUN /usr/sbin/update-ca-certificates
COPY --from=builder /usr/local/cargo/bin/service .

CMD ["./service"]
