FROM daocloud.io/quantaxis/qarust:latest


WORKDIR /usr/src/qamarket-rs

COPY . .

RUN cargo run --release

