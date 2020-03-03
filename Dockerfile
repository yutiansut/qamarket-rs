FROM daocloud.io/quantaxis/qarust:latest


WORKDIR /usr/src/qamarket-rs

COPY . .

RUN cargo build --release -j 20


CMD [ "/usr/src/qamarket-rs/target/release/qamarket-rs" ]

