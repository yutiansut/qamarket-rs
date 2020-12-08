FROM daocloud.io/quantaxis/qarust:latest


WORKDIR /usr/src/qamarket-rs

COPY . .

RUN cargo build --release -j 20
COPY sub_list.toml /usr/src/qamarket-rs/target/release/sub_list.toml

CMD [ "/usr/src/qamarket-rs/target/release/qamarket-rs" ,"sub_list.toml" ]

