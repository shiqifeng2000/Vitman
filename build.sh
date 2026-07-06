#!/bin/sh

sudo rm -rf dst \
    && mkdir dst \
    && docker run -v ./src:/app/src \
        -v ./crates:/app/crates \
        -v ./.cargo:/app/.cargo \
        -v ./Cargo.toml:/app/Cargo.toml -v ./Cargo.lock:/app/Cargo.lock \
        -v ./dst:/app/target -t vitalk:builder /root/.cargo/bin/cargo build --release \
    && cp dst/release/DigtalTalk ./ \
    && sudo rm -rf dst

docker build . -t vitalk:v1

# zip webtalk.zip webtalk  && scp ./webtalk.zip root@10.10.181.175:/data/workspace/webtalk/ && ssh root@10.10.181.175