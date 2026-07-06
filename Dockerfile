# ARG DEBIAN_FRONTEND=noninteractive
From ubuntu:noble as ffnc
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /
COPY ./deploy/sources.list /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y --no-install-recommends libopus-dev libopus0 opus-tools libssl-dev libffi-dev curl openssl pkg-config libwebrtc-audio-processing-dev ca-certificates && update-ca-certificates 

RUN mkdir /app

WORKDIR /app
COPY ./cert /app/cert
COPY ./libs /app/libs
COPY ./models /app/models
COPY ./static /app/static
COPY ./.env /app/.env
COPY ./log4rs.yaml /app/log4rs.yaml

COPY ./DigtalTalk ./
RUN mkdir /app/logs

EXPOSE 10000 10001 31401

CMD ["./DigtalTalk"]
