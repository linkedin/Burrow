# stage 1: builder
FROM golang:1.14.3-alpine as builder

ENV BURROW_SRC /usr/src/Burrow/

RUN apk add --no-cache git curl
COPY . $BURROW_SRC
WORKDIR $BURROW_SRC

RUN go mod tidy && go build -o /tmp/burrow .

COPY docker-config/burrow.toml /etc/burrow/
