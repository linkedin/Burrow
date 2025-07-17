# stage 1: builder
FROM golang:1.24.1-alpine as builder

ENV BURROW_SRC /usr/src/Burrow/

RUN apk add --no-cache git curl
WORKDIR $BURROW_SRC

COPY go.mod go.sum ./

RUN go mod download

COPY . $BURROW_SRC
RUN go build -o /tmp/burrow .

# stage 2: runner
FROM alpine:3.21

LABEL maintainer="LinkedIn Burrow https://github.com/linkedin/Burrow"

COPY --from=builder /tmp/burrow /app/
COPY docker-config/burrow.toml /etc/burrow/

CMD ["/app/burrow", "--config-dir", "/etc/burrow"]
