FROM golang:1.12-alpine as builder

ENV BURROW_SRC /usr/src/Burrow/

RUN apk add --no-cache git curl gcc libc-dev
ADD . $BURROW_SRC
WORKDIR $BURROW_SRC
RUN go mod tidy && go build -o /tmp/burrow .

FROM iron/go
LABEL maintainer="LinkedIn Burrow https://github.com/linkedin/Burrow"

WORKDIR /app
COPY --from=builder /tmp/burrow /app/
ADD /docker-config/burrow.toml /etc/burrow/

CMD ["/app/burrow", "--config-dir", "/etc/burrow"]
