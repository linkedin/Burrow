FROM golang:1.9-alpine as builder

ENV DEP_VERSION="0.3.2"
RUN apk add --no-cache git curl && \
	curl -L -s https://github.com/golang/dep/releases/download/v${DEP_VERSION}/dep-linux-amd64 -o $GOPATH/bin/dep && \
	chmod +x $GOPATH/bin/dep && \
	mkdir -p $GOPATH/src/github.com/linkedin/Burrow

ADD . $GOPATH/src/github.com/linkedin/Burrow/
RUN cd $GOPATH/src/github.com/linkedin/Burrow && \
	dep ensure && \
	go build -o /tmp/burrow .

FROM iron/go
MAINTAINER LinkedIn Burrow "https://github.com/linkedin/Burrow"

WORKDIR /app
COPY --from=builder /tmp/burrow /app/
ADD /docker-config/burrow.toml /etc/burrow/

CMD ["/app/burrow", "--config-dir", "/etc/burrow"]
