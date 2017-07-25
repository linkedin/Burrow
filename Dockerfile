FROM golang:alpine

MAINTAINER LinkedIn Burrow "https://github.com/linkedin/Burrow"

RUN apk add --no-cache curl bash git ca-certificates wget \
 && update-ca-certificates \
 && go get -u github.com/golang/dep/cmd/dep

ADD . $GOPATH/src/github.com/linkedin/Burrow
RUN cd $GOPATH/src/github.com/linkedin/Burrow \
 && dep ensure \
 && go install \
 && mv $GOPATH/bin/Burrow $GOPATH/bin/burrow \
 && apk del git curl wget

ADD docker-config /etc/burrow

WORKDIR /var/tmp/burrow

CMD ["/go/bin/burrow", "--config", "/etc/burrow/burrow.cfg"]
