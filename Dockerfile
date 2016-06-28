FROM golang:alpine

MAINTAINER LinkedIn Burrow "https://github.com/linkedin/Burrow"

RUN apk add --update bash curl git && apk add ca-certificates wget && update-ca-certificates && rm -rf /var/cache/apk/*

RUN wget https://raw.githubusercontent.com/pote/gpm/v1.4.0/bin/gpm && chmod +x gpm && mv gpm /usr/local/bin

ADD . $GOPATH/src/github.com/linkedin/burrow
RUN cd $GOPATH/src/github.com/linkedin/burrow && gpm install && go install

ADD docker-config /etc/burrow

WORKDIR /var/tmp/burrow

CMD ["/go/bin/burrow", "--config", "/etc/burrow/burrow.cfg"]
