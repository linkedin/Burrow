FROM golang:alpine

LABEL maintainer "LinkedIn Burrow https://github.com/linkedin/Burrow"

ADD https://raw.githubusercontent.com/pote/gpm/v1.4.0/bin/gpm /usr/local/bin/gpm
RUN chmod +x /usr/local/bin/gpm

COPY . $GOPATH/src/github.com/linkedin/Burrow

RUN apk add --no-cache --virtual .build-deps bash git \
    && cd $GOPATH/src/github.com/linkedin/Burrow \
    && gpm install \
    && go install \
    && mv $GOPATH/bin/Burrow $GOPATH/bin/burrow \
    && apk del .build-deps

ADD docker-config /etc/burrow

WORKDIR /var/tmp/burrow

CMD ["/go/bin/burrow", "--config", "/etc/burrow/burrow.cfg"]
