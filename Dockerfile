FROM iron/go
MAINTAINER LinkedIn Burrow "https://github.com/linkedin/Burrow"

WORKDIR /app
ADD burrow /app/
ADD docker-config /etc/burrow

CMD ["/app/burrow", "--config-dir", "/etc/burrow"]
