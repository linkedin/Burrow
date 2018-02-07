FROM iron/go
MAINTAINER LinkedIn Burrow "https://github.com/linkedin/Burrow"

WORKDIR /app
ADD burrow /app/
ADD burrow.toml /etc/burrow/

CMD ["/app/burrow", "--config-dir", "/etc/burrow"]
