FROM iron/go
LABEL maintainer="LinkedIn Burrow https://github.com/linkedin/Burrow"

WORKDIR /app
ADD burrow /app/

CMD ["/app/burrow", "--config-dir", "/etc/burrow"]
