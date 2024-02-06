FROM alpine:latest

RUN apk add --no-cache \
    mariadb-client \
    tzdata

WORKDIR /app

COPY cronjob backup.sh .

RUN crontab cronjob

RUN chmod +x backup.sh

CMD ["/usr/sbin/crond", "-f", "-d", "8"]
