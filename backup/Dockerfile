FROM alpine:latest

RUN apk add --no-cache \
    mariadb-client \
    tzdata

COPY cronjob .

RUN crontab cronjob

# Copy your script to the working directory
COPY backup.sh .

# Give execute permissions to your script
RUN chmod +x backup.sh

# Run cron in the foreground
CMD ["/usr/sbin/crond", "-f", "-d", "8"]
