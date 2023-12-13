#!/bin/sh

# Program:
#       Use mariadb-dump to backup all database.
date="$(date '+%Y-%m-%d %H:%M:%S')"
file="/data/new.sql"
# Ignore SIGTERM
trap '' SIGTERM
mariadb-dump --host=db --databases example --user=root -p"$(cat /run/secrets/db-password)" > $file
if [ $? -eq 0 ]; then
    echo "$date: Backup succeed."
else
    echo "$date: Backup FAIL!"
    exit 1
fi
mv /data/new.sql /data/backup.sql
