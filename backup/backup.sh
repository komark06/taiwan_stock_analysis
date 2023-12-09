#!/bin/sh

# Program:
#       Use mariadb-dump to backup all database.
date="$(date '+%Y-%m-%d %H:%M:%S')"
mariadb-dump --host=db --all-databases -uroot -p"$(cat /run/secrets/db-password)" > /data/backup.sql
if [ $? -eq 0 ]; then
    echo "$date :Backup succeed."
else
    echo "$date :Backup FAIL!"
fi
