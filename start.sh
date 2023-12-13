#!/bin/bash

# Program:
#       If init.sql exists, we copy it into mariadb.

set -euo pipefail

sql_file="/data/backup.sql"
if [ ! -e "$sql_file" ]; then
    echo "The file $sql_file does not exist."
else
    date="$(date '+%Y-%m-%d %H:%M:%S')"
    echo "$date: $sql_file exists. Start copy to mariaDB."
    time mariadb --host=db --user=root --password=$(cat /run/secrets/db-password) -D example < $sql_file
    date="$(date '+%Y-%m-%d %H:%M:%S')"
    if [ $? -eq 0 ]; then
        echo "$date: Init succeed."
    else
        echo "$date: Init FAIL!"
        exit 1
    fi
fi

touch $HOME/ready

python run.py
