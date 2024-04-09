#!/bin/sh

# Program:
#       Restore database when needed.

database_name=${MARIADB_DATABASE}
backup_folder="/docker-entrypoint-initdb.d"
user="root"
password=$(cat /run/secrets/db-password)

backup_file=$(ls -1t $backup_folder | grep "^example_.*\.xz$" | head -n 1)
# Make sure backup exist so that we can initialize database
if [ -z $backup_file ]; then
    echo "No backup found. Skipping database initialization."
    exit 0
fi
backup_path=$backup_folder/$backup_file
xzcat $backup_path | mariadb --user=$user --password=$password $database_name
if [ $? -eq 0 ]; then
    echo "Database initialization succeeded."
    exit 0
else
    echo "Database initialization failed!"
    exit 1
fi

