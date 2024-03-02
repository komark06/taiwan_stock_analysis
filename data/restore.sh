#!/bin/sh

# Program:
#       Restore database when needed.

database_name=${MARIADB_DATABASE}
backup_folder="/docker-entrypoint-initdb.d"
table_name="stock_daily_trading"
user="root"
password=$(cat /run/secrets/db-password)

init_database() {
    local backup_file=$(ls -1t $backup_folder | grep "^example_.*\.xz$" | head -n 1)
    # Make sure backup exist so that we can initialize database
    if [ -z $backup_file ]; then
        echo "No backup found. Skipping database initialization."
        return 0
    fi
    local backup_path=$backup_folder/$backup_file
    xzcat $backup_path | mariadb --user=$user --password=$password $database_name
    if [ $? -eq 0 ]; then
        echo "Database initialization succeeded."
        return 0
    else
        echo "Database initialization failed!"
        return 1
    fi
}

mariadb --user=$user --password=$password --execute="SELECT COUNT(*) FROM $table_name" $database_name > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Table $table_name does not exist. Starting database initialization."
    init_database
else
    echo "Table $table_name already exists. No need to initialize database."
fi
