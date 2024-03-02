#!/bin/sh

# Program:
#       Backup database and control copy of backup.

backup_folder="/app/data"
database_name=${MARIADB_DATABASE}
max_backups=30

remove_old_backups() {
    local backups_count=$(ls -1 "${backup_folder}" | wc -l)
    local excess_backups=$((backups_count - max_backups - 1)) # Need extra space for new backup

    if [ $excess_backups -gt 0 ]; then
        echo "Removing $excess_backups oldest backup(s) with database name prefix and .xz suffix..."

        # Remove excess backups with database name prefix and .xz suffix
        ls -1t "${backup_folder}" | grep "^${database_name}_.*\.xz$" | tail -n "$excess_backups" | xargs -I {} rm "${backup_folder}/{}"
    fi
}

perform_backup() {
    local date="$(date '+%Y-%m-%d')"
    local backup_file="/app/data/${MARIADB_DATABASE}_${date}.xz"
    mariadb-dump --host=db --user=root --password="$(cat /run/secrets/db-password)" $database_name | xz -9 > $backup_file
    if [ $? -eq 0 ]; then
        echo "$date: Backup succeed."
    else
        echo "$date: Backup FAIL!"
    fi
}

remove_old_backups
perform_backup
