import logging
import os
import sqlite3


class StockInfoPipeline:
    data_type = {
        "classification": ("TEXT",),
        "symbol": ("TEXT",),
        "name": ("TEXT",),
        "ISINCode": ("TEXT",),
        "listing_date": ("TEXT",),
        "market_category": ("TEXT",),
        "industry_category": ("TEXT",),
        "CFICode": ("TEXT",),
        "remark": ("TEXT",),
    }
    primary_key = ("symbol", "name")

    def __init__(
        self,
    ):
        self.table_name = "stock_info"
        self.logger = logging.getLogger(self.__class__.__name__)

    def open_spider(self, spider):
        output_folder_name = "output"
        db_name = "stock_info.db"
        current_folder = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(current_folder, output_folder_name)
        os.makedirs(output_path, exist_ok=True)
        self.db_path = os.path.join(output_path, db_name)
        self.connection = sqlite3.connect(self.db_path)
        self.cursor = self.connection.cursor()
        self.create_table_query = self._create_table_command()
        self._create_table()

    def _create_table(self):
        """
        Check if a structure of table is as same as setting.

        If table is not existed, create a new one.
        If structure of table is incorrect, rename it by adding '_old'
        suffix and then create a new one.
        """
        self.cursor.execute(
            "SELECT sql FROM sqlite_master WHERE tbl_name = "
            f"'{self.table_name}'"
        )
        data = self.cursor.fetchall()
        if len(data) == 0:
            self.cursor.execute(self.create_table_query)
            return
        query = data[0][0]
        if query != self.create_table_query:
            new_table_name = self.table_name + "_old"
            self.logger.info(
                f"Structure of {self.table_name} is incorrect."
                "Rename it to {new_table_name} and create a new one."
            )
            self.cursor.execute(
                f"ALTER TABLE {self.table_name} RENAME TO {new_table_name}"
            )
            self.cursor.execute(self.create_table_query)

    def _create_table_command(self):
        """
        Return command to create table.
        """
        columns = []
        for name, statement in self.data_type.items():
            column = [name]
            if statement:
                column.append(*statement)
            columns.append(" ".join(column))
        if self.primary_key:
            columns.append(f"PRIMARY KEY ({', '.join(self.primary_key)})")
        return f"CREATE TABLE {self.table_name} ({', '.join(columns)})"

    def add(self, *data):
        if not data:
            return
        if len(data) != len(self.data_type):
            raise ValueError("Data items do not match table columns.")
        placeholders = ", ".join(["?" for _ in data])
        insert_sql = (
            f"INSERT OR REPLACE INTO {self.table_name} VALUES ({placeholders})"
        )
        self.cursor.execute(insert_sql, data)

    def close_spider(self, spider):
        self.connection.commit()
        self.connection.close()

    def process_item(self, item, spider):
        self.add(*item.values())
        return item
