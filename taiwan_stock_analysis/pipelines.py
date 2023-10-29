import logging
import os
import sqlite3

from scrapy.exceptions import NotConfigured


class StockInfoPipeline:
    data_type = {
        "classification": "TEXT",
        "symbol": "TEXT",
        "name": "TEXT",
        "ISINCode": "TEXT",
        "listing_date": "TEXT",
        "market_category": "TEXT",
        "industry_category": "TEXT",
        "CFICode": "TEXT",
        "remark": "TEXT",
    }

    def __init__(
        self,
        overwrite: bool = False,
    ):
        self.over_write = overwrite
        self.table_name = "stock_info"
        self.logger = logging.getLogger(self.__class__.__name__)

    @classmethod
    def from_crawler(
        cls,
        crawler,
    ):
        setting_name = "STOCK_INFO_OVERWRITE"
        overwrite = crawler.settings.get(setting_name)
        logger = logging.getLogger(cls.__name__)
        if type(overwrite) is not bool:
            logger.warning(
                f"{setting_name} must be Boolean value. "
                f"Set {setting_name} to False by default."
            )
            overwrite = False
        return cls(overwrite)

    def open_spider(
        self,
        spider,
    ):
        output_folder_name = "output"
        db_name = "stock_info.db"

        current_folder = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(
            current_folder,
            output_folder_name,
        )
        os.makedirs(
            output_path,
            exist_ok=True,
        )
        self.db_path = os.path.join(
            output_path,
            db_name,
        )
        self.connection = sqlite3.connect(self.db_path)
        self.cursor = self.connection.cursor()
        if self._check_table() is False:
            if self.over_write is False:
                raise NotConfigured(
                    f"Structure of table is incorrect and "
                    "overwrite is set to False: "
                    f"table name is {self.table_name}"
                )
            self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            columns = []
            for (
                column_name,
                column_type,
            ) in self.data_type.items():
                if column_type is not None:
                    columns.append(f"{column_name} {column_type}")
                else:
                    columns.append(column_name)
            columns.append("PRIMARY KEY (symbol, name)")
            self.cursor.execute(
                f"CREATE TABLE {self.table_name} ({', '.join(columns)})"
            )
            self.connection.commit()

    def _check_table(
        self,
    ) -> bool:
        """
        Check if a structure of table is as same as setting.
        """
        self.cursor.execute(f"PRAGMA table_info({self.table_name})")
        table = self.cursor.fetchall()
        if len(table) == 0:
            return False
        for column in table:
            name = column[1]
            expect_data_type = column[2]
            data_type = self.data_type.get(name)
            if data_type is None:
                return False
            elif data_type != expect_data_type:
                return False
        return True

    def add(
        self,
        *data,
    ):
        if not data:
            return
        if len(data) != len(self.data_type):
            raise ValueError("Data items do not match table columns.")
        placeholders = ", ".join(["?" for _ in data])
        insert_sql = f"INSERT INTO {self.table_name} VALUES ({placeholders})"
        self.cursor.execute(
            insert_sql,
            data,
        )

    def close_spider(
        self,
        spider,
    ):
        self.connection.commit()
        self.connection.close()

    def process_item(
        self,
        item,
        spider,
    ):
        self.add(*item.values())
        return item
