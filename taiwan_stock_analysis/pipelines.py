import logging
from datetime import datetime, timedelta

import mariadb


class StockInfoPipeline:
    data_type = {
        "classification": ("VARCHAR(25)",),
        "symbol": ("VARCHAR(25)",),
        "name": ("VARCHAR(25)",),
        "ISINCode": ("VARCHAR(25)",),
        "listing_date": ("VARCHAR(25)",),
        "market_category": ("VARCHAR(25)",),
        "industry_category": ("VARCHAR(25)",),
        "CFICode": ("VARCHAR(25)",),
        "remark": ("VARCHAR(25)",),
    }
    primary_key = ("symbol", "name")
    primary_key = ("name",)
    table_name = "stock_info"

    def open_spider(self, spider):
        password_file = "/run/secrets/db-password"
        with open(password_file, "r") as ps:
            self.connection = mariadb.connect(
                user="root",
                password=ps.read(),
                host="db",
                database="example",
            )
        self.cursor = self.connection.cursor()
        self.create_table_query = self._create_table_command()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cursor.execute(self._create_table_command())

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
        return (
            "CREATE TABLE IF NOT EXISTS "
            f"{self.table_name} ({', '.join(columns)})"
        )

    def add(self, *data):
        if not data:
            return
        if len(data) != len(self.data_type):
            raise ValueError("Data items do not match table columns.")
        placeholders = ", ".join(["?" for _ in data])
        sql_query = f"INSERT INTO {self.table_name} VALUES ({placeholders})"
        if self.primary_key:
            sql_query = (
                sql_query
                + " ON DUPLICATE KEY UPDATE "
                + ", ".join(
                    f"{column} = VALUES({column})"
                    for column in self.primary_key
                )
            )
        self.cursor.execute(sql_query, data)

    def process_item(self, item, spider):
        self.add(*item.values())
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.commit()
        self.connection.close()


class DailyTradingPipeline:
    data_type = {
        "symbol": ("SMALLINT", "UNSIGNED"),
        "year": ("SMALLINT", "UNSIGNED"),
        "month": ("TINYINT", "UNSIGNED"),
        "day": ("TINYINT", "UNSIGNED"),
        "volume": ("BIGINT", "UNSIGNED"),
        "value": ("BIGINT", "UNSIGNED"),
        "open": ("DECIMAL(5, 3)",),
        "highest": ("DECIMAL(5, 3)",),
        "lowest": ("DECIMAL(5, 3)",),
        "closing": ("DECIMAL(5, 3)",),
        "delta": ("VARCHAR(10)",),
        "transaction_volume": ("INTEGER",),
    }
    primary_key = ("symbol", "year", "month", "day")
    table_name = "stock_daily_trading"
    check_table_name = "check_table"

    def _create_check_table(self):
        """
        Create check table if needed.
        """
        command = (
            f"CREATE TABLE IF NOT EXISTS {self.check_table_name} "
            "(symbol INTEGER, year INTEGER, month INTEGER, update_time TEXT ,"
            "PRIMARY KEY(symbol, year, month))"
        )
        self.cursor.execute(command)

    def _create_table_command(self):
        """
        Return command to create table.
        """
        columns = []
        for name, statement in self.data_type.items():
            column = [name]
            if statement:
                column.extend(statement)
            columns.append(" ".join(column))
        if self.primary_key:
            columns.append(f"PRIMARY KEY ({', '.join(self.primary_key)})")
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} "
            f"({', '.join(columns)})"
        )

    def open_spider(self, spider):
        password_file = "/run/secrets/db-password"
        with open(password_file, "r") as ps:
            self.connection = mariadb.connect(
                user="root",
                password=ps.read(),
                host="db",
                database="example",
            )
        self.cursor = self.connection.cursor()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cursor.execute(self._create_table_command())
        self._create_check_table()

    def add(self, *data):
        if not data:
            return
        if len(data) != len(self.data_type):
            raise ValueError("Data items do not match table columns.")
        placeholders = ", ".join(["?" for _ in data])
        sql_query = f"INSERT INTO {self.table_name} VALUES ({placeholders})"
        if self.primary_key:
            sql_query = (
                sql_query
                + " ON DUPLICATE KEY UPDATE "
                + ", ".join(
                    f"{column} = VALUES({column})"
                    for column in self.primary_key
                )
            )
        self.cursor.execute(sql_query, data)

    def commit(self):
        self.connection.commit()

    def process_item(self, item, spider):
        symbol = item["symbol"]
        data_list = item["data"]
        fields = item["fields"]
        for data in data_list:
            date = data[0]
            formatted_data = {"symbol": symbol}
            formatted_data.update(
                (name, value)
                for name, value in zip(
                    ["year", "month", "day"], date.split("/")
                )
            )
            for name, value in zip(fields[1:], data[1:]):
                formatted_data[name] = value.replace(",", "")
            self.add(*formatted_data.values())
        current = datetime.utcnow() + timedelta(hours=8)  # Current taipei time
        year = int(formatted_data["year"])
        month = int(formatted_data["month"])
        if current.year != year or current.month != month:
            self.cursor.execute(
                f"INSERT INTO {self.check_table_name} "
                "VALUES (?, ?, ?, ?)  ON DUPLICATE KEY UPDATE "
                "symbol = VALUES(symbol), year = VALUES(year), "
                "month = VALUES(month)",
                (symbol, year, month, current.strftime("%Y/%m/%d/%H:%M")),
            )
            self.logger.info(f"{symbol} {year}/{month} is done.")
        else:
            self.logger.info(
                f"{symbol} {year}/{month} is current month, "
                "so we need to update everyday."
            )
        self.commit()
        return item

    def close_spider(self, spider):
        self.commit()
        self.connection.close()
