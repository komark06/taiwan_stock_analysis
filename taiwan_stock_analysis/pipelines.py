import logging
import os
from datetime import datetime, timedelta

from taiwan_stock_analysis.database.datatype import (
    UNSIGNED,
    BigInt,
    Decimal,
    Integer,
    SmallInt,
    Text,
    TinyInt,
    VarChar,
)

from .database.mariadbclient import MariadbClient, MariadbTable


def login_info() -> dict:
    """Return login info of Mariadb"""
    info = {"user": "root"}
    password_file = os.getenv("MARIADB_ROOT_PASSWORD_FILE")
    if password_file:
        with open(password_file, "r") as file:
            info["password"] = file.read()
    else:
        info["password"] = os.getenv("MARIADB_ROOT_PASSWORD")
    info["host"] = os.getenv("MARIADB_HOST")
    info["database"] = os.getenv("MARIADB_DATABASE")
    return info


class StockInfoPipeline:
    """
    A class for processing information of stock.

    Attributes:
        data_type (List[DateType]): A list composed of DataType used to
        represent a SQL table.
        table_name (str): The name of SQL table.
    """

    data_type = [
        VarChar("classification", 25),
        VarChar("symbol", 25, primary_key=True),
        VarChar("name", 25),
        VarChar("ISINCode", 25),
        VarChar("listing_date", 25),
        VarChar("market_category", 25),
        VarChar("industry_category", 25),
        VarChar("CFICode", 25),
        VarChar("remark", 25),
    ]
    table_name = "stock_info"

    def open_spider(self, spider):
        """
        Open the spider and initialize the MariaDB client.

        Parameters:
            spider: The spider instance.
        """
        table = MariadbTable(self.table_name, self.data_type)
        info = login_info()
        self.client = MariadbClient(table, **info)
        self.logger = logging.getLogger(self.__class__.__name__)

    def process_item(self, item, spider):
        """
        Process the scraped item and insert it into the MariaDB table.

        Parameters:
            item: The item to be processed.
            spider: The spider instance.

        Returns:
            item: The processed item.
        """
        self.client.insert(*item.values())
        self.logger.info(item)
        return item

    def close_spider(self, spider):
        """
        Close MariaDB client connection.

        Parameters:
            spider: The spider instance.
        """
        self.client.close()


class DailyTradingRecord:
    data_type = [
        SmallInt("symbol", attribute=UNSIGNED, primary_key=True),
        SmallInt("year", attribute=UNSIGNED, primary_key=True),
        TinyInt("month", attribute=UNSIGNED, primary_key=True),
        Text("update_time"),
    ]
    table_name = "stock_daily_trading_record"

    @property
    def table(self):
        """
        Return MariadbTable composed of table_name and data_type.
        """
        return MariadbTable(self.table_name, self.data_type)


class DailyTradingPipeline:
    """
    A class for processing daily trading information of stock.

    Attributes:
        data_type (List[DateType]): A list composed of DataType used to
        represent a SQL table.
        table_name (str): The name of SQL table.
        check_data_type (List[DateType]): A list composed of DataType
        used to represent a SQL table for storing update time.
        table_name (str): The name of SQL table for storing update time.
    """

    data_type = [
        SmallInt("symbol", attribute=UNSIGNED, primary_key=True),
        SmallInt("year", attribute=UNSIGNED, primary_key=True),
        TinyInt("month", attribute=UNSIGNED, primary_key=True),
        TinyInt("day", attribute=UNSIGNED, primary_key=True),
        BigInt("volume", attribute=UNSIGNED),
        BigInt("value", attribute=UNSIGNED),
        Decimal("open", 8, 3),
        Decimal("highest", 8, 3),
        Decimal("lowest", 8, 3),
        Decimal("closing", 8, 3),
        VarChar("delta", 10),
        Integer("transaction_volume"),
    ]
    table_name = "stock_daily_trading"

    def open_spider(self, spider):
        """
        Open the spider and initialize the MariaDB client.

        It will create another table for storing update time.

        Parameters:
            spider: The spider instance.
        """
        table = MariadbTable(self.table_name, self.data_type)
        info = login_info()
        self.client = MariadbClient(table, **info)
        self.record_client = MariadbClient(
            DailyTradingRecord().table, self.client.cursor(), **info
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def commit(self):
        """Commit to SQL database."""
        self.client.commit()

    def process_item(self, item, spider):
        """
        Process the scraped item and insert it into the MariaDB table.

        Parameters:
            item: The item to be processed.
            spider: The spider instance.

        Returns:
            item: The processed item.
        """
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
                if value == "--":
                    formatted_data[name] = None
                else:
                    formatted_data[name] = value.replace(",", "")
            self.client.insert(*formatted_data.values())
        current = datetime.utcnow() + timedelta(hours=8)  # Current taipei time
        year = int(formatted_data["year"])
        month = int(formatted_data["month"])
        if current.year != year or current.month != month:
            self.record_client.insert(
                symbol, year, month, current.strftime("%Y/%m/%d/%H:%M")
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
        """
        Close MariaDB client connection.

        Parameters:
            spider: The spider instance.
        """
        self.record_client.close()
        self.client.close()
