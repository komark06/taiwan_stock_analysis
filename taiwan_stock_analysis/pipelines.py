import datetime
import logging

import scrapy
from sqlalchemy.orm import Session

from taiwan_stock_analysis.table import (
    DailyTradingInfo,
    DailyTradingRecord,
    StockInfo,
    init_engine,
)


class StockInfoPipeline:
    """
    A class for processing information of stock.
    """

    def open_spider(self, spider: scrapy.Spider):
        """
        Initialize the SQLAlchemy engine and create table.

        Parameters:
            spider: The spider instance.
        """
        engine = init_engine()
        StockInfo.metadata.create_all(engine)
        self.session = Session(engine)
        self.logger = logging.getLogger(self.__class__.__name__)

    def process_item(self, item: dict, spider: scrapy.Spider):
        """
        Process the scraped item and insert it into database.

        Parameters:
            item: The item to be processed.
            spider: The spider instance.

        Returns:
            item: The processed item.
        """
        obj = StockInfo(**item)
        result = self.session.get(StockInfo, obj.ISINCode)
        if not result:
            self.session.add(obj)
        else:
            result.update_attributes(**item)
        self.logger.info(item)
        return item

    def close_spider(self, spider: scrapy.Spider):
        """
        Commit transaction and close SQLAlchemy session.

        Parameters:
            spider: The spider instance.
        """
        self.session.commit()
        self.session.close()


class DailyTradingPipeline:
    """
    A class for processing daily trading information of stock.
    """

    def open_spider(self, spider: scrapy.Spider):
        """
        Initialize the SQLAlchemy engine and create table.

        Parameters:
            spider: The spider instance.
        """
        self.engine = init_engine()
        DailyTradingInfo.metadata.create_all(self.engine)
        self.logger = logging.getLogger(self.__class__.__name__)

    def process_item(self, item: dict, spider: scrapy.Spider):
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
        with Session(self.engine) as session:
            for data in data_list:
                obj = DailyTradingInfo(
                    symbol,
                    *[
                        None if value == "--" else value.replace(",", "")
                        for value in data
                    ],
                )
                result = session.get(
                    DailyTradingInfo, (obj.symbol, obj.timestamp)
                )
                if not result:
                    session.add(obj)
                else:
                    result.update_attributes(obj)
            current = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
                hours=8
            )  # Current taipei time
            date_str = data_list[0][0]
            year, month, _ = map(int, date_str.split("/"))
            if current.year != year or current.month != month:
                obj = DailyTradingRecord(
                    symbol, datetime.date(year, month, 1), current
                )
                result = session.get(
                    obj.__class__, (obj.symbol, obj.timestamp)
                )
                if not result:
                    session.add(obj)
                else:
                    result.update_time = current
                self.logger.info(f"{symbol} {year}/{month} is done.")
            else:
                self.logger.info(
                    f"{symbol} {year}/{month} is current month, "
                    "so we need to update once this month concludes."
                )
            session.commit()
        return item

    def close_spider(self, spider: scrapy.Spider):
        pass
