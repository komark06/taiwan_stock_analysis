import datetime
import logging
import os
from typing import Optional

import scrapy
import sqlalchemy
from sqlalchemy import (
    Date,
    DateTime,
    Double,
    Integer,
    SmallInteger,
    String,
    create_engine,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    Session,
    mapped_column,
)


class Base(MappedAsDataclass, DeclarativeBase):
    pass


class StockInfo(Base):
    __tablename__ = "stock_info"
    classification: Mapped[str] = mapped_column(String(50))
    symbol: Mapped[str] = mapped_column(String(25))
    name: Mapped[str] = mapped_column(String(50))
    ISINCode: Mapped[str] = mapped_column(String(25), primary_key=True)
    listing_date: Mapped[datetime.date] = mapped_column(Date)
    market_category: Mapped[str] = mapped_column(String(50))
    industry_category: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True
    )
    CFICode: Mapped[str] = mapped_column(String(25))
    remark: Mapped[Optional[str]] = mapped_column(String(25), nullable=True)

    def update_attributes(self, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, attr, value)


class DailyTradingRecord(Base):
    __tablename__ = "stock_daily_trading_info_record"
    symbol: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    timestamp: Mapped[datetime.date] = mapped_column(Date, primary_key=True)
    update_time: Mapped[datetime.datetime] = mapped_column(DateTime)


class DailyTradingInfo(Base):
    __tablename__ = "stock_daily_trading_info"
    symbol: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    timestamp: Mapped[str] = mapped_column(Date, primary_key=True)
    volume: Mapped[int] = mapped_column(Integer)
    value: Mapped[int] = mapped_column(Integer)
    open: Mapped[float] = mapped_column(Double)
    highest: Mapped[float] = mapped_column(Double)
    lowest: Mapped[float] = mapped_column(Double)
    closing: Mapped[float] = mapped_column(Double)
    delta: Mapped[float] = mapped_column(String(10))
    transaction_volume: Mapped[int] = mapped_column(Integer)
    attr_name = [
        "symbol",
        "timestamp",
        "volume",
        "value",
        "open",
        "highest",
        "closing",
        "delta",
        "transaction_volume",
    ]

    def update_attributes(self, obj):
        for name in self.attr_name:
            setattr(self, name, getattr(obj, name))


def init_engine(echo: bool = False) -> sqlalchemy.engine.Engine:
    """
    Initialize the SQLAlchemy engine and return it.

    Parameters:
        echo (bool): A flag indicating whether to enable echo mode.
                     When set to True, SQL statements will be printed
                     to the console.
                     Defaults to False.

    Returns:
        sqlalchemy.engine.Engine: The SQLAlchemy engine instance.
    """
    password_file = os.getenv("MARIADB_ROOT_PASSWORD_FILE")
    with open(password_file, "r") as file:
        password = file.read()
    host = os.getenv("MARIADB_HOST")
    database = os.getenv("MARIADB_DATABASE")
    url = (
        "mariadb+mariadbconnector://root:"
        f"{password}@{host}/{database}?charset=utf8mb4"
    )
    return create_engine(
        url,
        pool_recycle=3600,
        echo=echo,
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
