import datetime
import os
from typing import Optional

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
    password_file = os.getenv("MARIADB_PASSWORD_FILE")
    with open(password_file, "r") as file:
        password = file.read()
    host = os.getenv("MARIADB_HOST")
    database = os.getenv("MARIADB_DATABASE")
    user = os.getenv("MARIADB_USER")
    url = (
        f"mariadb+mariadbconnector://{user}:"
        f"{password}@{host}/{database}?charset=utf8mb4"
    )
    return create_engine(
        url,
        pool_recycle=3600,
        echo=echo,
    )
