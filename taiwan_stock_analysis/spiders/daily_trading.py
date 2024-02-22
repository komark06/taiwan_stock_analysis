import json
import random
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import mariadb
from scrapy import Request, Spider
from scrapy.http import Response

from ..pipelines import DailyTradingRecord, StockInfoPipeline, login_info


class AbstractSymbolCursor(ABC):
    @abstractmethod
    def get_symbols(self) -> list[tuple[int, str]]:
        """
        Get symbol number and listing date of all symbols.
        """
        pass

    @abstractmethod
    def exist(self, year: int, month: int, symbol: int) -> bool:
        """
        Verify the presence of symbol data for a particular date.

        If exist, return True. Else, return False.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close connection to database.
        """
        pass


class SymbolCursor(AbstractSymbolCursor):
    def __init__(self):
        info = login_info()
        self.connection = mariadb.connect(**info)
        self.cursor = self.connection.cursor()

    def get_symbols(self) -> list[tuple[int, str]]:
        table = StockInfoPipeline.table_name
        self.cursor.execute(
            f"SELECT symbol,listing_date FROM {table} "
            "WHERE classification='股票'"
        )
        symbols = [(int(item[0]), item[1]) for item in self.cursor]
        return symbols

    def exist(self, year: int, month: int, symbol: int) -> bool:
        table = DailyTradingRecord.table_name
        data = (year, month, symbol)
        self.cursor.execute(
            f"SELECT * from {table} WHERE year=? AND " "month=? AND symbol=?",
            data,
        )
        if self.cursor.fetchone():
            return True
        return False

    def close(self):
        self.cursor.close()
        self.connection.close()


class DailyTradingSpider(Spider):
    name = "daily_trading"
    custom_settings = {
        "DOWNLOAD_DELAY": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "ITEM_PIPELINES": {
            "taiwan_stock_analysis.pipelines.DailyTradingPipeline": 300,
        },
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(
        self,
        *args,
        cursor: AbstractSymbolCursor = SymbolCursor,
        base_url: str = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cursor = cursor()
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = (
                "https://www.twse.com.tw/rwd/en/afterTrading/STOCK_DAY"
            )

    def get_symbols(self):
        return self.cursor.get_symbols()

    def exist(self, year, month, symbol):
        return self.cursor.exist(year, month, symbol)

    def start_requests(self):
        def next_month(year: int, month: int):
            """
            Return date of next month.
            """
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
            return year, month

        symbols = self.get_symbols()
        start_year = 2010
        start_month = 1
        for item in random.sample(symbols, len(symbols)):
            symbol = item[0]
            listing_date = item[1]
            year, month, day = map(int, listing_date.split("/"))
            current = datetime.utcnow() + timedelta(
                hours=8
            )  # Current taipei time
            """
            Begin from the listing date if the listing date is more
            recent than the specified start date.
            """
            if year < start_year:
                year = start_year
                month = start_month
            end_year, end_month = next_month(current.year, current.month)
            while end_year != year or end_month != month:
                if not self.exist(year, month, symbol):
                    yield Request(
                        url=self.generate_url(year, month, symbol),
                        callback=self.parse,
                        cb_kwargs={
                            "year": year,
                            "month": month,
                            "request_symbol": symbol,
                        },
                    )
                year, month = next_month(year, month)

    def generate_date(self, year: int, month: int):
        return f"{year}{month:02d}01"

    def generate_url(self, year: int, month: int, symbol: int):
        date_str = self.generate_date(year, month)
        return (
            f"{self.base_url}?date={date_str}&stockNo={symbol}&response=json"
        )

    def parse(
        self,
        response: Response,
        year: int,
        month: int,
        request_symbol: int,
    ):
        js = json.loads(response.text)
        if js["stat"] != "OK":
            self.logger.warning(f"{response.url}:{response.text}")
            return
        title = js["title"].strip()
        date = re.split(r"\s+", title)[0]
        if f"{year}/{month:02d}" != date:
            self.logger.warning(
                f"Wrong date. Request_symbol:{request_symbol}, year:{year}"
                f", month:{month}, url: {response.url}\n{title}\n"
            )
            return
        symbol = int(re.split(r"\s+", title)[-1])
        if request_symbol != symbol:
            self.logger.warning(
                f"Wrong symbol. Request_symbol:{request_symbol}, "
                f"symbol:{symbol}, year:{year}, month:{month}, "
                f"url: {response.url}\n{title}\n"
            )
            return
        yield {"symbol": symbol, "fields": js["fields"], "data": js["data"]}
