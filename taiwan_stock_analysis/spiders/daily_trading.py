import datetime
import json
import random
import re
from abc import ABC, abstractmethod

from scrapy import Request, Spider
from scrapy.http import Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..pipelines import DailyTradingRecord, StockInfo, init_engine


class AbstractSymbolCursor(ABC):
    @abstractmethod
    def get_symbol_info(self) -> list[tuple[int, datetime.date]]:
        """
        Get symbol number and listing date of all symbols.
        """
        pass

    @abstractmethod
    def exist(self, symbol: int, timestamp: datetime.date) -> bool:
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
        engine = init_engine()
        DailyTradingRecord.metadata.create_all(self.engine)
        self.session = Session(engine)

    def get_symbol_info(self) -> list[tuple[int, datetime.date]]:
        query = select(StockInfo).filter_by(
            classification="股票",
        )
        result = self.session.scalars(query)
        symbol_info = [
            {
                "symbol": int(item.symbol),
                "listing date": item.listing_date,
            }
            for item in result
        ]
        return symbol_info

    def exist(self, symbol: int, timestamp: datetime.date) -> bool:
        if self.session.get(DailyTradingRecord, (symbol, timestamp)):
            return True
        return False

    def close(self):
        self.session.close()


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

    def get_symbol_info(self) -> list[tuple[int, datetime.date]]:
        return self.cursor.get_symbol_info()

    def exist(self, symbol: int, timestamp: datetime.date) -> bool:
        return self.cursor.exist(symbol, timestamp)

    def generate_url(self, symbol: int, date: datetime.date):
        date_str = date.strftime("%Y%m%d")
        return (
            f"{self.base_url}?date={date_str}&stockNo={symbol}&response=json"
        )

    def start_requests(self):
        def next_month(date: datetime.date) -> datetime.datetime.date:
            """
            Return date of next month.
            """
            year = date.year
            month = date.month
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
            return datetime.date(year, month, 1)

        symbol_info = self.get_symbol_info()
        start_date = datetime.date(2010, 1, 1)
        for info in random.sample(symbol_info, len(symbol_info)):
            symbol = info["symbol"]
            date = info["listing date"]
            current = datetime.datetime.utcnow() + datetime.timedelta(
                hours=8
            )  # Current taipei time
            """
            Begin from the listing date if the listing date is more
            recent than the specified start date.
            """
            if date < start_date:
                date = start_date
            end_date = next_month(current)
            while end_date != date:
                if not self.exist(symbol, date):
                    yield Request(
                        url=self.generate_url(symbol, date),
                        callback=self.parse,
                        cb_kwargs={
                            "request_symbol": symbol,
                            "request_date": date,
                        },
                    )
                date = next_month(date)

    def parse(
        self,
        response: Response,
        request_symbol: int,
        request_date: datetime.date,
    ):
        js = json.loads(response.text)
        if js["stat"] != "OK":
            self.logger.warning(f"{response.url}:{response.text}")
            return
        title = js["title"].strip()
        date = re.split(r"\s+", title)[0]
        if request_date.strftime("%Y/%m") != date:
            self.logger.warning(
                f"Wrong date. Request_symbol:{request_symbol}, "
                f"year:{request_date.year}, month:{request_date.month}, "
                f"url: {response.url}\n{title}\n"
            )
            return
        symbol = int(re.split(r"\s+", title)[-1])
        if request_symbol != symbol:
            self.logger.warning(
                f"Wrong symbol. Request_symbol:{request_symbol}, "
                f"symbol:{symbol}, year:{request_date.year}, "
                f"month:{request_date.month}, url: {response.url}\n{title}\n"
            )
            return
        yield {"symbol": symbol, "data": js["data"]}
