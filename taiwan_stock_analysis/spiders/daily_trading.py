import json
import os
import random
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any

from scrapy import Request, Spider
from scrapy.http import Response

from ..pipelines import DailyTradingPipeline, StockInfoPipeline


class DailyTradingSpider(Spider):
    name = "daily_trading"
    custom_settings = {
        "DOWNLOAD_DELAY": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "ITEM_PIPELINES": {
            "taiwan_stock_analysis.pipelines.DailyTradingPipeline": 300,
        },
        "ROBOTSTXT_OBEY": False,
        "TELNETCONSOLE_USERNAME": "komark",
        "TELNETCONSOLE_PASSWORD": "123",
    }
    base_url = "https://www.twse.com.tw/rwd/en/afterTrading/STOCK_DAY"

    def __init__(self, name: str | None = None, **kwargs: Any):
        super().__init__(name, **kwargs)
        upper_folder = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
        db_folder = DailyTradingPipeline.output_folder_name
        db_name = DailyTradingPipeline.db_name
        db_path = os.path.join(upper_folder, db_folder, db_name)
        if os.path.exists(db_path):
            url_path = f"file:{db_path}?mode=ro"
            table = DailyTradingPipeline.check_table_name
            con = sqlite3.connect(url_path)
            cur = con.execute(
                "SELECT name FROM sqlite_master WHERE name = ?", (table,)
            )
            if not cur.fetchone():
                con.close()
                self.connection = None
            else:
                self.connection = con
        else:
            self.connection = None

    def start_requests(self):
        def get_symbols():
            """
            Get all informations of symbols.
            """
            upper_folder = os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))
            )
            db_folder = StockInfoPipeline.output_folder_name
            db_name = StockInfoPipeline.db_name
            db_path = os.path.join(upper_folder, db_folder, db_name)
            url_path = f"file:{db_path}?mode=ro"
            table = StockInfoPipeline.table_name
            con = sqlite3.connect(url_path)
            symbols = [
                (int(item[0]), item[1])
                for item in con.execute(
                    f"SELECT symbol,listing_date FROM {table} "
                    "WHERE classification='股票'"
                )
            ]
            con.close()
            return symbols

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

        def exist(year, month, symbol):
            """
            Check if data exist or not.

            If exist, return True. Else, return False.
            """
            if not self.connection:
                return False
            table = DailyTradingPipeline.check_table_name
            data = {"year": year, "month": month, "symbol": symbol}
            if self.connection.execute(
                f"SELECT * from {table} WHERE year=:year AND "
                "month=:month AND symbol=:symbol",
                data,
            ).fetchone():
                return True
            return False

        symbols = get_symbols()
        start_year = 2010
        start_month = 1
        for item in random.sample(symbols, len(symbols)):
            symbol = item[0]
            listing_date = item[1]
            year, month, day = listing_date.split("/")
            year = int(year)
            month = int(month)
            day = int(day)
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
                if not exist(year, month, symbol):
                    yield Request(
                        url=self.generate_url(year, month, symbol),
                        callback=self.parse,
                        cb_kwargs={
                            "year": year,
                            "month": month,
                            "request_symbol": symbol,
                        },
                    )
                else:
                    self.logger.info(
                        f"{symbol} {year}/{month} already exists. Pass it."
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
