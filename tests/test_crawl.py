import json
from datetime import datetime, timedelta
from pathlib import Path

from scrapy import signals
from scrapy.utils.test import get_crawler
from twisted.internet import defer
from twisted.trial.unittest import TestCase

from taiwan_stock_analysis.spiders.daily_trading import (
    AbstractSymbolCursor,
    DailyTradingSpider,
)
from taiwan_stock_analysis.spiders.stock_info import StockInfoSpider
from tests import sample_data_dir, sample_parm
from tests.daily_data import daily_data
from tests.mockserver import MockServer


class NoCustomSpider:
    custom_settings = {}

    def __init__(self, url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if url:
            self.start_urls = [url]


class TestDailyTradingSpider(NoCustomSpider, DailyTradingSpider):
    __test__ = False


class TestStockInfoSpider(NoCustomSpider, StockInfoSpider):
    __test__ = False


def file_url(filename: str):
    return f"/{sample_parm}/{filename}"


class StockInfoSpiderTestCase(TestCase):
    def setUp(self):
        self.mockserver = MockServer()
        self.mockserver.__enter__()
        self.spider = TestStockInfoSpider

    def tearDown(self):
        self.mockserver.__exit__(None, None, None)

    @defer.inlineCallbacks
    def test_parse(self):
        """
        Test if StockInfoSpider can parse valid web page or not.
        """
        items = []

        def on_item_scraped(item):
            print(item)
            items.append(item)

        filename = "stock_info.html"
        crawler = get_crawler(self.spider)
        crawler.signals.connect(on_item_scraped, signals.item_scraped)
        yield crawler.crawl(self.mockserver.url(file_url(filename)))
        self.assertEqual(
            len(items), 47, msg="Spider doesn't scrape exact 47 pages."
        )
        # Check there is no space in data
        for item in items:
            for key, value in item.items():
                if value:
                    self.assertFalse(" " in value)
        path = Path(sample_data_dir, "stock_info.json")
        with open(path) as file:
            js = json.load(file)
        # Check if scraped data is as expected
        for output, expect in zip(items, js):
            self.assertEqual(output, expect)


class FakeSymbolCursor(AbstractSymbolCursor):
    def get_symbols(self) -> list[tuple[int, str]]:
        current = datetime.utcnow() + timedelta(hours=8)  # Current taipei time
        return [(1101, current.strftime("%Y/%m/%d"))]

    def exist(self, year: int, month: int, symbol: int) -> bool:
        return False

    def close(self):
        pass


class DailyTradingSpiderTestCase(TestCase):
    def setUp(self):
        self.mockserver = MockServer()
        self.mockserver.__enter__()
        self.spider = TestDailyTradingSpider

    def tearDown(self):
        self.mockserver.__exit__(None, None, None)

    @defer.inlineCallbacks
    def test_parse(self):
        items = []

        def on_item_scraped(item):
            print(item)
            items.append(item)

        crawler = get_crawler(self.spider)
        crawler.signals.connect(on_item_scraped, signals.item_scraped)
        cursor = FakeSymbolCursor
        yield crawler.crawl(
            cursor=cursor, base_url=self.mockserver.url("/daily")
        )
        expect_data = list(daily_data.values())
        for item in items:
            for data in item["data"]:
                # The first element is not fixed so we don't test
                for output, expect in zip(data[1:], expect_data, strict=True):
                    self.assertEqual(output, expect)
