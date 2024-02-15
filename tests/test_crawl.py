import json
from pathlib import Path

from scrapy import Spider, signals
from scrapy.utils.test import get_crawler
from twisted.internet import defer
from twisted.trial.unittest import TestCase

from taiwan_stock_analysis.spiders.stock_info import StockInfoSpider
from tests import sample_data_dir, sample_parm
from tests.mockserver import MockServer


class MockServerSpider(Spider):
    def __init__(self, mockserver=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mockserver = mockserver


class NoCustomSpider(MockServerSpider):
    custom_settings = {}

    def __init__(self, url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = [url]


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
        yield crawler.crawl(
            self.mockserver.url(file_url(filename)), mockserver=self.mockserver
        )
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
