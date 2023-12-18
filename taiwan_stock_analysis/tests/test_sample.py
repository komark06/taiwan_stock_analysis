import os

from scrapy.http import TextResponse

from taiwan_stock_analysis.spiders.stock_info import StockInfoSpider


def html_path():
    dir_name = os.path.dirname(__file__)
    return os.path.join(dir_name, "example.html")


def test_spider():
    spider = StockInfoSpider()
    url = spider.start_urls[0]
    path = html_path()
    with open(path, "rb") as file:
        response = TextResponse(url=url, body=file.read(), encoding="big5")
    count = 0
    for info in spider.parse(response):
        print(info)
        assert len(info["classification"]) < 25
        count += 1
    assert count == 32659
