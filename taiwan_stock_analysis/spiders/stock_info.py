import scrapy
from bs4 import BeautifulSoup

from ..pipelines import StockInfoPipeline


class StockInfoSpider(scrapy.Spider):
    name = "stock_info"
    custom_settings = {
        "ITEM_PIPELINES": {
            "taiwan_stock_analysis.pipelines.StockInfoPipeline": 300,
        },
    }
    start_urls = ["https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"]

    def parse(self, response):
        type_name = StockInfoPipeline.data_type.keys()
        soup = BeautifulSoup(response.text, "lxml")
        for tr in soup.find_all("tr"):
            if tr.find("td", attrs={"bgcolor": "#FAFAD2"}) is None:
                continue
            if tr.find("td", attrs={"bgcolor": "#FAFAD2", "colspan": True}):
                category = tr.find(
                    "td", attrs={"bgcolor": "#FAFAD2", "colspan": True}
                ).b.text.strip()
                continue
            stock = {}
            value = [category]
            for td in tr.find_all("td", attrs={"bgcolor": "#FAFAD2"}):
                value.append(td.text.strip() if td.text else None)
            value = value[:1] + value[1].split("\u3000") + value[2:]
            for name, value in zip(type_name, value):
                stock[name] = value
            yield stock
