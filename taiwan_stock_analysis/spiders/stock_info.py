import scrapy
from bs4 import BeautifulSoup

from ..pipelines import StockInfoPipeline


class StockInfoSpider(scrapy.Spider):
    name = "stock_info"
    custom_settings = {
        "ITEM_PIPELINES": {
            "taiwan_stock_analysis.pipelines.StockInfoPipeline": 300,
        },
        "STOCK_INFO_OVERWRITE": True,
        "PARSER": "lxml",
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) "
        "Gecko/20100101 Firefox/118.0",
    }
    start_urls = ["https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"]

    def parse(self, response):
        parser = self.settings.attributes["PARSER"].value
        type_name = list(StockInfoPipeline.data_type.keys())
        soup = BeautifulSoup(response.text, parser)
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
            if "remark" not in stock:
                stock["remark"] = None
            yield stock
