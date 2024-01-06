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
        type_names = StockInfoPipeline.data_type.keys()
        soup = BeautifulSoup(response.text, "lxml")
        for td in soup.find_all("td", attrs={"bgcolor": "#FAFAD2"}):
            if td.attrs.get("colspan"):
                category = td.b.text.strip()
                continue
            attributes = [category]
            for element in td.parent.find_all("td"):
                text = element.text.strip()
                if "\u3000" in text:
                    attributes.extend(text.split("\u3000"))
                else:
                    attributes.append(text or None)
            stock = {
                key: value
                for key, value in zip(type_names, attributes, strict=True)
            }
            yield stock
