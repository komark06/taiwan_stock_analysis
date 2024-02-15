import scrapy

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
        type_names = [column.name for column in StockInfoPipeline.data_type]
        for tr in response.selector.xpath(".//tr[td[@bgcolor='#FAFAD2']]"):
            found_category = tr.xpath("td[@colspan]/b/text()").get()
            if found_category:
                category = found_category.strip()
                continue
            attributes = [category]
            for td in tr.xpath("td[@bgcolor='#FAFAD2']"):
                text = td.xpath("text()").get()
                attributes.append(text.strip() if text else None)
            attributes = (
                attributes[:1] + attributes[1].split("\u3000") + attributes[2:]
            )
            stock = {
                key: value
                for key, value in zip(type_names, attributes, strict=True)
            }
            yield stock
