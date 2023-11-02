BOT_NAME = "taiwan_stock_analysis"

SPIDER_MODULES = ["taiwan_stock_analysis.spiders"]
NEWSPIDER_MODULE = "taiwan_stock_analysis.spiders"

ROBOTSTXT_OBEY = True

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0)"
    "Gecko/20100101 Firefox/118.0"
)
