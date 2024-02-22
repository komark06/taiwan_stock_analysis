"""
Generate data for testing DailyTradingSpider
"""

import json
from datetime import datetime

daily_data = {
    "Trade Volume": "1,000,000",
    "Trade Value": "1,000,000",
    "Opening Price": "94.87",
    "Highest Price": "94.87",
    "Lowest Price": "94.87",
    "Closing Price": "94.87",
    "Change": "+0.01",
    "Transaction": "9487",
}
_daily_json = {
    "stat": "OK",
    "fields": [
        "Date",
        "Trade Volume",
        "Trade Value",
        "Opening Price",
        "Highest Price",
        "Lowest Price",
        "Closing Price",
        "Change",
        "Transaction",
    ],
    "notes": [
        " Symbols for Direction:+/-/ X represent Up/Down/Not compared.\n",
        (
            "Today statistics cover regular trading, Odd-lot, After-hour Fixed"
            " Price, Block trading, but exclude Auction and Tender offers."
        ),
        (
            "If the last digit of the 6 digit alphanumeric ETF security code"
            " is K, M, S or C, it is traded in foreign currency."
        ),
    ],
}


def daily_json(symbol: str, year: str, month: str, day: str):
    js = _daily_json.copy()
    js["date"] = f"{year}{month}{day}"
    js["title"] = f"{year}/{month}  Daily Trading Value/Volume of {symbol} "
    js["data"] = []
    year = int(year)
    month = int(month)
    day = int(day)
    date = datetime(year, month, 1)
    data = [date.strftime("%Y/%m/%d")] + list(daily_data.values())
    js["data"].append(data)
    return json.dumps(js)
