from fastapi import FastAPI

from taiwan_stock_analysis.database.mariadbclient import MariadbClient, MariadbTable
from taiwan_stock_analysis.pipelines import StockInfoPipeline, login_info

cls = StockInfoPipeline
table = MariadbTable(cls.table_name, cls.data_type)
info = login_info()
client = MariadbClient(table, **info)
app = FastAPI()


@app.get("/get_info")
async def get_info(symbol: str | None = None):
    if not symbol:
        symbol = "1101"
    cursor = client.cursor()
    cursor.execute("SELECT * FROM stock_info WHERE symbol=?", (symbol,))
    names = [description[0] for description in cursor.description]
    js = {}
    data = [
        {name: value for name, value in zip(names, row)}
        for row in cursor.fetchall()
    ]
    cursor.close()
    if not data:
        js["status"] = "No data"
        return js
    js["status"] = "OK"
    js["data"] = data
    return js
