import subprocess
from concurrent.futures import ProcessPoolExecutor

from apscheduler.schedulers.blocking import BlockingScheduler


def stock_daily():
    subprocess.run(["scrapy", "crawl", "daily_trading"])


def stock_info():
    subprocess.run(["scrapy", "crawl", "stock_info"])


if __name__ == "__main__":
    stock_info()
    stock_daily()
    executors = {
        "default": ProcessPoolExecutor(2),
    }
    scheduler = BlockingScheduler(timezone="Asia/Taipei", executors=executors)
    scheduler.add_job(stock_info, "cron", hour=0)
    scheduler.add_job(stock_daily, "cron", hour=18)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown(wait=False)
