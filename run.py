import os
import subprocess
import sys
from datetime import datetime

from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def init_database():
    sql_file = "/data/backup.sql"
    if not os.path.exists(sql_file):
        print(f"{sql_file} does not exist.")
        return

    print(f"{now()}: {sql_file} exists. Start copy to MariaDB.")
    with open("/run/secrets/db-password") as file:
        password = file.read()
    with open(sql_file, "r") as file:
        result = subprocess.run(
            [
                "mariadb",
                "--host=db",
                "--user=root",
                f"--password={password}",
                "-D",
                "example",
            ],
            input=file.read(),
            text=True,
        )
    if result.returncode == 0:
        print(f"{now()}: Init succeed.")
    else:
        print(f"{now()}: Init FAIL!")
        sys.exit(1)
    ready_path = os.path.join(os.path.expanduser("~"), "ready")
    with open(ready_path, "w") as file:
        pass


def stock_daily():
    subprocess.run(["scrapy", "crawl", "daily_trading"])


def stock_info():
    subprocess.run(["scrapy", "crawl", "stock_info"])


if __name__ == "__main__":
    init_database()
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
