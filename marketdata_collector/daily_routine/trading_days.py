import configparser
import time
import pandas as pd
from datetime import date, timedelta
from selenium.webdriver.common.by import By

from marketdata_collector.comm_tools.logger import log_progress
from marketdata_collector.comm_tools.database_mysql import load_to_MySQL_on_Cloud, run_query
from marketdata_collector.comm_tools.database_mysql import open_mysql
from marketdata_collector.comm_tools.config import Config
from marketdata_collector.comm_tools.selenium import open_chrome


def extract(sse_webpage):
    """
    This function aims to extract the last trading date
    from the website and check if it is yesterday.

    :param sse_webpage: set to the page of SSE.
    :return: return a date if it is trading day.
            return None if it is not.
    """
    log_progress("Start to extract date from SSE page.")
    with open_chrome() as driver:
        log_progress("Step 1/2. Loading webpage...")
        driver.get(sse_webpage)  # 加载页面
        time.sleep(1)

        log_progress("Step 2/2. Retrieving data from the table...")
        # locate the table in the page
        node = driver.find_element(By.CLASS_NAME, "new_date")
        new_date = node.text

        # retrieve date from str
        date_str = new_date[5:]
        date_list = date_str.split("-")
        day = date(int(date_list[0]), int(date_list[1]), int(date_list[2]))

        yesterday = date.today() - timedelta(days=1)
        if yesterday == day:
            return yesterday

        return None

def execute():
    """
    Every morning, we need to visit SSE page to check if yesterday was
    a trading day. If it is, then save that date to database.

    :return: none
    """

    """  读取参数  """
    # 创建 ConfigParser 对象
    c = Config()

    """  从交易所首页抓取数据  """
    date = extract(c.sse_vol_stc_d)
    print(date)

    """  验证数据是否完整  """
    if date:
        df_date = pd.DataFrame({'Date': [date]})
        """  将抓取的数据存入数据库  """
        with open_mysql(c) as engine:
            # 将 DataFrame 写入 MySQL
            load_to_MySQL_on_Cloud(df_date, engine, c.table_trading_days)

    """  从数据库读取数据并打印在控制台  """
    # Q3 = f"SELECT Market_Type from {table_name} LIMIT 5"
    # df_retrieved = run_query(Q3, engine)
    # print(df_retrieved)


