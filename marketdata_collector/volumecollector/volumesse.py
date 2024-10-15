import time
import configparser
import pandas as pd
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By

from marketdata_collector.comm_tools.logger import log_progress
from marketdata_collector.comm_tools.data_tool import verify, transform
from marketdata_collector.comm_tools.database_mysql import load_to_MySQL_on_Cloud, run_query
from marketdata_collector.comm_tools.database_mysql import open_mysql
from marketdata_collector.comm_tools.config import Config
from marketdata_collector.comm_tools.selenium import open_chrome


def execute():
    """
    Get volume data from SSE webpage.
    Data includes stock, fund, bond, and margin data.
    :return: none
    """

    """  loading configure data  """
    # 创建 ConfigParser 对象
    c = Config()

    """  从交易所首页抓取数据  """
    df_transformed = extract(c)
    print(df_transformed)

    """  验证数据是否完整  """
    if verify(df_transformed):
        """  将抓取的数据存入数据库  """
        with open_mysql(c) as engine:
            # 将 DataFrame 写入 MySQL
            load_to_MySQL_on_Cloud(df_transformed, engine, c.table_name)

    """  从数据库读取数据并打印在控制台  """
    # Q3 = f"SELECT Market_Type from {table_name} LIMIT 5"
    # df_retrieved = run_query(Q3, engine)
    # print(df_retrieved)