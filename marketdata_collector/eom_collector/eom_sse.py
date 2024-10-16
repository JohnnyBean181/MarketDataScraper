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


def extract(sse_webpage):
    """
    This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing.

    :param sse_webpage: set to the main page of SSE as default.
    :return: return a list, which contains two row data.
    """
    log_progress("Start to extract data from SSE in main page.")
    with open_chrome() as driver:
        log_progress("Step 1/6. Loading webpage...")
        driver.get(sse_webpage)  # 加载页面

        log_progress("Step 2/6. Scroll down to locate the table...")
        # 查找上交所首页中的数据表格，该表格需要等待下拉后才能加载
        data_list = driver.find_element(By.CLASS_NAME, "sse_market_data_con")
        ActionChains(driver).scroll_to_element(data_list).perform()
        time.sleep(2)  # 等待2秒，用于加载网页

        log_progress("Step 3/6. Retrieving data from the table...")
        # 读取表格中的数据，每一个li中包含一个数据
        data_rows = data_list.find_elements(By.TAG_NAME, "li")
        # 数据格式规范化
        data_dict = transform(data_rows, "沪市")
        df = pd.DataFrame(data_dict, index=[0])

        log_progress("Step 4/6. Switching to tab of MarketStar...")
        # 选取表格面板中的“科创板”按钮，点击后，等待2秒以加载“科创板”数据
        nav_tabs = driver.find_element(by=By.ID, value="nav_tabs")
        tabs = nav_tabs.find_elements(by=By.TAG_NAME, value="span")
        tabs[2].click()
        time.sleep(2)

        log_progress("Step 5/6. Retrieving data of MarketStar...")
        # 读取“科创板”数据
        data_list2 = driver.find_element(By.XPATH, '//*[@id="tab_main"]/div[3]')
        data_rows2 = data_list2.find_elements(By.TAG_NAME, "li")
        # 数据格式规范化
        data_dict = transform(data_rows2, "科创板")
        df_1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df, df_1], ignore_index=True)

        log_progress("Step 6/6. Data extraction complete...")

    return df


def execute():
    """
    EOM stands for End of Month.
    Although the name has EOM in it, this script should be executed on the
    first day of every month, no mater it's holiday or weekend day.

    :return: none
    """

    """  读取参数  """
    # 创建 ConfigParser 对象
    c = Config()

    """  从交易所首页抓取数据  """
    df_transformed = extract(c.url_sse)
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


