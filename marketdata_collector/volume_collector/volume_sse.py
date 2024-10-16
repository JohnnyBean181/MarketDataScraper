import time
import configparser
from re import split
from datetime import date as getdate

import pandas as pd
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By

from marketdata_collector.comm_tools.logger import log_progress
from marketdata_collector.comm_tools.data_tool import verify_vol, transform
from marketdata_collector.comm_tools.database_mysql import load_to_MySQL_on_Cloud, run_query
from marketdata_collector.comm_tools.database_mysql import open_mysql
from marketdata_collector.comm_tools.config import Config
from marketdata_collector.comm_tools.selenium import open_chrome


def margin_fixed(margin):
    cleaned_string = margin.replace(",", "")
    return int(cleaned_string)/10000


class VolumeDict:
    def __init__(self, market_type):
        self.data = dict()
        self.data["Market_Type"] = market_type

    def set_date(self, date):
        self.data["Date"] = date

    def set_stock_m(self, stock_m):
        self.data["Stock_Vol_Month"] = float(stock_m)

    def set_fund_m(self, fund_m):
        self.data["Fund_Vol_Month"] = float(fund_m)

    def set_bond_m(self, bond_m):
        self.data["Bond_Vol_Month"] = float(bond_m)/10000

    def set_mrg(self, margin1, margin2):
        self.data["Margin1"] = margin_fixed(margin1)
        self.data["Margin2"] = margin_fixed(margin2)

    def get_df(self):
        return pd.DataFrame(self.data, index=[0])

def find_vol_from_web(driver, webpage, tr, td):
    log_progress("Step 1/3. Loading webpage vol ...")
    driver.get(webpage)  # 加载页面
    time.sleep(1)

    log_progress("Step 2/3. Verify the target month...")
    # Check if data is up-to-date
    # TODO

    log_progress("Step 3/3. Retrieving data from the table...")
    # locate the table in the page
    table = driver.find_element(By.CLASS_NAME, "table-responsive")
    tbody = table.find_element(By.TAG_NAME, "tbody")
    # 读取表格中的数据，每一个tr中包含一个数据
    rows = tbody.find_elements(By.TAG_NAME, "tr")
    row = rows[tr]
    tds = row.find_elements(By.TAG_NAME, 'td')
    td_text = tds[td].text

    return td_text

def find_mrg_from_web(driver, webpage, date):
    log_progress("Step 1/2. Loading webpage vol ...")
    driver.get(webpage)  # 加载页面
    time.sleep(1)

    log_progress("Step 2/2. Retrieving data from the table...")
    # locate the table in the page
    table = driver.find_element(By.CLASS_NAME, "table-responsive")
    tbody = table.find_element(By.TAG_NAME, "tbody")
    # 读取表格中的数据，每一个tr中包含一个数据
    rows = tbody.find_elements(By.TAG_NAME, "tr")
    for row in rows:
        tds = row.find_elements(By.TAG_NAME, 'td')
        if tds[0].text == date:
            return tds[1].text, tds[4].text
        continue

    return None


def extract(c):
    """
    This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing.

    :param sse_webpage: set to the main page of SSE as default.
    :return: return a list, which contains two row data.
    """
    log_progress("Start to extract monthly vol data from SSE webpage.")
    with open_chrome() as driver:
        stock_m = find_vol_from_web(driver, c.sse_vol_stc_m, 3, 1)
        # save data into VolumeDict
        data_dict = VolumeDict("沪市")
        data_dict.set_date(getdate(2024,9,30))
        data_dict.set_stock_m(stock_m)

        fund_m = find_vol_from_web(driver, c.sse_vol_fnd_m, 1, 1)
        # save data into VolumeDict
        data_dict.set_fund_m(fund_m)

        bond_m = find_vol_from_web(driver, c.sse_vol_bnd_m, -1, 2)
        # save data into VolumeDict
        data_dict.set_bond_m(bond_m)

        mrg1, mrg2 = find_mrg_from_web(driver, c.sse_vol_mrg, "20240930")
        # save data into VolumeDict
        data_dict.set_mrg(mrg1, mrg2)

        log_progress("Data extraction complete...")

    return data_dict.get_df()


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
    if verify_vol(df_transformed):
        #  将抓取的数据存入数据库  
        with open_mysql(c) as engine:
            # 将 DataFrame 写入 MySQL
            load_to_MySQL_on_Cloud(df_transformed, engine, c.table_vol)

    """  从数据库读取数据并打印在控制台  """
    # Q3 = f"SELECT Market_Type from {table_name} LIMIT 5"
    # df_retrieved = run_query(Q3, engine)
    # print(df_retrieved)
