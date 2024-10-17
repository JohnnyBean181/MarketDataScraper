import time
import configparser
from re import split
from datetime import date as getdate

import pandas as pd
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By

from marketdata_collector.comm_tools.logger import log_progress
from marketdata_collector.comm_tools.data_tool import verify_bse_vol, transform
from marketdata_collector.comm_tools.database_mysql import load_to_MySQL_on_Cloud, run_query
from marketdata_collector.comm_tools.database_mysql import open_mysql
from marketdata_collector.comm_tools.config import Config
from marketdata_collector.comm_tools.selenium import open_chrome


def margin_fixed(margin):
    cleaned_string = margin.replace(",", "")
    return float(cleaned_string)*10000


class VolumeDict:
    def __init__(self, market_type):
        self.data = dict()
        self.data["Market_Type"] = market_type

    def set_date(self, date):
        self.data["Date"] = date

    def set_stock_m(self, stock_m):
        val_cleaned = stock_m.replace(",", "")
        self.data["Stock_Vol_Month"] = float(val_cleaned)

    def set_bond_m(self, bond_m):
        val_cleaned = bond_m.replace(",", "")
        self.data["Bond_Vol_Month"] = float(val_cleaned)/10000

    def get_df(self):
        return pd.DataFrame(self.data, index=[0])

def find_vol_from_web(driver, webpage):
    log_progress("Step 1/3. Loading webpage vol ...")
    driver.get(webpage)  # 加载页面
    time.sleep(1)

    # switch to data by "month"
    time_bar = driver.find_element(by=By.ID, value="ulTab")
    items = time_bar.find_elements(By.TAG_NAME, "li")
    items[2].click()
    time.sleep(1)

    # unfold trading table
    link = driver.find_element(by=By.XPATH, value="//*[@id='accordion1']/div[2]/div[1]/h4/a")
    link.click()
    time.sleep(1)

    log_progress("Step 2/3. Verify the target month...")
    # Check if data is up-to-date
    # TODO

    log_progress("Step 3/3. Retrieving data from the table...")
    # get data from trading table
    vol_table = driver.find_element(by=By.ID, value="neeqDeal")
    rows = vol_table.find_elements(by=By.TAG_NAME, value="tr")
    for row in rows:
        tds = row.find_elements(by=By.TAG_NAME, value="td")
        if len(tds) > 0 and tds[0].text == "202409":
            return tds[2].text
        continue

    return None

def find_bond_vol_from_web(driver, webpage):
    log_progress("Step 1/2. Loading webpage vol ...")
    driver.get(webpage)  # 加载页面
    time.sleep(1)

    # switch to bond market
    link = driver.find_element(by=By.XPATH, value="//*[@id='root']/div[4]/div/div/div[2]/div/div/ul/li[4]/ul/li[2]/ul/li[2]/span/a")
    link.click()
    time.sleep(1)

    bond_table = driver.find_element(by=By.CSS_SELECTOR, value=".bg-bai.monthReport")
    rows = bond_table.find_elements(by=By.TAG_NAME, value="tr")
    for row in rows:
        tds = row.find_elements(by=By.TAG_NAME, value="td")
        if len(tds) > 0 and tds[0].text == "202409":
            return tds[2].text
        continue

    return None

def extract(c):
    """
    This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing.

    :param szse_webpage: set to the main page of SZSE as default.
    :return: return a list, which contains two row data.
    """
    log_progress("Start to extract monthly vol data from BSE webpage.")
    with open_chrome() as driver:
        stock_m = find_vol_from_web(driver, c.bse_vol_stc_m)
        # save data into VolumeDict
        data_dict = VolumeDict("北证")
        data_dict.set_date(getdate(2024,9,30))
        data_dict.set_stock_m(stock_m)

        bond_m = find_bond_vol_from_web(driver, c.bse_vol_bnd_m)
        # save data into VolumeDict
        data_dict.set_bond_m(bond_m)

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
    if verify_bse_vol(df_transformed):
        #  将抓取的数据存入数据库  
        with open_mysql(c) as engine:
            # 将 DataFrame 写入 MySQL
            load_to_MySQL_on_Cloud(df_transformed, engine, c.table_vol)
    """  从数据库读取数据并打印在控制台  """
    # Q3 = f"SELECT Market_Type from {table_name} LIMIT 5"
    # df_retrieved = run_query(Q3, engine)
    # print(df_retrieved)
