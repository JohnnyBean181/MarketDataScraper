import time
import configparser
from re import split
from datetime import date as get_date

import pandas as pd
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

from marketdata_collector.comm_tools.logger import log_progress
from marketdata_collector.comm_tools.data_tool import verify_bse_vol, transform, get_last_day_of_previous_month
from marketdata_collector.comm_tools.data_tool import get_last_trading_day_of_previous_month
from marketdata_collector.comm_tools.database_mysql import load_to_MySQL_on_Cloud, run_query
from marketdata_collector.comm_tools.database_mysql import open_mysql
from marketdata_collector.comm_tools.config import Config
from marketdata_collector.comm_tools.selenium import open_chrome

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

    def set_overall_m(self):
        self.data["Overall_Vol_Month"] = (self.data["Stock_Vol_Month"]
                                          + self.data["Bond_Vol_Month"])

    def set_mrg(self, margin1, margin2):
        self.data["Margin1"] = margin1
        self.data["Margin2"] = margin2

    def get_df(self):
        return pd.DataFrame(self.data, index=[0])

def select_date(driver, date_input_box, date):
    date_input_box.click()
    time.sleep(0.5)

    table = driver.find_element(by=By.CLASS_NAME, value="table-condensed")
    month_select = table.find_element(by=By.CLASS_NAME, value="monthselect")
    m_select = Select(month_select)
    m_select.select_by_value(str(date.month-1))
    time.sleep(0.5)

    year_select = driver.find_element(by=By.CLASS_NAME, value="yearselect")
    y_select = Select(year_select)
    y_select.select_by_value(str(date.year))
    time.sleep(0.5)

    table = driver.find_element(by=By.CLASS_NAME, value="table-condensed")
    tbody = table.find_element(by=By.TAG_NAME, value="tbody")
    rows = tbody.find_elements(by=By.TAG_NAME, value="tr")
    start = False
    for row in rows:
        tds = row.find_elements(by=By.TAG_NAME, value="td")
        for td in tds:
            if td.text == '1':
                start = True
            if start and td.text == str(date.day):
                td.click()
                return

def find_vol_from_web(driver, webpage, date):
    log_progress("Step 1/3. Loading webpage vol ...")
    driver.get(webpage)  # 加载页面
    time.sleep(1)

    # switch to data by "month"
    time_bar = driver.find_element(by=By.ID, value="ulTab")
    items = time_bar.find_elements(By.TAG_NAME, "li")
    items[2].click()
    time.sleep(1)

    # unfold trading table
    link = driver.find_element(by=By.XPATH, value="//*[@id='accordion1']/div[2]")
    link.click()
    time.sleep(1)

    log_progress("Step 2/3. Verify the target month...")
    # Check if data is up-to-date
    # TODO

    log_progress("Step 3/3. Retrieving data from the table...")
    # get data from trading table
    vol_table = driver.find_element(by=By.ID, value="neeqDeal")
    rows = vol_table.find_elements(by=By.TAG_NAME, value="tr")
    row = rows[-2]
    tds = row.find_elements(by=By.TAG_NAME, value="td")
    if tds[0].text == date[:6]:
        return tds[2].text

    return None

def find_bond_vol_from_web(driver, webpage, date):
    log_progress("Step 1/2. Loading webpage vol ...")
    driver.get(webpage)  # 加载页面
    time.sleep(1)

    # switch to bond market
    #link = driver.find_element(by=By.XPATH, value="//*[@id='root']/div[4]/div/div/div[2]/div/div/ul/li[4]/ul/li[2]/ul/li[2]/span/a")
    #link.click()
    #time.sleep(1)

    bond_table = driver.find_element(by=By.CSS_SELECTOR, value=".bg-bai.monthReport")
    rows = bond_table.find_elements(by=By.TAG_NAME, value="tr")
    for row in rows:
        tds = row.find_elements(by=By.TAG_NAME, value="td")
        if len(tds) > 0 and tds[0].text == date[:6]:
            return tds[2].text
        continue

    return None

def find_mrg_from_web(driver, webpage, date):
    log_progress("Step 1/2. Loading webpage vol ...")
    driver.get(webpage)  # 加载页面
    time.sleep(1)

    # enter date in "date input" bar
    date_input_box = driver.find_element(by=By.ID, value="date")
    select_date(driver, date_input_box, get_last_trading_day_of_previous_month())

    #text_input.clear()
    #text_input.send_keys("2024-11-29")
    time.sleep(1)

    # then click on "select button"
    select_btn = driver.find_element(by=By.ID, value="submit")
    select_btn.click()
    time.sleep(1)

    log_progress("Step 2/2. Retrieving data from the table...")
    # locate the table in the page
    table = driver.find_element(By.ID, "table")
    tbody = table.find_element(By.ID, "summaryData")
    # 读取表格中的数据，每一个tr中包含一个数据
    row = tbody.find_element(By.TAG_NAME, "tr")
    tds = row.find_elements(By.TAG_NAME, 'td')
    return tds[1].text, tds[4].text

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
        # create VolumeDict
        data_dict = VolumeDict("北证")

        # setup date
        last_day_of_previous_month = get_last_day_of_previous_month()
        data_dict.set_date(last_day_of_previous_month)

        # find 'stock volume' and save it into 'VolumeDict'
        date_str = str(last_day_of_previous_month).replace('-', '')
        stock_m = find_vol_from_web(driver, c.bse_vol_stc_m, date_str)
        data_dict.set_stock_m(stock_m)

        # find 'bond volume' and save it into 'VolumeDict'
        bond_m = find_bond_vol_from_web(driver, c.bse_vol_bnd_m, date_str)
        data_dict.set_bond_m(bond_m)

        # compute 'overall volume' and save it into 'VolumeDict'
        data_dict.set_overall_m()

        # find 'margin volume' and save it into 'VolumeDict'
        mrg1, mrg2 = find_mrg_from_web(driver, c.bse_vol_mrg, date_str)
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
    if verify_bse_vol(df_transformed):
        #  将抓取的数据存入数据库  
        with open_mysql(c) as engine:
            # 将 DataFrame 写入 MySQL
            load_to_MySQL_on_Cloud(df_transformed, engine, c.table_vol)
    """  从数据库读取数据并打印在控制台  """
    # Q3 = f"SELECT Market_Type from {table_name} LIMIT 5"
    # df_retrieved = run_query(Q3, engine)
    # print(df_retrieved)
