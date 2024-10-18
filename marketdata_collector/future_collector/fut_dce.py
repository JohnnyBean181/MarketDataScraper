import time
import configparser
import os
import pdfplumber
import urllib.parse
from enum import Enum
from re import split
from datetime import date as getdate

import pandas as pd
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By

from marketdata_collector.comm_tools.logger import log_progress
from marketdata_collector.comm_tools.data_tool import verify_fut, transform
from marketdata_collector.comm_tools.database_mysql import load_to_MySQL_on_Cloud, run_query
from marketdata_collector.comm_tools.database_mysql import open_mysql
from marketdata_collector.comm_tools.config import Config
from marketdata_collector.comm_tools.selenium import open_firefox
from marketdata_collector.comm_tools.selenium import get_dl_dir

class DataType(Enum):
    AMOUNT = 1
    VOLUME = 2
    POSITION = 3

def pick_up_data(tables, type):
    if type == DataType.AMOUNT:
        table = tables[0]
        for row in table:
            if "期货小计" in row[0]:
                return row[8]
    elif type == DataType.VOLUME:
        table = tables[0]
        for row in table:
            if "期货小计" in row[0]:
                return row[1]
    elif type == DataType.POSITION:
        table = tables[2]
        for row in table:
            if "期货小计" in row[0]:
                return row[1]

    return None

def get_data_from_pdf(filename):
    file_path = str(os.path.join(get_dl_dir(), filename))

    # 打开 PDF 文件
    with pdfplumber.open(file_path) as pdf:
        # 选择要提取表格的具体页面
        page = pdf.pages[0]  # 选择第一页，索引从0开始

        # 使用 extract_tables 方法提取所有表格
        tables = page.extract_tables()

    return tables

def get_name(filename_raw):
    filename = os.path.basename(filename_raw)
    decoded_filename = urllib.parse.unquote(filename)
    return decoded_filename

class VolumeDict:
    def __init__(self, market_type):
        self.data = dict()
        self.data["Market_Type"] = market_type

    def set_date(self, date):
        self.data["Date"] = date

    def set_volume_m(self, volume_m):
        val_cleaned = volume_m.replace(",", "")
        self.data["Volume_Month"] = float(val_cleaned)

    def set_amount_m(self, amount_m):
        val_cleaned = amount_m.replace(",", "")
        self.data["Amount_Month"] = float(val_cleaned)

    def set_position_m(self, position_m):
        val_cleaned = position_m.replace(",", "")
        self.data["Position_Month"] = float(val_cleaned)

    def get_df(self):
        return pd.DataFrame(self.data, index=[0])

def find_trade_data_from_web(driver, webpage):
    log_progress("Step 1/3. Loading webpage vol ...")
    driver.get(webpage)  # 加载页面
    time.sleep(1)

    log_progress("Step 2/3. Verify the target month...")
    # Check if data is up-to-date
    # TODO

    log_progress("Step 3/3. Download the Excel file...")
    # download excel from which to get data
    file_list = driver.find_element(by=By.CLASS_NAME, value="list_tpye06")
    links = file_list.find_elements(by=By.TAG_NAME, value="a")
    filename_raw = None
    for link in links:
        if '2024' in link.text and '9' in link.text:
            link.click()
            filename_raw = link.get_attribute('href')
            time.sleep(5)
            print("file downloaded.")
            break

    # if target file not found, return None
    if filename_raw is None:
        return None

    log_progress("Step 4/3. Pick up data from Excel...")
    filename = get_name(filename_raw)

    tables = get_data_from_pdf(filename)
    amount = pick_up_data(tables, DataType.AMOUNT)
    volume = pick_up_data(tables, DataType.VOLUME)
    position = pick_up_data(tables, DataType.POSITION)

    return amount,volume,position

def extract(c):
    """
    This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing.

    :param dce_webpage: set to the main page of dzce as default.
    :return: return a list, which contains two row data.
    """
    log_progress("Start to extract monthly trading data from DCE webpage.")
    with open_firefox() as driver:
        # create a new dict
        data_dict = VolumeDict("DCE")
        data_dict.set_date(getdate(2024,9,30))

        # save data into VolumeDict
        amount,volume,position = find_trade_data_from_web(driver, c.dce_fut_m)
        data_dict.set_amount_m(amount)
        data_dict.set_volume_m(volume)
        data_dict.set_position_m(position)

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
    if verify_fut(df_transformed):
        #  将抓取的数据存入数据库  
        with open_mysql(c) as engine:
            # 将 DataFrame 写入 MySQL
            load_to_MySQL_on_Cloud(df_transformed, engine, c.table_fut)
    """  从数据库读取数据并打印在控制台  """
    # Q3 = f"SELECT Market_Type from {table_name} LIMIT 5"
    # df_retrieved = run_query(Q3, engine)
    # print(df_retrieved)
