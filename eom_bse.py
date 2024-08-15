import time
import configparser
import pandas as pd
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from logger import log_progress
from datetime import date
from database_mysql import load_to_MySQL_on_Cloud, run_query


def transform(data_rows, data_type: str) -> dict:
    """
    This function receive raw data from webpage, and transforms
    data into int or floats, and adds Date to the Dict.

    :param data_rows: raw data in rows.
    :return: dict
    """
    log_progress(f"Start to transform data for {data_type}.")
    data_dict = dict()

    for row in data_rows:
        entry = row.text.strip().split('\n')
        if "上市公司" in entry[0].strip():
            print(entry[0].strip())
            print(entry[1].strip())
            data_dict["Company_Num"] = int(entry[1].strip())
        elif "总市值" in entry[0].strip():
            data_dict["Market_Value"] = float(entry[1].strip())
        elif "流通市值" in entry[0].strip():
            data_dict["Circulation_Market_Value"] = float(entry[1].strip())
        elif "平均市盈率" in entry[0].strip():
            data_dict["AVG_PE"] = float(entry[1].strip())
    data_dict["Date"] = date.today()
    data_dict["Market_Type"] = data_type

    log_progress("Data transformation complete.")
    return data_dict


def extract(bse_webpage):
    """
    This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing.

    :param sse_webpage: set to the main page of SSE as default.
    :return: return a list, which contains two row data.
    """
    log_progress("Start to extract data from BSE in main page.")
    log_progress("Step 1/4. Loading webpage...")
    driver = webdriver.Chrome()  # 设置浏览器为谷歌浏览器
    driver.implicitly_wait(10)
    driver.get(bse_webpage)  # 加载页面
    driver.maximize_window()

    log_progress("Step 2/4. Scroll down to locate the table...")
    # 查找上交所首页中的数据表格，该表格需要等待下拉后才能加载
    data_list = driver.find_element(By.CLASS_NAME, "col-sm-5")
    ActionChains(driver).scroll_to_element(data_list).perform()
    time.sleep(2)  # 等待2秒，用于加载网页

    log_progress("Step 3/4. Retrieving data from the table...")
    # 读取表格中的数据，每一个div中包含一个数据
    data_rows = data_list.find_elements(By.CLASS_NAME, "market_info_detail")
    # 数据格式规范化
    data_dict = transform(data_rows, "北交所")
    df = pd.DataFrame(data_dict, index=[0])

    log_progress("Step 4/4. Data extraction complete...")
    driver.close()

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
    config = configparser.ConfigParser()
    # 读取 ini 文件
    config.read('config.ini')
    # 提取常规数据，如网页等
    url_bse = config['DEFAULT']['url_bse']
    # 提取数据库连接信息
    user = config['database']['user']
    password = config['database']['password']
    host = config['database']['host']
    port = config['database']['port']
    database = config['database']['database']
    table_name = config['database']['table_name']

    """  从交易所首页抓取数据  """
    df_transformed = extract(url_bse)
    print(df_transformed)

    """  将抓取的数据存入数据库  """
    # 创建 SQLAlchemy 引擎
    connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    # 将 DataFrame 写入 MySQL
    load_to_MySQL_on_Cloud(df_transformed, engine, table_name)

    """  从数据库读取数据并打印在控制台  """
    Q3 = f"SELECT Market_Type from {table_name}"
    df_retrieved = run_query(Q3, engine)
    print(df_retrieved)