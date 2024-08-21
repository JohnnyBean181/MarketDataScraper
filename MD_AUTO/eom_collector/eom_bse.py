import time
import configparser
import pandas as pd
from sqlalchemy import create_engine, false
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from MD_AUTO.comm_tools.logger import log_progress
from MD_AUTO.comm_tools.database_mysql import load_to_MySQL_on_Cloud, run_query
from MD_AUTO.comm_tools.data_tool import verify, transform
from MD_AUTO.comm_tools.config import Config


def extract(bse_webpage, bse_webpage2):
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
    data_dict = transform(data_rows, "北证")

    log_progress("Step 4/4. Loading webpage 2...")
    driver.get(bse_webpage2)  # 加载页面
    driver.maximize_window()

    log_progress("Step 5/6. Switching to tab of MarketStar...")
    # 选取表格面板中的“月报”按钮，点击后，等待3秒以加载“月报”数据
    nav_panel = driver.find_element(by=By.ID, value="ulTab")
    tabs = nav_panel.find_elements(by=By.TAG_NAME, value="li")
    tabs[2].click()
    time.sleep(3)

    log_progress("Step 5/6. Retrieving data of MarketStar...")
    # 读取“市盈率”数据
    table = driver.find_element(By.ID, 'neeqStatistical')
    trs = table.find_elements(By.TAG_NAME, 'tr')
    tr = trs[-2]
    tds = tr.find_elements(By.TAG_NAME, 'td')
    td_text = tds[3].text

    log_progress("Step 5/6. Retrieving data of MarketStar...")
    data_dict["AVG_PE"] = float(td_text.strip())
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
    c = Config()

    """  从交易所首页抓取数据  """
    df_transformed = extract(c.url_bse, c.url_bse2)
    print(df_transformed)

    """  验证数据是否完整  """
    if verify(df_transformed):
        """  将抓取的数据存入数据库  """
        # 创建 SQLAlchemy 引擎
        connection_string = (f"mysql+mysqlconnector://{c.user}:{c.password}"
                             f"@{c.host}:{c.port}/{c.database}")
        engine = create_engine(connection_string)

        # 将 DataFrame 写入 MySQL
        load_to_MySQL_on_Cloud(df_transformed, engine, c.table_name)

    """  从数据库读取数据并打印在控制台  """
    # Q3 = f"SELECT Market_Type from {table_name}"
    # df_retrieved = run_query(Q3, engine)
    # print(df_retrieved)