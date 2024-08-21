from contextlib import contextmanager
from selenium import webdriver


@contextmanager
def open_chrome():
    try:
        # 设置浏览器为谷歌浏览器
        driver = webdriver.Chrome()
        driver.implicitly_wait(10)
        driver.maximize_window()
        yield driver
    finally:
        if driver is not None:
            # 释放驱动
            driver.close()
