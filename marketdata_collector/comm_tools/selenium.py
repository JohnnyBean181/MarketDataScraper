import os
from contextlib import contextmanager
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options as FFOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.firefox.service import Service as FFService
from webdriver_manager.chrome import ChromeDriverManager


def get_dl_dir():
    download_dir = os.path.join(os.getcwd(), 'downloads')  # 指定下载文件的目录
    os.makedirs(download_dir, exist_ok=True)  # 如果目录不存在则创建
    return download_dir

def get_chrome_option(download_dir):
    # 设置 Chrome 下载选项
    chrome_options = Options()
    """
    chrome_options.add_experimental_option('prefs', {
        "download.default_directory": download_dir,  # 指定下载目录
        "download.prompt_for_download": False,  # 不弹出下载窗口
        "download.directory_upgrade": True,  # 启用此选项以确保覆盖默认下载目录
    })"""
    chrome_options.add_argument("--ignore-certificate-errors")  # 忽略证书错误
    chrome_options.add_argument("--allow-insecure-localhost")  # 允许访问不安全的本地网址
    chrome_options.add_argument("--disable-web-security")  # 禁用浏览器的安全性
    # chrome_options.add_argument("--headless")  # 设置为无头模式
    return chrome_options

def get_firefox_option(download_dir):
    # 创建一个新的 Firefox 配置文件
    profile_path = os.path.expanduser("~/.selenium_firefox_profile")  # 设置配置文件路径
    os.makedirs(profile_path, exist_ok=True)  # 如果文件夹不存在，则创建它

    # 创建 Firefox 配置文件并设置下载选项
    profile = webdriver.FirefoxProfile(profile_path)
    profile.set_preference("browser.download.folderList", 2)  # 2: 使用自定义下载路径
    profile.set_preference("browser.download.dir", download_dir)  # 替换为您希望的下载目录
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")  # 比如 xlsx 文件
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk","application/pdf") # 比如 pdf 文件
    profile.set_preference("pdfjs.disabled", True)  # 如果您在处理 PDF 下载，禁用内置 PDF 查看器
    profile.set_preference("browser.download.manager.showWhenStarting", False)  # 不显示下载管理器

    # 创建 Firefox 选项
    firefox_options = FFOptions()
    firefox_options.profile = profile  # 使用设置的配置文件

    firefox_options.add_argument("--ignore-certificate-errors")  # 忽略证书错误
    firefox_options.add_argument("--allow-insecure-localhost")  # 允许访问不安全的本地网址
    firefox_options.add_argument("--disable-web-security")  # 禁用浏览器的安全性
    # firefox_options.add_argument("--headless")  # 设置为无头模式
    return firefox_options


@contextmanager
def open_chrome():
    # 设定下载路径
    download_dir = get_dl_dir()

    chrome_service = Service(executable_path=r'./chromedriver')
    chrome_options = get_chrome_option(download_dir)

    try:
        # 设置浏览器为谷歌浏览器
        driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
        driver.implicitly_wait(10)
        driver.maximize_window()
        yield driver
    finally:
        if driver is not None:
            # 释放驱动
            driver.close()


@contextmanager
def open_firefox():
    # 设定下载路径
    download_dir = get_dl_dir()

    firefox_service = FFService(executable_path='geckodriver')
    firefox_options = get_firefox_option(download_dir)

    try:
        # 设置浏览器为firefox浏览器
        driver = webdriver.Firefox(service=firefox_service, options=firefox_options)
        driver.implicitly_wait(10)
        driver.maximize_window()
        yield driver
    finally:
        if driver is not None:
            # 释放驱动
            driver.close()
