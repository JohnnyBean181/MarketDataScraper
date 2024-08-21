import configparser


class Config:
    def __init__(self):
        # 创建 ConfigParser 对象
        config = configparser.ConfigParser()
        # 读取 ini 文件
        config.read('config.ini')
        # 提取数据库连接信息
        self.user = config['database']['user']
        self.password = config['database']['password']
        self.host = config['database']['host']
        self.port = config['database']['port']
        self.database = config['database']['database']
        self.table_name = config['database']['table_name']
        # 提取常规数据，如网页等
        self.url_sse = config['DEFAULT']['url_sse'] # 上交所网页
        self.url_szse = config['DEFAULT']['url_szse'] # 深交所网页
        self.url_bse = config['DEFAULT']['url_bse'] # 北交所网页1
        self.url_bse2 = config['DEFAULT']['url_bse2']#  北交所网页2
