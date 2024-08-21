import configparser
from sqlalchemy import create_engine
from openpyxl import load_workbook
from MD_AUTO.comm_tools.database_mysql import run_query


def rename_cols(df):
    e_name = ['Company_Num', 'Market_Value', 'Circulation_Market_Value',
                    'AVG_PE', 'Date', 'Market_Type']
    c_name = ['上市公司数（家）', '股票市值（亿元）', '流通市值（亿元）',
                    'PE', '日期', '市场']
    df.rename(columns={e_name[0]: c_name[0],
                       e_name[1]: c_name[1],
                       e_name[2]: c_name[2],
                       e_name[3]: c_name[3],
                       e_name[4]: c_name[4],
                       e_name[5]: c_name[5]}, inplace=True)

def reorder_cols(df):
    new_order = ['Date', 'Market_Type', 'Company_Num', 'Market_Value',
                 'Circulation_Market_Value', 'AVG_PE']
    df = df.reindex(columns=new_order)
    return df


def get_eom(day):
    # 创建 ConfigParser 对象
    config = configparser.ConfigParser()
    # 读取 ini 文件
    config.read('config.ini')
    # 提取数据库连接信息
    user = config['database']['user']
    password = config['database']['password']
    host = config['database']['host']
    port = config['database']['port']
    database = config['database']['database']
    table_name = config['database']['table_name']

    # 创建 SQLAlchemy 引擎
    connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    Q3 = f"SELECT * from {table_name} WHERE Date='{day}' "
    df_retrieved = run_query(Q3, engine)
    df_retrieved = reorder_cols(df_retrieved)
    rename_cols(df_retrieved)
    df_retrieved.to_excel('output.xlsx', index=False)

    # 加载已经存在的Excel文件
    wb = load_workbook('output.xlsx')
    # 获取当前活动的工作表
    sheet = wb.active

    # 设置每列的列宽为自动调整
    for col in sheet.columns:
        sheet.column_dimensions[col[0].column_letter].auto_size = True

    # 保存工作簿
    wb.save('output.xlsx')