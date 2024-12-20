from datetime import date, timedelta
from marketdata_collector.comm_tools.logger import log_progress
from marketdata_collector.comm_tools.config import Config
from marketdata_collector.comm_tools.database_mysql import open_mysql, run_query


def verify(df):
    cols = ['Company_Num', 'Market_Value', 'Circulation_Market_Value', 'AVG_PE']
    for col in cols:
        # 是否存在这个column列
        if not col in df.columns:
            log_progress(f"Data verification error, {col} not found.")
            print(f"Data verification error, {col} not found.")
            return False
        # 该列中的数据是否为有效数据，都大于零
        if not (df[col] > 0).all():
            log_progress(f"Data verification error, {col} value invalid.")
            print(f"Data verification error, {col} value invalid.")
            return False

    log_progress("Data verification complete.")
    return True


def verify_vol(df):
    cols = ['Stock_Vol_Month', 'Fund_Vol_Month', 'Bond_Vol_Month',
            'Margin1', 'Margin2']
    for col in cols:
        # 是否存在这个column列
        if not col in df.columns:
            log_progress(f"Data verification error, {col} not found.")
            print(f"Data verification error, {col} not found.")
            return False
        # 该列中的数据是否为有效数据，都大于零
        if not (df[col] > 0).all():
            log_progress(f"Data verification error, {col} value invalid.")
            print(f"Data verification error, {col} value invalid.")
            return False

    log_progress("Data verification complete.")
    return True


def verify_bse_vol(df):
    cols = ['Stock_Vol_Month', 'Bond_Vol_Month']
    for col in cols:
        # 是否存在这个column列
        if not col in df.columns:
            log_progress(f"Data verification error, {col} not found.")
            print(f"Data verification error, {col} not found.")
            return False
        # 该列中的数据是否为有效数据，都大于等于零
        if not (df[col] >= 0).all():
            log_progress(f"Data verification error, {col} value invalid.")
            print(f"Data verification error, {col} value invalid.")
            return False

    log_progress("Data verification complete.")
    return True

def verify_fut(df):
    cols = ['Amount_Month', 'Volume_Month', 'Position_Month']
    for col in cols:
        # 是否存在这个column列
        if not col in df.columns:
            log_progress(f"Data verification error, {col} not found.")
            print(f"Data verification error, {col} not found.")
            return False
        # 该列中的数据是否为有效数据，都大于等于零
        if not (df[col] >= 0).all():
            log_progress(f"Data verification error, {col} value invalid.")
            print(f"Data verification error, {col} value invalid.")
            return False

    log_progress("Data verification complete.")
    return True


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
            data_dict["Company_Num"] = int(entry[1].strip())
        elif "总市值" in entry[0].strip():
            data_dict["Market_Value"] = float(entry[1].strip())
        elif "流通市值" in entry[0].strip():
            data_dict["Circulation_Market_Value"] = float(entry[1].strip())
        elif "平均市盈率" in entry[0].strip():
            data_dict["AVG_PE"] = float(entry[1].strip())
    data_dict["Date"] = date.today() - timedelta(days=1)
    data_dict["Market_Type"] = data_type

    log_progress("Data transformation complete.")
    return data_dict

def get_last_day_of_previous_month():
    # 获取当前日期
    today = date.today()
    # 将当前日期设置为本月第一天，然后减去一天
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    return last_day_of_previous_month

def belong_to_same_year_month(date1, date2):
    date2_str = str(date2).replace('-', '')
    if date1[:6] == date2_str[:6]:
        return True
    else:
        return False

def get_last_month():
    # 获取当前日期
    today = date.today()

    return today.month - 1

def get_last_trading_day_of_previous_month():
    c = Config()
    # 获取当前日期
    last_day_prev_month = get_last_day_of_previous_month()
    query = f"SELECT * from {c.table_trading_days} WHERE MONTH(Date) = {last_day_prev_month.month} AND YEAR(Date) = {last_day_prev_month.year} ORDER BY Date DESC LIMIT 1"
    with open_mysql(c) as engine:
        df_retrieved = run_query(query, engine)
    print(df_retrieved.iloc[0,0])
    return df_retrieved.iloc[0,0]
