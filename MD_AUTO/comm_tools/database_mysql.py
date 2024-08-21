import pandas as pd
from sqlalchemy import create_engine
from contextlib import contextmanager
from MD_AUTO.comm_tools.logger import log_progress
from MD_AUTO.comm_tools.config import Config


def load_to_MySQL_on_Cloud(df, sql_connection, table_name):
    """
    This function saves the final data frame to a database
    table with the provided name. Function returns nothing.

    :param df: data to be saved
    :param sql_connection: MySQL conn
    :param table_name: table name
    :return: none
    """
    try:
        df.to_sql(name=table_name, con=sql_connection, index=False, if_exists='append')  # 'replace' 替换已有表
        log_progress("Data loaded to Database as a table, Executing queries")
    except Exception as e:
        if "Duplicate" in str(e):
            log_progress("Data not loaded due to duplicate entry")


def run_query(query_statement, sql_connection):
    """
    This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing.

    :param query_statement:
    :param sql_connection:
    :return: retrieved data as dataframe
    """
    log_progress(f"Query statement is : {query_statement}")
    query_output = pd.read_sql(query_statement, sql_connection)
    log_progress(f"Query output is : {query_output}")
    return query_output


@contextmanager
def open_mysql(c:Config):
    try:
        # 创建 SQLAlchemy 引擎
        connection_string = (f"mysql+mysqlconnector://{c.user}:{c.password}"
                             f"@{c.host}:{c.port}/{c.database}")
        engine = create_engine(connection_string)
        yield engine
    finally:
        if engine is not None:
            # 释放引擎
            engine.dispose()
