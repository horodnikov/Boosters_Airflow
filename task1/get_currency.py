from config import host, port, db_name, user, password
import psycopg2
from datetime import datetime
import pandas as pd


def connect():
    """ Connect to the PostgresSQL database server """
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=db_name,
            user=user,
            password=password)
        print("Connected!")
        return connection
    except Exception as exception:
        print(f"Failed to connect! Exception: {exception}")


db_conn = connect()


def wrapped_execute(query: str, data_tuple: tuple, autocommit=True):
    """Checks database connectivity
    Makes queries with parameters more flexible"""
    db_conn.autocommit = autocommit
    try:
        with db_conn.cursor() as cursor:
            cursor.execute(query, data_tuple)
            records = cursor.fetchall()
            return records
    except Exception as exception:
        print(f"Failed query execution. Exception: {exception}")


def get_column(table_name: str):
    """get column names of the table"""
    column = wrapped_execute(
        "select column_name from information_schema.columns "
        "where table_name = %s", (table_name,))
    column_names = [row[0] for row in column]
    return column_names


def get_currency(date_from: datetime, date_to: datetime, currency: list = None):
    """Exchange rate for selected days for selected currencies/for all currencies."""
    records = wrapped_execute(
        "SELECT c.date, c.exchange_rate, c.created_on, c.currency_code "
        "FROM currency as c WHERE c.date BETWEEN %s and %s",
        (date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d")))
    column_names = get_column('currency')
    df = pd.DataFrame(records, columns=column_names)
    if currency:
        return df.loc[df['currency_code'].isin(currency)]
    else:
        return df


if __name__ == "__main__":

    start_date = datetime(2022, 9, 20)
    end_date = datetime(2022, 10, 2)

    currency_df = get_currency(start_date, end_date, ['EUR', 'GBP'])
    print(f"\nResult dataframe filtered by currency codes :\n{currency_df}")
