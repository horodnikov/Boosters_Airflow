from __future__ import annotations
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import timedelta, datetime
import pendulum
import requests
import pandas as pd


@dag(
    schedule='@daily',
    start_date=pendulum.datetime(2022, 9, 1),
    catchup=False,
    tags=['APILaye'],
)
def currency_api():
    """
        Daily transfer data from https://apilayer.com/ to Postgres
        currency_api using three simple tasks for Extract, Transform, and Load.
        Also function Wrapped_execute and task Is_missed
        """
    def wrapped_execute(query: str, data_tuple: tuple, autocommit=True):
        """Connect to the database using  PostgresHook."""
        hook = PostgresHook(postgres_conn_id="postgres_apilayer")
        conn = hook.get_conn()
        conn.autocommit = autocommit
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, data_tuple)
                records = cursor.fetchall()
                return records
        except Exception as exception:
            print(f"Failed query execution. Exception: {exception}")

    @task()
    def extract(url: str):
        """Extract task to get data ready for the rest of the data
            pipeline."""
        response = requests.get(url=url, headers=headers)
        if response.status_code == 200:
            json_data = response.json()
            return json_data

    @task()
    def transform(json_data: dict):
        """Transform to  convenient form. Prepare to saving data"""
        trans = []
        for key, value in json_data['rates'].items():
            currency = {'currency_code': key,
                        'exchange_rate': value,
                        'date': json_data['date']}
            trans.append(currency)
        return trans

    @task()
    def load(trans: list, autocommit=True):
        """Load and upsert data to Postgres database"""
        try:
            hook = PostgresHook(postgres_conn_id="postgres_apilayer")
            conn = hook.get_conn()
            conn.autocommit = autocommit
            with conn.cursor() as cursor:
                for row in trans:
                    cursor.execute(f"""INSERT INTO currency
                                    (currency_code, date, exchange_rate)
                                VALUES(
                                    '{row['currency_code']}',
                                    '{row['date']}',
                                    {row['exchange_rate']})
                            ON CONFLICT (date, currency_code) DO NOTHING;""")
        except Exception as exception:
            print(f"Failed query execution. Exception: {exception}")

    @task()
    def is_missed():
        """Find missing values in database for a period of 30 days"""
        res = pd.date_range(
            datetime.today() - timedelta(days=30),
            datetime.today()
        ).strftime('%Y-%m-%d').tolist()
        records = wrapped_execute(
            "SELECT DISTINCT currency.date from currency "
            "WHERE currency.exchange_rate is not null", ())
        db_dates = [i[0].strftime("%Y-%m-%d") for i in records]
        dates_diff = list(set(res) - set(db_dates))
        if dates_diff:
            url_lst = [f"https://api.apilayer.com/exchangerates_data/{missed_date}?&base={base}" for missed_date in dates_diff]
            print(url_lst)
            return url_lst[0]
        return "Database contains the full list of dates"

    base = 'USD'
    daily_url = f"https://api.apilayer.com/exchangerates_data/latest?&base={base}"
    User_Agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
                 "(KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"

    headers = {
        'user_agent': User_Agent,
        "apikey": Variable.get("KEY_API_Currency")
    }

    extract_daily = extract(daily_url)
    transform_daily = transform(extract_daily)
    load(transform_daily)
    missed_list = is_missed()
    historical = extract(missed_list)
    transform_historical = transform(historical)
    load(transform_historical)


currency_api()
