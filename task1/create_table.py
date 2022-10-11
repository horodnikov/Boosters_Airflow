from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id='apilayer_create_table',
    start_date=datetime(2020, 8, 1),
    schedule="@once",
) as dag:
    task_create_table = PostgresOperator(
        task_id='create_table_currency',
        postgres_conn_id='postgres_apilayer',
        sql=f""" 
                CREATE TABLE IF NOT EXISTS currency (
                    currency_code CHAR(3),
                    date date NOT NULL,
                    exchange_rate DECIMAL,
                    created_on TIMESTAMP NOT null default CURRENT_TIMESTAMP,
                    PRIMARY KEY (date, currency_code) );"""
        )

    task_create_table
