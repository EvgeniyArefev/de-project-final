from datetime import datetime
import pandas as pd
import psycopg2
from vertica_python import connect

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


postgres_conn_id = 'PG_WAREHOUSE_CONNECTION'
psql_hook = PostgresHook(postgres_conn_id)
psql_connection = dwh_hook.get_conn()

# Параметры подключения к Vertica
vertica_host = 'vertica.tgcloudenv.ru'
vertica_port = 5433
vertica_database = 'dwh'
vertica_user = 'stv202310160'
vertica_password = 'sYPIATLm691wg2i'

# Установка подключения к Vertica
vertica_connection = connect(
    host=vertica_host,
    port=vertica_port,
    database=vertica_database,
    user=vertica_user,
    password=vertica_password,
    autocommit=True
)


def upload_from_postgre_to_csv(connection, data):
    with connection as conn:
        with conn.cursor() as cur:
            sql = (
                f"""
                copy (
                    select * 
                    from public.{data}
                    where transaction_dt::date = '2022-10-25' and operation_id = '2a1aefa7-253a-4110-800b-26dd50037a48'
                    ) 
                to stdout with csv header;
                """
            )
            with open(f'./{data}.csv', 'w') as file:
                cur.copy_expert(sql, file)


def load_from_csv_to_vertica(connection, data):
    df = pd.read_csv(f'./{data}.csv')
    columns = ','.join(df.columns)

    with connection as conn:
        with conn.cursor() as cur:
            cur.execute(
                f""" 
                delete from STV202310160__STAGING.{data}
                where operation_id = '2a1aefa7-253a-4110-800b-26dd50037a48';
                """
            )

            cur.execute(
                f""" 
                copy STV202310160__STAGING.{data} ({columns})
                from local './{data}.csv'
                delimiter ',';
                """
            )


with DAG (
    dag_id = 'load_to_staging',
    schedule_interval='0 0 * * *',
    start_date=datetime.today()
) as dag:
    upload_transactions_from_psql = PythonOperator(
        task_id='upload_transactions_from_psql',
        python_callable=upload_from_postgre_to_csv,
        op_kwargs={
            'connection': psql_connection,
            'data': 'transactions'
        }
    )

    load_transactions_to_vertica = PythonOperator(
        task_id='load_transactions_to_vertica',
        python_callable=load_from_csv_to_vertica,
        op_kwargs={
            'connection': vertica_connection,
            'data': 'transactions'
        }
    )

    upload_transactions_from_psql >> load_transactions_to_vertica


