import pendulum
import io

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


# Подключение Postgres
postgres_conn_id = 'PG_WAREHOUSE_CONNECTION'
psql_hook = PostgresHook(postgres_conn_id)
psql_connection = psql_hook.get_conn()

# Подключение Vertica
vertica_conn_id = 'VERTICA_CONNCETION'
vertica_hook = VerticaHook(vertica_conn_id)
vertica_connection = vertica_hook.get_conn()


def upload_from_postgre(connection, table_name, field_with_date, scheduled_date):
    with connection as conn:
        with conn.cursor() as cur:
            sql = (
                f"""
                copy (
                    select * 
                    from public.{table_name}
                    where {field_with_date}::date = '{scheduled_date}'::date-1
                    ) 
                to stdout;
                """
            )
            input = io.StringIO()
            cur.copy_expert(sql, input)

    return input

def load_to_vertica(connection, schema, table_name, field_with_date, scheduled_date, postgres_data):
    with connection as conn:
        with conn.cursor() as cur:
            cur.execute(
                f""" 
                delete from {schema}.{table_name}
                where {field_with_date}::date = '{scheduled_date}'::date-1;
                """
            )

            cur.copy(
                f""" 
                copy {schema}.{table_name}
                from stdin 
                delimiter e'\t';
                """,
                postgres_data.getvalue() 
            )

def update_data(psql_connection, vertica_connection, vertica_schema, table_name, field_with_date, scheduled_date):
    postgres_data = upload_from_postgre(psql_connection, table_name, field_with_date, scheduled_date)

    load_to_vertica(vertica_connection, vertica_schema, table_name, field_with_date, scheduled_date, postgres_data)

with DAG (
    dag_id = 'load_to_staging',
    schedule_interval='@daily',
    start_date=pendulum.parse('2022-10-02'),
    end_date=pendulum.parse('2022-11-02'),
    catchup=True
) as dag:
    start = DummyOperator(task_id='start')

    load_transactions = PythonOperator(
        task_id='load_transactions',
        python_callable=update_data,
        op_kwargs={
            'psql_connection': psql_connection,
            'vertica_connection': vertica_connection, 
            'vertica_schema': 'STV202310160__STAGING',
            'table_name': 'transactions',
            'field_with_date': 'transaction_dt',
            'scheduled_date': '{{ ds }}'
        }
    )

    load_currencies = PythonOperator(
        task_id='load_currencies',
        python_callable=update_data,
        op_kwargs={
            'psql_connection': psql_connection,
            'vertica_connection': vertica_connection, 
            'vertica_schema': 'STV202310160__STAGING',
            'table_name': 'currencies',
            'field_with_date': 'date_update',
            'scheduled_date': '{{ ds }}'
        }
    )

    end = DummyOperator(task_id='end')

    (
    start 
    >> load_transactions 
    >> load_currencies 
    >> end
    )