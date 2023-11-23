import pendulum

from airflow import DAG
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


# Подключение Vertica
vertica_conn_id = 'VERTICA_CONNCETION'
vertica_hook = VerticaHook(vertica_conn_id)
vertica_connection = vertica_hook.get_conn()

def load_to_dwh_tab(connection, scheduled_date):
    with open('../sql/dml_dwh.sql', 'r') as file:
        sql = file.read().format(date=scheduled_date)
        
    with connection as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


with DAG (
    dag_id = 'load_to_dwh',
    schedule_interval='@daily',
    start_date=pendulum.parse('2022-10-02'),
    end_date=pendulum.parse('2022-11-02'),
    catchup=True
) as dag:
    start = DummyOperator(task_id='start')

    load_to_global_metrics = PythonOperator(
        task_id='load_to_global_metrics',
        python_callable=load_to_dwh_tab,
        op_kwargs= {
            'connection': vertica_connection,
            'scheduled_date': '{{ ds }}'
        }
    )

    end = DummyOperator(task_id='end')

    (
    start 
    >> load_to_global_metrics 
    >> end
    )