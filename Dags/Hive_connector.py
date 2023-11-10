from airflow import DAG
from airflow.operators.hive_operator import HiveOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'admin',
    'start_date': days_ago(1)
}

# dag instance
dag = DAG(
    'test_hive_connection',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

hive_query = """
SHOW DATABASES;
"""

run_hive_query = HiveOperator(
    task_id='airflow_hive_query',
    hql=hive_query,
    hive_cli_conn_id='hive_connector',
    dag=dag,
)

run_hive_query

if __name__ == "__main__":
    dag.cli()