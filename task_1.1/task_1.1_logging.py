from datetime import datetime, timedelta
import pendulum
import logging
import time

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': 5
}

local_tz = pendulum.timezone("Europe/Moscow")


@dag(
    dag_id="insert_logs_dag_runs",
    default_args=default_args,
    description="Загрузка логов в таблицу dag_logs",
    schedule_interval='*/1 * * * *',
    start_date=pendulum.datetime(2024, 1, 4, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
    max_active_tasks=6,
    tags=['task_1.1', 'neoflex', 'logs']
)
def main():
    @task
    def insert_logs_dag_runs():
        time.sleep(5)
        dag = DagBag().get_dag('extract_csv_and_download_to_postgres_cycle')

        last_dagrun_run_id = dag.get_last_dagrun(include_externally_triggered=True)

        dag_runs = DagRun.find(dag_id='extract_csv_and_download_to_postgres_cycle')
        for dag_run in dag_runs:
            if dag_run.execution_date == last_dagrun_run_id.execution_date:
                start_date = datetime.strptime(str(dag_run.start_date.astimezone(local_tz)).split("+")[0], "%Y-%m-%d %H:%M:%S.%f")
                end_date = datetime.strptime(str(dag_run.end_date.astimezone(local_tz)).split("+")[0], "%Y-%m-%d %H:%M:%S.%f")
                duration = end_date - start_date

                postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
                conn = postgres_hook.get_conn()
                cur = conn.cursor()
                cur.execute("""insert into logs.dag_logs (dag_name, run_id, start_date, end_date, duration, status)
                                values (%s, %s, %s, %s, %s, %s)
                                on conflict (run_id) do nothing;
                            """, ('extract_csv_and_download_to_postgres_cycle', dag_run.run_id, start_date, end_date, duration, dag_run.state))

                conn.commit()
            
    insert_logs_dag_runs()

main()
