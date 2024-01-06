import datetime
import pendulum
import logging

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': 30
}


def pg_copy_csv(table_name, data_path):
    """Download csv to postgres"""

    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    with open(data_path, "r") as file:
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"'",
        file,
        )
    
    conn.commit()
    logging.info(f"CSV файл {data_path} успешно загружен в таблицу {table_name}")


@dag(
    dag_id="extract_csv_and_download_to_postgres_cycle",
    default_args=default_args,
    description="Загрузка данных из csv файла и построение детального слоя",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2024, 1, 4, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    max_active_runs=1,
    max_active_tasks=6,
    tags=['task_1.1', 'neoflex']
)
def main():
    for table in ['ft_balance_f', 'ft_posting_f', 'md_account_d', 'md_currency_d', 'md_exchange_rate_d', 'md_ledger_account_s']:
        @task_group(group_id=table)
        def etl():
            create_tmp_table = PostgresOperator(
                task_id=f"create_{table}_tmp",
                postgres_conn_id="postgres_default",
                sql=f"sql/create_tmp_tables/create_tmp_{table}.sql",
            )

            upload_csv = PythonOperator(
                task_id=f'upload_csv_to_{table}_tmp',
                python_callable=pg_copy_csv,
                op_kwargs={
                    'table_name': f'ds.{table}_tmp',
                    'data_path': f'/opt/airflow/dags/data/{table}.csv'
                    }
            )

            create_ds_table = PostgresOperator(
                task_id=f"create_{table}",
                postgres_conn_id="postgres_default",
                sql=f"sql/create_ds_tables/create_{table}.sql",
            )

            upsert_data = PostgresOperator(
                task_id=f"upsert_{table}",
                postgres_conn_id="postgres_default",
                sql=f"sql/upsert_to_tables/upsert_{table}.sql",
            )

            delete_tmp_table = PostgresOperator(
                task_id=f"delete_tmp_{table}",
                postgres_conn_id="postgres_default",
                sql=f"drop table if exists ds.{table}_tmp;",
            )

            create_tmp_table >> upload_csv >> create_ds_table >> upsert_data >> delete_tmp_table

        etl()    

main()
