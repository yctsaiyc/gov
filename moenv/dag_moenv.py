from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timezone, timedelta, datetime
import sys

sys.path.insert(0, "/opt/airflow/script/moenv")
from etl_moenv import ETL_moenv


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 10, 16, 0, 0, 0, tzinfo=timezone(timedelta(hours=8))),
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}


def run_etl_moenv():
    etl_moenv = ETL_moenv(
        code="aqx_p_04",
        api_key="",
        data_dir_path="/opt/airflow/data/moenv",
        checkpoint_path="/opt/airflow/checkpoint/moenv/checkpoint.json",
    )

    json_data = etl_moenv.save_json()
    etl_moenv.save_csv(json_data)


with DAG(
    f"Crawler_moenv",
    default_args=default_args,
    schedule_interval="10 * * * *",
    max_active_runs=1,
    tags=["crawler", "moenv"],
) as dag:

    task = PythonOperator(
        task_id=f"crawler_moenv",
        python_callable=run_etl_moenv,
        dag=dag,
    )
