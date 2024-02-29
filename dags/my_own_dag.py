import os
import requests
import logging
from pendulum import datetime
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2022,11,1),
    schedule="@daily",
    catchup=False
)
def my_own_dag():

        @task
        def get_data():
            # NOTE: configure this as appropriate for your airflow environment
            data_path = "/opt/airflow/dags/try/employees.csv"
            os.makedirs(os.path.dirname(data_path), exist_ok=True)
            # url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"

            # response = requests.request("GET", url)

            # with open(data_path, "w") as file:
            #     file.write(response.text)

            # postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            # conn = postgres_hook.get_conn()
            # cur = conn.cursor()
            # with open(data_path, "r") as file:
            #     cur.copy_expert(
            #         "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
            #         file,
            #     )
            # conn.commit() 

        get_data()
       


my_own_dag()