from pendulum import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

PATH_TO_DBT_PROJECT = "/usr/local/airflow/dags/my_dbt_postgres"
PATH_TO_DBT_VENV = ".dbt/bin/activate"


@dag(
    start_date=datetime(2023, 3, 23),
    schedule="@daily",
    catchup=False,
)
def dbt_project():
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd /usr/local/airflow && chmod -R 777 /usr/local/airflow/dags/dbt/runcockpit && source {PATH_TO_DBT_VENV} && cd  /usr/local/airflow/dags/dbt/runcockpit && dbt run",
        # env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        # cwd=PATH_TO_DBT_PROJECT,
    )


dbt_project()