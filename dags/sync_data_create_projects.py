from airflow.decorators import dag, task
from pendulum import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
script_dir = os.path.join(current_dir, 'scripts')
print(script_dir)
sys.path.append(script_dir)

# from data_loaders.load_data_from_create_projects import load_data_mongo
# from data_transformers.transform_data_create_projects import transform_data

# def load_data_task(**kwarg):
#     data = load_data_mongo()
#     return data
@dag(
    start_date=datetime(2023, 3, 23),
    schedule="@daily",
    catchup=False,
)
def sync_data_create_projects():
    @task()
    def load_data_task():
        from scripts.data_loaders.load_data_from_create_projects import load_data_mongo
        data = load_data_mongo()
        print("Data returned by load_data_task:", data)
        return data
    
    @task()
    def transform_data_task(data):
        from scripts.data_transformers.transform_data_create_projects import transform_data
        transformed_data = transform_data(data)
        return transformed_data
    
    @task()
    def task_success(data):
        if True:
            print("DONE TASK!!")
    data = load_data_task()
    transform_data_task(data)
    task_success(data)
sync_data_create_projects()