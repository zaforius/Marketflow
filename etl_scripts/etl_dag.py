
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
# from airflow.hooks.base_hook import BaseHook
# from sqlalchemy import create_engine
from datetime import datetime
# import pandas as pd
# import csv
from exctract_script import extract_function
# from transform_script import write_data
from final_ws_extract import extraction_function
from final_ws_transform import transformation_function

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 10),
}

dag = DAG('etl_dag', default_args=default_args, schedule_interval=None)

    


task1 = PythonOperator(
    task_id='Extract_K',
    python_callable=extract_function,
    dag = dag
)

task2 = PythonOperator(
    task_id='Extract_MM',
    python_callable= extraction_function,
    dag = dag
)


task3 = PythonOperator(
    task_id='Transform',
    python_callable= transformation_function,
    dag = dag
)

task4 = EmptyOperator(
        task_id="Load",
        dag = dag
    )



[task1 , task2] >> task3 >> task4

