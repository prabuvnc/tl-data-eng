__author__ = 'Prabakaran Chenni'

"""
    TrueFilm - DAG to unit test TF profit movie functions
    Tests Involved:
    1) Validates the count of IMDB data and final output data
    2) Validate the columns 
    3) Data correctness check against a internet source

"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'prabakaran_chenni',
    'email': ['prabakaran.vnc@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'depends_on_past': False,
}

with DAG('tf_profit_movies_ut', default_args=default_args, catchup=False, schedule_interval=None) as dag:

    init_task = DummyOperator(
        task_id='init_task'
    )

    count_test = BashOperator(
        task_id='count_test',
        bash_command = 'python -m pytest /usr/local/airflow/dags/tests/tf_pytests.py -m count_check -v --disable-warnings' 
    )

    columns_test = BashOperator(
        task_id='columns_test',
        bash_command = 'python -m pytest /usr/local/airflow/dags/tests/tf_pytests.py -m columns_check -v --disable-warnings'
    )

    data_test = BashOperator(
        task_id='data_test',
        bash_command = 'python -m pytest /usr/local/airflow/dags/tests/tf_pytests.py -m data_check -v --disable-warnings'
    )

    
    init_task >> [count_test, columns_test, data_test]
