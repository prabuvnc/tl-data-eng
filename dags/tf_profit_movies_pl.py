__author__ = 'Prabakaran Chenni'

"""
    TrueFilm - DAG to determine high profit movies
    Steps Involved:
    1) Downloads the data from internet if file is not downloaded already
    2) Determines the high profit movies based on ratio of budget to revenue
    3) Loads the top 1000 movies (with highest ratio) with wiki details into Postgres Database

"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from helpers.tf_helper import (_download_movie_data, get_high_profit_movies,
                               load_to_pg)

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

# Global Variables
DATA_DIR = 'data/'
OUTPUT_DIR = 'output/'
PG_TABLE_NAME = 'tf_high_profit_movies'
URL_INFO = {
    'imdb_info': 'data/movies_metadata.csv',
    'wiki_info': 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz'
}


def _get_high_profit_movies(p_path: str, **context):
    imdb_info_file = context['ti'].xcom_pull(key='imdb_info')
    wiki_info_file = context['ti'].xcom_pull(key='wiki_info')
    out_file = get_high_profit_movies(p_path, imdb_info_file, wiki_info_file)
    context['ti'].xcom_push('out_file', out_file)


def _load_data_to_pg(p_table_name: str, **context):
    out_file = context['ti'].xcom_pull(key='out_file')
    load_to_pg(p_table_name, out_file)


with DAG('tf_profit_movies_pl', default_args=default_args, catchup=False, schedule_interval=None) as dag:

    download_task = PythonOperator(
        task_id='download_movie_data',
        python_callable=_download_movie_data,
        op_kwargs={
            'p_path': DATA_DIR,
            'p_url_info': URL_INFO
        }
    )

    prep_high_profit_movies = PythonOperator(
        task_id='prep_high_profit_movies',
        python_callable=_get_high_profit_movies,
        op_kwargs={
            'p_path': OUTPUT_DIR
        }
    )

    load_data_to_pg = PythonOperator(
        task_id='load_data_to_pg',
        python_callable=_load_data_to_pg,
        op_kwargs={
            'p_table_name': PG_TABLE_NAME
        }
    )

    download_task >> prep_high_profit_movies >> load_data_to_pg
