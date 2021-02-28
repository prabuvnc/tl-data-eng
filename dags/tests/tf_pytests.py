import sys
import os
sys.path.append('/usr/local/airflow/dags/helpers/')

from tf_helper import get_imdb_data, get_wiki_data, get_high_profit_movies
import pandas as pd
import pytest

@pytest.mark.count_check
def test_imdb_count():
    df = get_imdb_data('/usr/local/airflow/data/movies_metadata.csv')
    assert len(df.index) == 1000

@pytest.mark.count_check
def test_high_ratio_movie_count(tmpdir):
    out_file = get_high_profit_movies(tmpdir, '/usr/local/airflow/data/movies_metadata.csv',
                                      '/usr/local/airflow/data/enwiki-latest-abstract.xml.gz')
    df = pd.read_csv(out_file)
    assert len(df.index) == 1000

@pytest.mark.columns_check
def test_imdb_cols(imdb_cols):
    df = get_imdb_data('/usr/local/airflow/data/movies_metadata.csv')
    assert df.columns.tolist() == imdb_cols

@pytest.mark.columns_check
def test_wiki_cols(wiki_cols):
    df = get_wiki_data('/usr/local/airflow/data/enwiki-latest-abstract.xml.gz')
    assert df.columns.tolist() == wiki_cols

@pytest.mark.data_check
def test_high_profit_movies(tmpdir, high_profit_movies):
    out_file = get_high_profit_movies(tmpdir, '/usr/local/airflow/data/movies_metadata.csv',
                                      '/usr/local/airflow/data/enwiki-latest-abstract.xml.gz')
    df = pd.read_csv(out_file)
    assert set(high_profit_movies).issubset(df['title'].values.tolist())

@pytest.mark.columns_check
def test_high_profit_cols(tmpdir, high_profit_movie_cols):
    out_file = get_high_profit_movies(tmpdir, '/usr/local/airflow/data/movies_metadata.csv',
                                      '/usr/local/airflow/data/enwiki-latest-abstract.xml.gz')
    df = pd.read_csv(out_file)
    assert df.columns.tolist() == high_profit_movie_cols