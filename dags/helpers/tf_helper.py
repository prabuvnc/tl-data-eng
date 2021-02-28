"""
Collection of helper functions related to TrueFilm Data Pipeline
"""
import gzip
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from airflow.hooks.postgres_hook import PostgresHook


def _download_movie_data(p_path: str, p_url_info: dict, **context):
    """Downloads the data into p_path from the url list passed

    Args:
        p_path (str): Download directory
        p_url_list (list): List of URL's to be downloaded
        context: Airflow context information
    """
    make_dirs(p_path)
    for info, e_url in p_url_info.items():
        logging.info(f'Downloading file from {e_url}')
        if 'https:' in e_url:
            file_name = e_url.rsplit('/', 1)[1]
            download_file = os.path.join(p_path, file_name)
            context['ti'].xcom_push(info, download_file)
            if os.path.isfile(download_file):
                logging.info(
                    f'File {file_name} has been already downloaded. Skipping it...')
                continue
            response = requests.get(e_url, allow_redirects=True)
            with open(download_file, 'wb') as f:
                f.write(response.content)
        else:
            context['ti'].xcom_push(info, e_url)

    else:
        logging.info('All files are downloaded succesfully')


def get_high_profit_movies(p_path: str, p_imdb_file: str, p_wiki_file: str):
    """Retrieve high profit movies based on below logic
        - For each film from IMDB dataset , calculate ratio of budget to revenue
        - Match each IMDB movie to its corresponding wikipedia info
    Args:
        p_path (str): Output directory to where the files should be written
    """

    imdb_info_df = get_imdb_data(p_imdb_file)
    wiki_info_df = get_wiki_data(p_wiki_file)
    merge_df = pd.merge(imdb_info_df, wiki_info_df, on='title', how='left')
    merge_df['load_timestamp'] = datetime.now()
    make_dirs(p_path)
    out_file_name = f'high_profit_movies_{get_current_datetime()}.csv'
    out_file = os.path.join(p_path, out_file_name)
    merge_df.to_csv(out_file, index=False, header=True)
    return out_file


def make_dirs(p_path):
    if not os.path.isdir(p_path):
        os.makedirs(p_path)


def get_current_datetime():
    return datetime.now().strftime('%Y%m%d%H%M%S')


def get_imdb_data(p_file: str):
    """Transforms imdb movie dataset as per the requirement

    Args:
        p_file (str): imdb dataset file
    """
    logging.info(f'Reading file::{p_file}')
    # List of columns required for the logic
    select_cols = ['title', 'budget', 'release_date', 'revenue', 'vote_average', 'production_companies']
    imdb_md_df = pd.read_csv(p_file, dtype=object, usecols=select_cols)

    # Converting the columns for calculating ratio to numeric
    imdb_md_df[["budget", "revenue"]] = imdb_md_df[["budget", "revenue"]].apply(pd.to_numeric, errors='coerce')

    # Some movies have very less budget due to wrong info , setting a threshold of 200 USD
    imdb_md_df = imdb_md_df[imdb_md_df['budget'] >= 200]

    # Settting ratio to zero if budget or revenue equals to zero to avoid NaNs and inf
    zero_condn = (imdb_md_df.budget == 0) | (imdb_md_df.revenue == 0)
    imdb_md_df['ratio'] = np.where(zero_condn, 0, (imdb_md_df['revenue'] / imdb_md_df['budget']))

    # Production companies are in list of dict format , converting it to string delimited by ;
    imdb_md_df['production_companies'] = imdb_md_df['production_companies'].apply(lambda x: extract_prod_company(x))

    arrange_cols = ['title', 'budget', 'release_date', 'revenue', 'vote_average', 'production_companies', 'ratio']
    imdb_md_df = imdb_md_df[arrange_cols]
    
    # returning only top 1000 records with highest ratio
    return imdb_md_df.sort_values('ratio', ascending=False).head(1000)


def extract_prod_company(p_str: str) -> str:
    """Extracts production company names and returns as ; delimited string

    Args:
        p_str (str): Production company info as list of dicts

    Returns:
        str: ; delimited string
    """
    if isinstance(p_str, str) and '[' in p_str:
        pc_list = eval(p_str)
        ret_list = []
        for pc in pc_list:
            ret_list.append(pc.get('name', ''))
        return ';'.join(ret_list)
    else:
        return None


def parse_wiki_data(xml_file: str, element_tag: str) -> list:
    """Parse the wiki dumps xml and extracts the required tags based on events

    Args:
        xml_file (str): XML file to be parsed
        element_tag (str): Tag of each record

    Returns:
        list<dict>: Returns a list of dict contatining tag info
        example:
            [{title: 'title', ....}]
    """
    ret_dict = {}
    ret_list = []
    context = ET.iterparse(xml_file, events=("start", "end"))
    is_first = True
    i = 0
    for event, elem in context:
        # if i == 10000:
        #     break
        if is_first:
            root = elem
            is_first = False
        if event == 'end':
            if elem.tag in ['title', 'url', 'abstract']:
                if elem.tag == 'title':
                    ret_dict[elem.tag] = elem.text.replace('Wikipedia:', '').replace('(film)', '').strip()
                else:
                    ret_dict[elem.tag] = elem.text
            if elem.tag == element_tag:
                # yield ret_dict
                ret_list.append(ret_dict)
                ret_dict = {}
                # i = i + 1
                root.clear()
    return ret_list


def get_wiki_data(p_file: str):
    """Parses and extracts required info from wiki xml dumps
        If parsed csv already exists it reads and sends back the dataframe

    Args:
        p_file (str): wiki dumps file
    """
    logging.info('Checking if parsed file exists already...')
    csv_file = os.path.join(os.path.dirname(p_file), os.path.basename(p_file).replace('.xml.gz', '.csv'))
    if os.path.isfile(csv_file):
        logging.info(f'Parsed file {csv_file} exists already... Skipping the xml parsing..')
        return pd.read_csv(csv_file, dtype=object)
    logging.info(f'Reading file::{p_file}')
    with gzip.open(p_file, 'r') as xml_file:
        data_list = parse_wiki_data(xml_file, 'doc')
        wiki_df = pd.DataFrame(data_list)
        wiki_df.drop_duplicates(subset=['title'], inplace=True)
        wiki_df.to_csv(csv_file, header=True, index=False)
    return wiki_df


def load_to_pg(p_table_name: str, p_file: str):
    """Loads the generated output file to Postgres database via Pandas to_sql
       It creates the table according to dataframe if it doesnt exists
       and appends data if it exists


    Args:
        p_table_name ([str]): Table name 
        p_table_name ([str]): File needs to be loaded
    """
    pg_hook = PostgresHook(postgres_conn_id='pg_local')
    conn = pg_hook.get_sqlalchemy_engine()
    load_df = pd.read_csv(p_file)
    load_df.to_sql(p_table_name, conn, index=False, if_exists='append')
