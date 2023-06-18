"""Main module for ETL process."""
import datetime
import os
from contextlib import closing
from os import getenv
from time import sleep

import psycopg2
from dotenv import load_dotenv
from elastic_loader import ElasticLoader
from extract import Extractor, ModifiedDatetimes
from log import logger
from psycopg2.extras import DictCursor
from state import MoviesStateManager

load_dotenv('../.env')

DSL = {
    'dbname': os.environ.get('DB_NAME', 'movies_database'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', 'admin'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432),
}


def start_etl():
    """Start extract-transform-load process."""
    logger.info('Start ETL process')
    if not getenv('ELASTIC_HOST'):
        logger.error('Elastic host is not defined')
        raise ValueError('Elastic host is not defined')
    elastic_loader = ElasticLoader(
        hosts=getenv('ELASTIC_HOST'),
        basic_auth=(getenv('ELASTIC_USERNAME', 'elastic'), getenv('ELASTIC_PASSWORD', 'changeme'),),
        index_name=getenv('ELASTIC_INDEX_NAME', 'movies'),
    )
    elastic_loader.create_index_if_not_exists()
    elastic_loader.create_index_if_not_exists()
    state_type = getenv('STATE_TYPE', 'json')
    sleep_time = float(getenv('SLEEP_TIME', 1))
    if not state_type:
        logger.error('State type is not defined')
        raise ValueError('State type is not defined')

    state_manager = MoviesStateManager(state_type, getenv('DATE_FORMAT', '%Y-%m-%d %H:%M:%S.%f%z'))
    modified = ModifiedDatetimes(
        state_manager.last_date_of_modified_person or datetime.datetime(1900, 1, 1),
        state_manager.last_date_of_modified_genre or datetime.datetime(1900, 1, 1),
        state_manager.last_date_of_modified_movie or datetime.datetime(1900, 1, 1),
    )

    with closing(psycopg2.connect(**DSL, cursor_factory=DictCursor)) as pg_connection:
        while True:
            try:
                extractor = Extractor(pg_connection, state_manager)
                movies, last_modified = extractor.get_data_to_elastic(modified)
                elastic_loader.load_movies(movies)
                logger.info('{} movies been processed.'.format(len(movies)))
                state_manager.last_date_of_modified_movie = last_modified.movie
                state_manager.last_date_of_modified_genre = last_modified.genre
                state_manager.last_date_of_modified_person = last_modified.person
            except Exception:
                logger.exception('Exception occur while processing ETL')
            sleep(sleep_time)


if __name__ == '__main__':
    start_etl()
