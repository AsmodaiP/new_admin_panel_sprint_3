"""Test excractor module."""
import os
from contextlib import closing

import psycopg2
from psycopg2.extras import DictCursor
from pytest import fixture

from postgres_to_es.extract import Extractor
from postgres_to_es.state import MockStorage

DSL = {
    'dbname': os.environ.get('DB_NAME', 'movies_database'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', 'admin'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432),
}


@fixture
def extractor():
    """Fixture for extractor."""
    with closing(psycopg2.connect(**DSL, cursor_factory=DictCursor)) as pg_connection:
        yield Extractor(pg_connection, state_manager=MockStorage())


def test_extract_persons(extractor: Extractor):
    """Test extract persons."""
    extractor.extract_persons(from_modified_date='2021-01-01', max_records=10)


def test_extract_movies_by_person_ids(extractor: Extractor):
    """Test extract movies by person ids."""
    persons = extractor.extract_persons(from_modified_date='2021-01-01', max_records=10)
    extractor.extract_movies_by_person_ids([person.id for person in persons])


def test_get_full_info(extractor: Extractor):
    """Test get full info."""
    persons = extractor.extract_persons(from_modified_date='2021-01-01', max_records=10)
    movies = extractor.extract_movies_by_person_ids([person.id for person in persons])
    extractor.get_full_info([movie.id for movie in movies])


def test_transform_movies_info_to_schema(extractor: Extractor):
    """Test transform movies info to schema."""
    persons = extractor.extract_persons(from_modified_date='2021-01-01', max_records=10)
    movies = extractor.extract_movies_by_person_ids([person.id for person in persons])
    movies_with_full = extractor.get_full_info([movie.id for movie in movies])
    extractor._transform_movies_info_to_schema(movies_with_full)
