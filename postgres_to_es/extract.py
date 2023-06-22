"""Module providing class for extracting data from db."""
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from typing import NamedTuple

import psycopg2.extensions
from backoff import backoff
from constants import JUNCTION_TABLES, MOVIE_TABLE_NAME
from models import MovieSchema, PersonSchema
from state import State

person_from_db = namedtuple('person_from_db', ['id', 'name', 'modified'])
movies_from_db = namedtuple('movies_from_db', ['id', 'modified'])


@dataclass
class ModifiedDatetimes:
    """Modified datetimes for each table."""

    person: datetime
    genre: datetime
    film_work: datetime


class PersonFromDb(NamedTuple):
    """Person from db."""

    id: str
    role: str
    name: datetime


class GenreFromDb(NamedTuple):
    """Genre from db."""

    id: str
    name: str
    modified: datetime


class MoviesFromDb(NamedTuple):
    """Movies from db."""

    id: str
    modified: datetime


class MoviesWithFull(NamedTuple):
    """Movies with full info from db."""

    fw_id: str
    title: str
    description: str
    rating: float
    type: str
    created: datetime
    modified: datetime
    persons: list[PersonFromDb]
    genre: str


class Extractor:
    """Class provides methods for extract data from database and transform it to schema for elastic."""

    def __init__(self, pg_client: psycopg2.extensions.connection, state_manager: State) -> None:
        self.pg_client = pg_client
        self.state_manager = state_manager

    def get_data_to_elastic(
        self, modified_dates: ModifiedDatetimes, max_extract_row_by_query: int = 1000
    ) -> tuple[list[MovieSchema], ModifiedDatetimes]:
        """Get data from database and transform it to schema for elastic."""
        movies_to_update = []
        for table_name, junction_table in JUNCTION_TABLES.items():

            movies, max_modified_junction = self.extract_modified(
                table_name, junction_table, getattr(modified_dates, table_name), max_extract_row_by_query
            )
            if max_modified_junction is not None:
                modified_dates.__setattr__(table_name, max_modified_junction)
            movies_to_update += movies
        movies_with_full_info = self.get_full_info([movie.id for movie in movies_to_update])
        return movies_with_full_info, modified_dates

    @backoff
    def extract_modified(
        self, table_name: str, junction_table: str, from_modified_date: datetime, max_records: int
    ) -> tuple[MoviesFromDb, datetime]:
        """Get records which modified after from_modified_date and their movies."""
        if junction_table is None and table_name == MOVIE_TABLE_NAME:
            query = f"""
                SELECT t.id as movie_id, t.modified as movie_modified, t.modified as table_modified
                FROM content.{table_name} t
                WHERE t.modified > %s
                ORDER BY t.modified
                LIMIT %s
                """
        else:
            query = f"""
                SELECT fw.id as movie_id, fw.modified as movie_modified, t.modified as table_modified
                FROM content.{table_name} t
                JOIN content.{junction_table} jt ON jt.{table_name}_id = t.id
                JOIN content.film_work fw ON jt.film_work_id = fw.id
                WHERE t.modified > %s
                ORDER BY fw.modified, t.modified
                LIMIT %s
                """
        with self.pg_client.cursor() as cursor:
            cursor.execute(
                query, (from_modified_date, max_records),
            )
            max_modified_junction = None
            movies = []
            for row in cursor.fetchall():
                movies.append(MoviesFromDb(*row[:2]))
                max_modified_junction = max(row[2], max_modified_junction or row[2])
        return movies, max_modified_junction

    @backoff
    def get_full_info(self, movies_id: list[str]) -> list[MovieSchema]:
        """Get full info about movies by id."""
        if not movies_id:
            return []
        with self.pg_client.cursor() as cursor:
            cursor.execute(
                """
            SELECT
                fw.id,
                fw.title,
                fw.description,
                fw.rating as imdb_rating,
                fw.type,
                fw.created,
                fw.modified,
                COALESCE (
                    json_agg(
                        DISTINCT jsonb_build_object(
                            'id', p.id,
                            'name', p.full_name
                        )
                    ) FILTER (WHERE p.id is not null AND pfw.role='a'),
                    '[]'
                ) as actors,
                COALESCE (
                    json_agg(
                        DISTINCT jsonb_build_object(
                            'id', p.id,
                            'name', p.full_name
                        )
                    ) FILTER (WHERE p.id is not null AND pfw.role='w'),
                    '[]'
                ) as writers,
                COALESCE (
                    jsonb_agg(
                        DISTINCT jsonb_build_object(
                            'id', p.id,
                            'name', p.full_name
                        )
                    ) FILTER (WHERE p.id is not null AND pfw.role='d'),
                    '[]'
                ) as directors,
                array_agg(DISTINCT g.name) as genres
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN %s
            GROUP BY fw.id, fw.title, fw.description, fw.rating, fw.type, fw.created, fw.modified;
            """,
                (tuple(movies_id),),
            )
            columns = [desc[0] for desc in cursor.description]
            movies: list[MovieSchema] = []

            for row in cursor.fetchall():
                res = dict(zip(columns, row))
                res['director'] = res['directors'][0]['name'] if res['directors'] else ''
                res['actors_names'] = [actor['name'] for actor in res['actors']]
                res['writers_names'] = [writer['name'] for writer in res['writers']]
                res['description'] = res['description'] if res['description'] else ''
                res['imdb_rating'] = float(res['imdb_rating']) if res['imdb_rating'] else 0
                for genre in res['genres']:
                    movies.append(
                        MovieSchema(
                            id=res['id'],
                            imdb_rating=res['imdb_rating'],
                            genre=genre,
                            title=res['title'],
                            description=res['description'],
                            director=res['director'],
                            actors_names=res['actors_names'],
                            writers_names=res['writers_names'],
                            actors=[
                                PersonSchema(id=actor['id'], name=actor['name']) for actor in res['actors']
                            ],
                            writers=[
                                PersonSchema(id=writer['id'], name=writer['name'])
                                for writer in res['writers']
                            ],
                        )
                    )
            return movies
