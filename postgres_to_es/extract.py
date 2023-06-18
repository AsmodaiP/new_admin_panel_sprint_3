"""Module providing class for extracting data from db."""
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from typing import NamedTuple

import psycopg2.extensions
from backoff import backoff
from models import MovieSchema, PersonSchema
from state import State

person_from_db = namedtuple('person_from_db', ['id', 'name', 'modified'])
movies_from_db = namedtuple('movies_from_db', ['id', 'modified'])


@dataclass
class ModifiedDatetimes:
    """Modified datetimes for each table."""

    person: datetime
    genre: datetime
    movie: datetime


class PersonFromDb(NamedTuple):
    """Person from db."""

    id: str
    name: str
    modified: datetime


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
    role: str
    p_id: str
    full_name: str
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
        persons = self.extract_persons(modified_dates.person, max_extract_row_by_query)
        movies_by_persons = self.extract_movies_by_person_ids([person.id for person in persons])
        modified_dates.person = max([person.modified for person in persons] or [modified_dates.person])

        genres = self.extract_genres(modified_dates.genre, max_extract_row_by_query)
        movies_by_genres = self.extract_movies_by_genre_ids([genre.id for genre in genres])
        modified_dates.genre = max([genre.modified for genre in genres] or [modified_dates.genre])

        movies = self.extract_modified_movies(modified_dates.movie, max_extract_row_by_query)
        modified_dates.movie = max([movie.modified for movie in movies] or [modified_dates.movie])

        movies_with_full = self.get_full_info([movie.id for movie in movies_by_persons + movies_by_genres])
        movies = movies + movies_with_full

        return self._transform_movies_info_to_schema(movies), modified_dates

    def extract_genres(self, from_modified_date: datetime, max_records: int) -> list[GenreFromDb]:
        """Get genres which modified after from_modified_date."""
        with self.pg_client.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, name, modified
                FROM content.genre
                WHERE modified > %s
                ORDER BY modified
                LIMIT %s
                """,
                (from_modified_date, max_records),
            )
            return [GenreFromDb(*row) for row in cursor.fetchall()]

    def extract_movies_by_genre_ids(self, genre_ids: list[str]) -> list[MoviesFromDb]:
        """Get movies by genre ids."""
        if not genre_ids:
            return []
        with self.pg_client.cursor() as cursor:
            cursor.execute(
                """
                SELECT fw.id, fw.modified
                FROM content.film_work fw
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                WHERE gfw.genre_id IN %s
                ORDER BY fw.modified
                LIMIT 100;
                """,
                (tuple(genre_ids),),
            )
            return [MoviesFromDb(*row) for row in cursor.fetchall()]

    def extract_modified_movies(self, from_modified_date: datetime, max_records: int) -> list[MoviesWithFull]:
        """Get modified movies."""
        with self.pg_client.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    fw.id as fw_id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    fw.type,
                    fw.created,
                    fw.modified,
                    pfw.role,
                    p.id,
                    p.full_name,
                    g.name
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                WHERE fw.modified > %s
                ORDER BY fw.modified
                LIMIT %s;
                """,
                (from_modified_date, max_records),
            )
            return [MoviesWithFull(*row) for row in cursor.fetchall()]

    @backoff
    def extract_persons(self, from_modified_date: datetime, max_records: int) -> list[PersonFromDb]:
        """Get persons which modified after from_modified_date."""
        with self.pg_client.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, full_name, modified
                FROM content.person
                WHERE modified > %s
                ORDER BY modified
                LIMIT %s
                """,
                (from_modified_date, max_records),
            )

            return [PersonFromDb(*row) for row in cursor.fetchall()]

    @backoff
    def extract_movies_by_person_ids(self, person_ids: list[str]) -> list[MoviesFromDb]:
        """Get movies by person ids."""
        if not person_ids:
            return []
        with self.pg_client.cursor() as cursor:
            cursor.execute(
                """
                SELECT fw.id, fw.modified
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                WHERE pfw.person_id IN %s
                ORDER BY fw.modified
                LIMIT 100;
                """,
                (tuple(person_ids),),
            )

            return [MoviesFromDb(*row) for row in cursor.fetchall()]

    @backoff
    def get_full_info(self, movies_id: list[str]) -> list[MoviesWithFull]:
        """Get full info about movies by id."""
        if not movies_id:
            return []
        with self.pg_client.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    fw.id as fw_id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    fw.type,
                    fw.created,
                    fw.modified,
                    pfw.role,
                    p.id,
                    p.full_name,
                    g.name
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                WHERE fw.id IN %s;
                """,
                (tuple(movies_id),),
            )
            return [MoviesWithFull(*row) for row in cursor.fetchall()]

    def _transform_movies_info_to_schema(self, movies: list[MoviesWithFull]) -> list[MovieSchema]:
        """Transform movies info to schema."""
        movies = sorted(movies, key=lambda x: x.fw_id)
        result: list[MovieSchema] = []
        actors: dict[str, list[PersonFromDb]] = {}
        writers: dict[str, list[PersonFromDb]] = {}
        directors: dict[str, str] = {}

        for movie in movies:
            person = PersonFromDb(id=movie.p_id, name=movie.full_name, modified=movie.modified)
            if movie.role == 'a':
                actors.setdefault(movie.fw_id, set()).add(person)
            elif movie.role == 'w':
                writers.setdefault(movie.fw_id, set()).add(person)
            if movie.role == 'd':
                directors[movie.fw_id] = movie.full_name

            result.append(
                MovieSchema(
                    id=movie.fw_id,
                    imdb_rating=movie.rating or 0,
                    genre=movie.genre,
                    title=movie.title,
                    description=movie.description if movie.description else '',
                    director='',
                    actors_names='',
                    writers_names='',
                    actors=[],
                    writers=[],
                )
            )
        for movie in result:
            movie.actors = [PersonSchema(id=actor.id, name=actor.name) for actor in actors.get(movie.id, [])]
            movie.writers = [
                PersonSchema(id=writer.id, name=writer.name) for writer in writers.get(movie.id, [])
            ]
            movie.actors_names = [actor.name for actor in movie.actors]
            movie.writers_names = [writer.name for writer in movie.writers]
            movie.director = directors.get(movie.id, '')
        return result
