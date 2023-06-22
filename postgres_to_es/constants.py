"""Constants for the project."""

MOVIE_TABLE_NAME = 'film_work'
JUNCTION_TABLES = {
    MOVIE_TABLE_NAME: None,
    'person': 'person_film_work',
    'genre': 'genre_film_work',
}
