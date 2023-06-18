"""Models for the ETL process."""
import pydantic


class PersonSchema(pydantic.BaseModel):
    """Person schema."""

    id: str
    name: str

    class Config:
        orm_mode = True


class MovieSchema(pydantic.BaseModel):
    """Movie schema."""

    id: str
    imdb_rating: float
    genre: str
    title: str
    description: str
    director: str
    actors_names: str
    writers_names: str
    actors: list[PersonSchema]
    writers: list[PersonSchema]

    class Config:
        orm_mode = True
