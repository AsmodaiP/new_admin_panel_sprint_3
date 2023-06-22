"""Module for working with state."""
import abc
import datetime
import json
from enum import Enum
from os import getenv
from typing import Any

import redis


class StorageType(Enum):
    """Types of storage."""

    JSON = 'json'
    REDIS = 'redis'


class StorageFactory:
    """Factory for creating storage."""

    @staticmethod
    def get_storage(storage_type: StorageType):
        """Get storage by type."""
        if storage_type == StorageType.JSON.value:
            return JsonFileStorage('state.json')
        elif storage_type == StorageType.REDIS.value:
            return RedisStorage(
                redis.Redis(host=getenv('REDIS_HOST', 'localhost'), port=getenv('REDIS_PORT', 6379))
            )
        else:
            storage_types = ', '.join([storage_type.value for storage_type in StorageType])
            raise ValueError(
                'Unknown storage type {} provided. You can use only {}.'.format(storage_type, storage_types)
            )


class BaseStorage(abc.ABC):
    """Abstract class for storage."""

    @abc.abstractmethod
    def save_state(self, state: dict[str, Any]) -> None:
        """Save state."""

    @abc.abstractmethod
    def retrieve_state(self) -> dict[str, Any]:
        """Get state."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        json.dump(state, open(self.file_path, 'w'))

    def retrieve_state(self) -> dict[str, Any]:
        """Получить состояние из хранилища."""
        try:
            return json.load(open(self.file_path, 'r'))
        except FileNotFoundError:
            return {}


class RedisStorage(BaseStorage):
    """Redis storage."""

    def __init__(self, redis_client: redis.Redis) -> None:
        self.redis_client = redis_client

    def save_state(self, state: dict[str, Any]) -> None:
        """Save state in redis."""
        self.redis_client.set('state', json.dumps(state))

    def retrieve_state(self) -> dict[str, Any]:
        """Get state from redis."""
        redis_state = self.redis_client.get('state')
        if not redis_state:
            return {}
        return json.loads(redis_state) or {}


class State:
    """Class for working with state."""

    def __init__(self, type_of_storage: StorageType) -> None:
        self.storage = StorageFactory.get_storage(type_of_storage)

    def set_state(self, key: str, value: Any) -> None:
        """Set state by key, value."""
        st = self.storage.retrieve_state()
        st[key] = value
        self.storage.save_state(st)

    def get_state(self, key: str) -> Any:
        """Get state by key."""
        return self.storage.retrieve_state().get(key)


class MoviesStateManager(State):
    """Class for working with state of movies."""

    def __init__(self, type_of_storage: StorageType, date_format: str = '%Y-%m-%d %H:%M:%S.%f%z') -> None:
        super().__init__(type_of_storage)
        self._date_format = date_format

    def _get_date_or_none(self, key: str) -> datetime.datetime | None:
        """Get date from state or None."""
        date_as_str = self.get_state(key)
        if date_as_str is None:
            return None
        return datetime.datetime.strptime(date_as_str, self._date_format)

    @property
    def last_date_of_modified_movie(self) -> datetime.datetime | None:
        """Get date of last modified movie."""
        return self._get_date_or_none('last_date_of_modified_film_work')

    @last_date_of_modified_movie.setter
    def last_date_of_modified_movie(self, value: datetime.datetime) -> None:
        """Set date of last modified movie."""
        self.set_state('last_date_of_modified_film_work', value.strftime(self._date_format))

    @property
    def last_date_of_modified_person(self) -> datetime.datetime | None:
        """Get date of last modified person."""
        return self._get_date_or_none('last_date_of_modified_person')

    @last_date_of_modified_person.setter
    def last_date_of_modified_person(self, value: datetime.datetime) -> None:
        """Установить дату последнего изменения персоны."""
        self.set_state('last_date_of_modified_person', value.strftime(self._date_format))

    @property
    def last_date_of_modified_genre(self) -> datetime.datetime | None:
        """Get date of last modified genre."""
        return self._get_date_or_none('last_date_of_modified_genre')

    @last_date_of_modified_genre.setter
    def last_date_of_modified_genre(self, value: datetime.datetime) -> None:
        """Set date of last modified genre."""
        self.set_state('last_date_of_modified_genre', value.strftime(self._date_format))


class MockStorage(BaseStorage):
    """Mock storage."""

    def __init__(self) -> None:
        self._state = {}

    def save_state(self, state: dict[str, Any]) -> None:
        """Save state."""
        self._state.update(state)

    def retrieve_state(self) -> dict[str, Any]:
        """Get state."""
        return self._state
