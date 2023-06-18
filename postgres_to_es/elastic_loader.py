"""Module providing class for working with elastic.""" ""
import time
from typing import NamedTuple

from backoff import backoff
from elastic_schema import ELASTIC_SCHEMA
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from models import MovieSchema


class ElasticBasicAuthCredentials(NamedTuple):
    """Basic auth credentials for elastic."""

    username: str
    password: str


class ElasticLoader:
    """Class providing method for working with elastic."""

    def __init__(self, hosts: list[str], basic_auth: ElasticBasicAuthCredentials, index_name: str):
        self.index_name = index_name
        self.hosts = hosts
        self.es = Elasticsearch(**{'hosts': hosts, 'basic_auth': basic_auth, 'verify_certs': False})

    def _generate_actions(self, movies: list[MovieSchema]):
        """Generate actions to elastic."""
        for movie in movies:
            yield {'_index': self.index_name, '_id': movie.id, '_source': movie.dict()}

    def create_index_if_not_exists(self):
        """Create index if not exists."""
        while not self.es.ping():
            time.sleep(1)
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body=ELASTIC_SCHEMA)

    @backoff
    def load_movies(self, movies: list[MovieSchema]):
        """Load movies to elastic."""
        bulk(self.es, list(self._generate_actions(movies)))
