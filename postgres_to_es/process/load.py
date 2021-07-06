import Elasticsearch

from postgres_to_es.config import ELASTICSEARCH_HOST, ELASTICSEARCH_PORT, ELASTICSEARCH_INDEX


class ESLoader:
    def __init__(self):
        self.es = Elasticsearch([{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}])
        self.es.indices.create(index=ELASTICSEARCH_INDEX, ignore=400)
