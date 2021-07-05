import datetime
import json
import logging
from abc import ABC, abstractmethod
from typing import Any

from redis import Redis

from postgres_to_es.config import STATE_DB


# class DateTimeEncoder(json.JSONEncoder):
#
#     def default(self, obj):
#         if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
#             return obj.isoformat()
#         elif isinstance(obj, str):
#             try:
#                 datetime.time.strptime(obj, "%Y-%m-%dT%H:%M:%S")
#             except ValueError:
#                 return super(DateTimeEncoder, self).default(obj)
#             return datetime.datetime.fromisoformat(obj)
#         return super(DateTimeEncoder, self).default(obj)


class BaseStorage(ABC):
    @abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass
    
    @abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class RedisStorage(BaseStorage):
    """
    Класс хранилища, в котором будут запоминаться состояния.
    В данной реализации хранилищем выступает Redis.
    """

    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter
    
    def save_state(self, state: dict) -> None:
        """
        Сохранить состояние в постоянное хранилище.
        :param state: словарь состояний
        """
        # encoder = DateTimeEncoder()
        # state = encoder.encode(state)
        self.redis_adapter.set(STATE_DB, json.dumps(state))
    
    def retrieve_state(self) -> dict:
        """
        Загрузить состояние локально из постоянного хранилища.
        """
        data = self.redis_adapter.get(STATE_DB)
        if data is None:
            return {}
        return json.loads(data)


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    """
    
    def __init__(self, storage: BaseStorage):
        self.storage = storage
    
    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        state = self.storage.retrieve_state()
        state[key] = value
        self.storage.save_state(state)
    
    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state = self.storage.retrieve_state()
        # encoder = DateTimeEncoder()
        # state = encoder.encode(state)
        # print(state, type(state))
        return state.get(key)
