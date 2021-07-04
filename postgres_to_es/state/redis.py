import abc
import json
from datetime import datetime

from redis import Redis, exceptions
from typing import Any

from postgres_to_es.config import REDIS_HOST, REDIS_DICT


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass
    
    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter
    
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        self.redis_adapter.set(REDIS_DICT, json.dumps(state))
    
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        data = self.redis_adapter.get(REDIS_DICT)
        if data is None:
            return {}
        return json.loads(data)


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
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
        return state.get(key)


if __name__ == '__main__':
    """
    Пример использования
    Обратите внимание на приведение datetime к строке и наоборот
    """
    key_name = 'producer'
    storage = RedisStorage(Redis(REDIS_HOST))
    print(Redis(REDIS_HOST))
    state = State(storage)
    
    try:
        created_at = state.get_state(key_name)
    except exceptions.ConnectionError as error_message:
        print(error_message)
    else:
        print(f'Value of key {key_name} is', created_at, type(created_at))
        
        state.set_state(key_name, datetime.now().isoformat())
        created_at = datetime.fromisoformat(state.get_state(key_name))
        print(f'Value of key {key_name} is', created_at, type(created_at))
