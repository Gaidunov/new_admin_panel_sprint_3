import json

from typing import Any
from source.pSQL import psql_unload


class JsonFileStorage():
        
    def retrieve_state():
        # чтение из файла
        try:
            with open('source\condition.json', "r") as readfile:
                state = json.load(readfile)
        except:
            state = {}
        return state
            
    def save_state(state: dict):
        # запись в файл
        with open('source\condition.json', "w") as writefile:
            json.dump(state, writefile)

class State:

    def __init__(self):
        self.storage = JsonFileStorage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage[key] = value
        state = self.storage
        JsonFileStorage.save_state(state)
        return True

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        try:
            return self.storage.get(key)
        except KeyError:
            return None

def last_modif_time(table):
    date = psql_unload(f'select max(modified) from content.{table};')[0][0]
    return date