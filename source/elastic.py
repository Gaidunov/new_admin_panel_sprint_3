from unittest import result
from elasticsearch import Elasticsearch, helpers, exceptions
import time
from source.settings import ES




# загрузка пачкой 
def load_el_bulk(data_processed):
    try:
        t = 0.1
        for _ in range(100):
            try:
                result = helpers.bulk(ES, data_processed)
                return result
            except exceptions.ConnectionError:
                time.sleep(t)
                if t < 10:
                    t*=2
        return False
    except Exception:
        return False


if __name__ == '__main__':
    print('файл с функцией для загрузки данных в эластик')