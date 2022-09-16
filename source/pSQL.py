import psycopg2
import os
import time
from psycopg2.extras import execute_batch, DictCursor
from source.settings import DSN


# функция выгрузки данных из pSQL
def psql_unload(req):
    try:
        t = 0.1
        for _ in range(100):
            try:
                with psycopg2.connect(**DSN, cursor_factory=DictCursor) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(req)
                    p = cur.fetchall()
                    return p
            except psycopg2.OperationalError:
                time.sleep(t)
                if t < 10:
                    t*=2
        return False
    except Exception:
        return False

# функция выполнения запросов изменения данных pSQL
def psql_up(req):
    try:
        t = 0.1
        q = 0
        for _ in range(100):
            print(t)
            print(q)
            try:
                with psycopg2.connect(**DSN) as conn, conn.cursor() as cur:
                    cur.execute(req)
                    if cur.rowcount:
                        return True
                    return False
            except psycopg2.DatabaseError:
                time.sleep(t)
                if t < 10:
                    t*=2
                q += 1
        print('nen')
        return False
    except:
        return False


if __name__ == '__main__':
    print('функции для работы с бд pSQL')