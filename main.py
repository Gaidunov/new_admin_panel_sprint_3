from datetime import datetime
import json
from typing import Any
from source.pSQL import psql_unload
from pydantic import BaseModel, Field
from source.elastic import load_el_bulk
from loguru import logger # библиотека для логирования
import datetime
import schedule # библиотека для планировщика задач


logger.add('log/tag_push_os.log', level='DEBUG', rotation='1024 MB', compression='zip')

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





# функция выгрузки данных из psql
def unloading_psql(date = False, table = False, id_record = False):
    if date:
        condition = f"{table}.modified > '{date}'"
    else:
        condition = f"{table}.id = '{id_record}'"
    data_p = psql_unload("""
                            SELECT
                            fw.id,
                            fw.title,
                            fw.description,
                            fw.rating,
                            fw.type,
                            fw.created,
                            fw.modified,
                            COALESCE (
                                json_agg(
                                    DISTINCT jsonb_build_object(
                                        'person_role', pfw.role,
                                        'person_id', p.id,
                                        'person_name', p.full_name
                                    )
                                ) FILTER (WHERE p.id is not null),
                                '[]'
                            ) as persons,
                            array_agg(DISTINCT g.name) as genres
                            FROM content.filmwork fw
                            LEFT JOIN content.person_filmwork pfw ON pfw.film_work_id = fw.id
                            LEFT JOIN content.person p ON p.id = pfw.person_id
                            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                            LEFT JOIN content.genre g ON g.id = gfw.genre_id
                            WHERE """+condition+"""
                            GROUP BY fw.id
                            ORDER BY fw.modified
                            LIMIT 100;
                            """
                        )
    return data_p


def last_modif_time(table):
    date = psql_unload(f'select max(modified) from content.{table};')[0][0]
    return date




# функция загрузки данных в elastik
# def loading_el():


def main():

    logger.info('старт главной функции main')
    
    def formation(st, last_modif_films = False, table = 'fw'):
        amount_downloads = 0
        while True:
            st.set_state('last_modif_films', str(last_modif_films)) # сохраняем состояние последней загрузки на случай выхода из строя хранилищь, что бы загрузка началась с того же момента
            if last_modif_films:
                logger.info(f'грузим данные сортирую из таблицы {table}, после {last_modif_films}')
                data_p = unloading_psql(date = str(last_modif_films), table = table)
            else:
                logger.info('первая выгрузка из БД начинаем с 2021-06-15')
                data_p = unloading_psql(date = '2021-06-15', table = table) # дата первой загрузки данных у меня
            if data_p:
                logger.info(f'выгрузка завершена колличество фильмов {len(data_p)}')
                data_processed = []
                logger.info('начинаю перебор результатов, валидацию и формирование фильмов на загрузку')
                for i_film in data_p:
                    i_film = dict(i_film)
                    logger.info(f"работаю с фильмом {i_film['title']}")
                    q = valid_and_transform(i_film)
                    if not q:
                        logger.error(f"ОШИБКА!!!! Ошибка валидации данных. фильм\n\n{i_film}\n\n")
                        continue
                    last_modif_films = i_film['modified']
                    logger.info(f'фильм {q.title} обработан, добавляем его в список для загрузки и идем дальше')
                    data_processed.append({"_index": "movies","_type": "_doc","_id":q.id,"_source": q.dict()})
                # грузим все в эластик
                logger.info(f'все фильмы обработаны, составлен список. Колличество фильмов в списке {len(data_processed)}. Начинаю загрузку в эластик')
                result = load_el_bulk(data_processed)
                if not result:
                    logger.error('!!!!!!!!!!!!!!!!!!!!!ошибка в работе elastic!!!!!!!!!!!!!!!!!!!!!!!!!!')
                    return False
                logger.warning(f'Документы загружены. INFO {result}')
                amount_downloads += result[0]
                # return True
            else:
                if data_p == False:
                    logger.error('!!!!!!!!!!!!!!!!!!!!!ошибка в работе БД!!!!!!!!!!!!!!!!!!!!!!!!!!')
                    return False
                logger.info(f'в выгрузке нет больше данных, записываю поледнюю дату модификации таблицы в файл. колличество загруженных фильмов {amount_downloads}')
                st.set_state('last_modif_films', str(last_modif_films))
                return True

    def update_el(table, date_sort):
        logger.info(f'начинаем обновление данных в таблице "{table}"')
        amount_downloads = 0
        id_fields = psql_unload(f"select id from content.{table} where modified > '{date_sort}';")
        if not id_fields:
            logger.error('!!!!!!!!!!!!!!!!!!!!!ошибка в работе БД!!!!!!!!!!!!!!!!!!!!!!!!!!')
            return False
        logger.info(f'Колличество id записей для обновления {len(id_fields)}')
        for id_field in id_fields:
            data_p = unloading_psql(table = table[0], id_record = id_field[0])
            if data_p:
                logger.info(f'выгрузка завершена колличество фильмов {len(data_p)}')
                logger.info('начинаю перебор результатов, валидацию и формирование фильмов на загрузку')
                data_processed = []
                for i_film in data_p:
                    i_film = dict(i_film)
                    logger.info(f"работаю с фильмом {i_film['title']}")
                    q = valid_and_transform(i_film)
                    if not q:
                        logger.error(f"ОШИБКА!!!! Ошибка валидации данных. фильм\n\n{i_film}\n\n")
                        continue
                    logger.info(f'фильм {q.title} обработан, добавляем его в список для загрузки и идем дальше')
                    data_processed.append({"_index": "movies","_type": "_doc","_id":q.id,"_source": q.dict()})
                # грузим все в эластик
                logger.info(f'все фильмы обработаны, составлен список. Колличество фильмов в списке {len(data_processed)}. Начинаю загрузку в эластик')
                # print(data_processed)
                result = load_el_bulk(data_processed)
                if not result:
                    logger.error('!!!!!!!!!!!!!!!!!!!!!ошибка в работе elastic!!!!!!!!!!!!!!!!!!!!!!!!!!')
                    return False
                logger.warning(f'Документы загружены. INFO {result}')
                amount_downloads += result[0]
            else:
                if data_p == False:
                    logger.error('!!!!!!!!!!!!!!!!!!!!!ошибка в работе БД!!!!!!!!!!!!!!!!!!!!!!!!!!')
                    return False
                logger.info(f'в выгрузке нет больше данных. колличество загруженных фильмов {amount_downloads}')
        logger.info('обновление данных прошло успешно')

    class V_films(BaseModel):
        id : str
        imdb_rating : float = Field(alias='rating')
        genre: list = Field(alias='genres')
        title : str
        description : str = None
        director: list
        actors_names: list
        writers_names: list
        actors: list
        writers: list

    def valid_and_transform(i_film):
        try:
            actor = []
            writer = []
            director = []
            i_film['actors'] = []
            i_film['writers'] = []
            for person in i_film['persons']:
                match person['person_role']:
                    case 'actor':
                        actor.append(person['person_name'])
                        i_film['actors'].append({'id': person['person_id'],'name': person['person_name']})
                    case 'writer':
                        writer.append(person['person_name'])
                        i_film['writers'].append({'id': person['person_id'],'name': person['person_name']})
                    case 'director':
                        director.append(person['person_name'])
            i_film['director'] = director
            i_film['actors_names'] = actor
            i_film['writers_names'] = writer
            i_film['persons'] = {'actor':actor, 'writer':writer,'director':director}
            # print(i_film)
            q = V_films.parse_obj(i_film)
            return q
        except Exception:
            return False
        
    st = State()
    last_modif_films = st.get_state('last_modif_films')
    logger.info(f'время последней модицикации фильмов {last_modif_films}')
    if last_modif_films:
        logger.info('Запись о времени последней модификации есть. Начинаю проверку актуальности')
        logger.info('проверка таблицы filmwork')
        l_modif_time = last_modif_time('filmwork')
        if not l_modif_time:
            logger.error('!!!!!!!!!!!!!!!!!!!!!ошибка в работе БД!!!!!!!!!!!!!!!!!!!!!!!!!!')
            return True
        if datetime.datetime.strptime(last_modif_films, '%Y-%m-%d %H:%M:%S.%f%z') < l_modif_time:
            logger.info('В таблице filmwork есть обновленные данные, инициирую актуализацию данных')
            formation(st,last_modif_films)
        logger.info('проверка таблицы person')
        last_modif_pers = st.get_state('last_modif_pers')
        l_modif_time = last_modif_time('person')
        if not l_modif_time:
            logger.error('!!!!!!!!!!!!!!!!!!!!!ошибка в работе БД!!!!!!!!!!!!!!!!!!!!!!!!!!')
            return True
        if datetime.datetime.strptime(last_modif_pers, '%Y-%m-%d %H:%M:%S.%f%z') < l_modif_time:
            logger.info('В таблице person есть обновленные данные, инициирую актуализацию данных')
            update_el('person', last_modif_pers)
        logger.info('проверка таблицы genre')
        last_modif_genre = st.get_state('last_modif_genre')
        l_modif_time = last_modif_time('genre')
        if not l_modif_time:
            logger.error('!!!!!!!!!!!!!!!!!!!!!ошибка в работе БД!!!!!!!!!!!!!!!!!!!!!!!!!!')
            return True
        if datetime.datetime.strptime(last_modif_genre, '%Y-%m-%d %H:%M:%S.%f%z') < l_modif_time:
            logger.info('В таблице genre есть обновленные данные, инициирую актуализацию данных')
            update_el('genre', last_modif_genre) 
        logger.info('Данные о фильмах актуалтны')
        return True # завершаем работу функции
    logger.info('Это первый старт инициирую первоначальную загрузку данных')
    if not formation(st):
        logger.error('выходим из программы ждем следующей загрузки (глядишь востановится)')
        return True
    last_modif_pers = last_modif_time('person')
    st.set_state('last_modif_pers', str(last_modif_pers))
    last_modif_genre = last_modif_time('genre')
    st.set_state('last_modif_genre', str(last_modif_genre))



if __name__ == '__main__':
    # main()
    logger.warning(f'Старт ETL сервиса')
    schedule.every(15).minutes.do(main)

    while True:
            schedule.run_pending()