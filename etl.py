from pydantic import BaseModel, Field

from source.elastic import load_el_bulk
from log.loger import logger
from source.pSQL import psql_unload


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

def valid_and_transform(i_film: dict) -> object:
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
    except KeyError or ValueError or NameError:
        return False

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
            data_processed = []
            for i_film in data_p:
                i_film = dict(i_film)
                logger.info(f"работаю с фильмом {i_film['title']}")
                q = valid_and_transform(i_film)
                print(type(q))
                if not q:
                    logger.error(f"ОШИБКА!!!! Ошибка валидации данных. фильм\n\n{i_film}\n\n")
                    continue
                last_modif_films = i_film['modified']
                logger.info(f'фильм {q.title} обработан, добавляем его в список для загрузки и идем дальше')
                data_processed.append({"_index": "movies","_type": "_doc","_id":q.id,"_source": q.dict()})
            # грузим все в эластик
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
            data_processed = []
            for i_film in data_p:
                i_film = dict(i_film)
                logger.info(f"работаю с фильмом {i_film['title']}")
                q = valid_and_transform(i_film)
                if not q:
                    logger.error(f"ОШИБКА!!!! Ошибка валидации данных. фильм\n\n{i_film}\n\n")
                    continue
                data_processed.append({"_index": "movies","_type": "_doc","_id":q.id,"_source": q.dict()})
            # грузим все в эластик
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