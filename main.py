from datetime import datetime
import schedule # библиотека для планировщика задач

from log.loger import logger
from source.working_state import State, last_modif_time
from etl import formation, update_el

def main():

    logger.info('старт главной функции main')    
        
    st = State()
    last_modif_films = st.get_state('last_modif_films')
    logger.info(f'время последней модицикации фильмов {last_modif_films}')
    if last_modif_films:
        l_modif_time = last_modif_time('filmwork')
        if not l_modif_time:
            logger.error('!!!!!!!!!!!!!!!!!!!!!ошибка в работе БД!!!!!!!!!!!!!!!!!!!!!!!!!!')
            return True
        if datetime.datetime.strptime(last_modif_films, '%Y-%m-%d %H:%M:%S.%f%z') < l_modif_time:
            logger.info('В таблице filmwork есть обновленные данные, инициирую актуализацию данных')
            formation(st,last_modif_films)
        last_modif_pers = st.get_state('last_modif_pers')
        l_modif_time = last_modif_time('person')
        if not l_modif_time:
            logger.error('!!!!!!!!!!!!!!!!!!!!!ошибка в работе БД!!!!!!!!!!!!!!!!!!!!!!!!!!')
            return True
        if datetime.datetime.strptime(last_modif_pers, '%Y-%m-%d %H:%M:%S.%f%z') < l_modif_time:
            logger.info('В таблице person есть обновленные данные, инициирую актуализацию данных')
            update_el('person', last_modif_pers)
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