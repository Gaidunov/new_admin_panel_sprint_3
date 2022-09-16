from loguru import logger # библиотека для логирования


logger.add('log/tag_push_os.log', level='DEBUG', rotation='1024 MB', compression='zip')