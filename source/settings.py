from dotenv import load_dotenv
from elasticsearch import Elasticsearch

from pathlib import Path
import os

load_dotenv()
env_path = Path('.')/'.env'
load_dotenv(dotenv_path=env_path)

CONNECT_ES = os.getenv("CONNECT_ES")
ES = Elasticsearch(CONNECT_ES)

DSN = {
    'dbname': 'movies_database',
    'user': os.getenv('PSQL_LOGIN'),
    'password': os.getenv('PSQL_PASS'),
    'host': os.getenv('PSQL_ADRESS'),
    'port': os.getenv('PSQL_PORT'),
    'options': '-c search_path=content',
}

