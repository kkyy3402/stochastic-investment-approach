import pymysql
from decouple import config

DB_HOST = config('DB_HOST')
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_NAME = config('DB_NAME')
DART_API_KEY = config('DART_API_KEY')


def get_db_connection():
    return pymysql.connect(host=DB_HOST,
                           user=DB_USER,
                           password=DB_PASSWORD,
                           db=DB_NAME,
                           charset='utf8mb4')
