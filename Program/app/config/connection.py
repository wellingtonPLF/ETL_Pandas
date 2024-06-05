from dotenv import load_dotenv
import os
import psycopg2
from sqlalchemy import create_engine
from urllib.parse import quote_plus

from ..shared import utils as utils

load_dotenv(utils.getEnvPath())

db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
#db_schema = os.getenv("DB_SCHEMA")

def getConnection(): 
    conn = psycopg2.connect(
        database=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port)
    return conn

def getConnectionSqlAlchemyGetEngine(): 
    db_pass_encoded = quote_plus(db_pass)
    url_conexao = f'postgresql://{db_user}:{db_pass_encoded}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(url_conexao)
    return engine

def getConnectionSqlAlchemyGetEngineApi(): 
    db_pass_encoded = quote_plus(db_pass)
    url_conexao = f'postgresql://{db_user}:{db_pass_encoded}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(url_conexao)
    conn = engine.connect()
    return conn

# ------------------------------------------------------------------------------------------------------------