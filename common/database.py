from urllib.parse import quote
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from common.constant import MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_HOST, MYSQL_DB_PORT

Session = None

Base = declarative_base()


def get_db_session(is_test=False):
    db_url = f"mysql+pymysql://{MYSQL_DB_USER}:{quote(MYSQL_DB_PASSWORD)}@{MYSQL_DB_HOST}:{MYSQL_DB_PORT}/ab180?charset=utf8mb4"

    engine = create_db_engine(db_url)
    session = create_db_session(engine)

    return session


def create_db_engine(db_conn_string, debug_mode=False):
    return create_engine(db_conn_string,
                         echo=debug_mode,
                         pool_size=3,
                         max_overflow=5,
                         pool_recycle=3600,
                         pool_pre_ping=True,
                         pool_use_lifo=True)


def create_db_session(engine):
    global Session
    if not Session:
        Session = sessionmaker(bind=engine)
    return Session()
