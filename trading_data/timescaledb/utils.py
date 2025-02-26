import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


TIMESCALEDB_URI = os.getenv('TIMESCALEDB_URI')


def get_db_client():
    # Connect to TimescaleDB
    engine = create_engine(TIMESCALEDB_URI)
    # engine = create_engine('postgresql+psycopg2://username:password@localhost/dbname')
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

