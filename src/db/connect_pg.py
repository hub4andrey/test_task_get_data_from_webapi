# Native Python libs:
import logging
import sys

# 3rd party Python libs:
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, BigInteger, Numeric, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import pendulum

# own modules and libs:
from utils.init_params import config

# Get root (__main__) logger:
logger = logging.getLogger(f"__main__.{__name__}")


Base = declarative_base()

engine = None
session = None

def connect_engine():
    """Build SQLAlchemy engine
    """

    logger.debug('Build SQLAlchemy engine')
    config['DB_CONSTR'] = 'postgresql://{user}:{passwd}@{host}:{port}/{db_name}'.format(
            user=config['DB_USER'],
            passwd=config['DB_PASS'],
            host=config['DB_HOST'],
            port=config['DB_PORT'],
            db_name=config['DB_NAME']
            )

    return create_engine(config['DB_CONSTR'], echo=False, connect_args={'options': '-c search_path={}'.format(config['DB_SCHEMA'])})

try:
    engine = connect_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
except SQLAlchemyError as e:
    logger.error(f"{e}")
    sys.exit(1)

def get_engine():
    return engine

def get_session():
    return session