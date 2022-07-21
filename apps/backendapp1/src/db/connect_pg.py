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
from utils.init_params import opts


# Get root (__main__) logger:
logger = logging.getLogger(f"__main__.{__name__}")


Base = declarative_base()

engine = None
session = None

def connect_engine():
    """Build SQLAlchemy engine
    """

    logger.debug('Build SQLAlchemy engine')

    opts.db_constr = 'postgresql://{user}:{passwd}@{host}:{port}/{db_name}'.format(
            user=opts.db_user,
            passwd=opts.db_pass,
            host=opts.db_host,
            port=opts.db_port,
            db_name=opts.db_name
            )

    return create_engine(opts.db_constr, echo=False, connect_args={'options': '-c search_path={}'.format('dashboards')})
    

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