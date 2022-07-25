# Native Python libs:
import logging

# 3rd party Python libs:
import pandas as pd
import pendulum
from sqlalchemy import Column, Integer, String, BigInteger, Numeric, Date, DateTime
from sqlalchemy import or_, and_
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects import postgresql

# own modules and libs:
from utils.init_params import opts
from .connect_pg import get_engine, get_session
from utils.datetime import validate_date_from_date_to

engine = get_engine()
session = get_session()

logger = logging.getLogger(f"__main__.db.{__name__}")

Base = declarative_base()

def get_asset_catalog(asset_id=None, asset_name=None):
    """SELECT from dashboards.asset_metadata
    optionally set limits to id and name columns.

    return pandas.DataFrame()

    Example:
    import db.asset as asset
    df1 = asset.get_asset_catalog(asset_id=1)
    df2 = asset.get_asset_catalog(asset_name='aa')
    """

    logger.debug(f"Start get_asset_catalog with asset_id={asset_id}, asset_name={asset_name}")
    where = 'where 1=1'
    if isinstance(asset_id, int):
        where += f" AND id = {asset_id}"
    if isinstance(asset_name, str):
        where += f" AND name ilike '%%{asset_name}%%'"
            
    sql = f"""
        select * 
        from dashboards.asset_metadata
        {where}
        order by 
            name
        """
    logger.debug(f"composed SQL={sql}")
    df = pd.read_sql(sql, engine)
    logger.debug(df.head(5))

    return df


def get_asset_attribute(asset_name=None, attribute_name=None):
    """SELECT from dashboards.asset_attribute
    optionally set limits to asset_name and attribute_name columns.

    return pandas.DataFrame()

    Example:
    import db.asset as asset
    df1 = asset.get_asset_attribute(asset_name='aap')
    df2 = asset.get_asset_attribute(attribute_name='close')
    """

    logger.debug(f"Start get_asset_attribute with asset_name={asset_name}, attribute_name={attribute_name}")
    where = 'where 1=1'
    if isinstance(asset_name, str):
        where += f" AND asset_name ilike '%%{asset_name}%%'"
    if isinstance(attribute_name, str):
        where += f" AND attribute_name ilike '%%{attribute_name}%%'"
            
    sql = f"""
        select asset_name, attribute_name
        from dashboards.asset_attribute
        {where}
        order by 
            asset_name,
            attribute_name
        """
    logger.debug(f"composed SQL={sql}")
    df = pd.read_sql(sql, engine)
    logger.debug(df.head(5))

    return df


class Asset(Base):
    __tablename__ = 'prices'
    datetime = Column(DateTime, primary_key=True)
    asset_id = Column(BigInteger, primary_key=True)
    price = Column(Numeric)
    insertion_timestamp = Column(DateTime)

    def __repr__(self):
        return "<Asset(asset_id='%s', price='%s', datetime='%s')>" % (
                                self.asset_id, self.value, self.datetime)


def get_asset_daily_values_by_id(asset_id=None, date_from=None, date_to=None):
    """SELECT from dashboards.prices
    "Price" shows assets with attribute "Day Close Price" only.
    optionally set limits to asset_id and published_at_cet columns.

    return pandas.DataFrame()

    Example:
    import db.asset as asset
    df1 = asset.get_asset_daily_values_by_id(asset_id=1, date_from=pendulum.datetime(2021, 1, 1))
    df2 = asset.get_asset_daily_values_by_id(asset_id=1, date_from=pendulum.today().subtract(months=3),date_to='2022-02-10')
    """
    filters = list()
    if isinstance(asset_id, int):
        filters.append(Asset.asset_id == asset_id)
    date_from, date_to = validate_date_from_date_to(date_from, date_to)
    if date_from:
        filters.append(Asset.datetime >= date_from)
    if date_to:
        filters.append(Asset.datetime <= date_to)
    if filters:
        logger.debug(session.query(Asset).filter(and_(*filters)).statement.compile(compile_kwargs={"literal_binds": True}))
        df = pd.read_sql(
            session.query(Asset).filter(and_(*filters)).statement,session.bind)
    else:
        logger.debug(session.query(Asset).statement.compile(compile_kwargs={"literal_binds": True}))
        df = pd.read_sql(
            session.query(Asset).statement,session.bind)
    
    return df


class AssetHourlyAvg(Base):
    __tablename__ = 'asset_price_scalar_hourly_avg'
    asset_name = Column(String, primary_key=True)
    attribute_name = Column(String, primary_key=True)
    value = Column(Numeric)
    published_at_cet = Column(DateTime, primary_key=True)
    
    def __repr__(self):
        return "<AssetHourly(asset_name='%s', attribute_name='%s', value='%s', published_at_cet='%s')>" % (
                                self.asset_name, self.attribute_name, self.value, self.published_at_cet)


def get_asset_hourly_avg_values_by_name(asset_name=None, attribute_name=None, date_from=None, date_to=None):
    """SELECT from dashboards.asset_price_scalar_hourly_avg
    optionally set limits to asset_name, attribute_name and published_at_cet columns.

    return pandas.DataFrame()

    Example:
    import db.asset as asset
    df = asset.get_asset_hourly_avg_values_by_name(asset_name='equity_msft', attribute_name='price;day;close', date_from='2022-02-10')
    """
    filters = list()
    if isinstance(asset_name, str):
        filters.append(AssetHourlyAvg.asset_name == asset_name)
    if isinstance(attribute_name, str):
        filters.append(AssetHourlyAvg.attribute_name == attribute_name)
    date_from, date_to = validate_date_from_date_to(date_from, date_to)
    if date_from:
        filters.append(AssetHourlyAvg.published_at_cet >= date_from)
    if date_to:
        filters.append(AssetHourlyAvg.published_at_cet <= date_to)
    if filters:
        df = pd.read_sql(
            session.query(AssetHourlyAvg).filter(and_(*filters)).statement,session.bind)
    else:
        df = pd.read_sql(
            session.query(AssetHourlyAvg).statement,session.bind)
    
    return df



def add_asset_scalar_publication(df = pd.DataFrame()):
    """INSERT padnas.DataFrame content into dashboards.prices view

    Example:
    df = pd.DataFrame({
        "asset_id": [1, 1, 1],
        "price":[101,102,103],
        "datetime":["2022-06-01 19:00:00","2022-06-02 19:00:00","2022-06-03 19:00:00"]
        })
    add_asset_scalar_publication(df)
    """

    logger.debug(f"Start add_asset_scalar_publication")
    logger.debug(f"Get data for INSERT:")
    logger.debug(df.head(5))

    # try:
    #     df = df[['datetime','asset_id','price']]
    #     with engine.begin() as conn:
    #         df.to_sql('prices', conn, if_exists='append', schema='dashboards', index=False, chunksize=1000)
    #         if opts.dryrun:
    #              conn.rollback()
    #              logger.debug("Success on INSERT simulation into prices. Rolling back")

    #         logger.debug("Success on INSERT into prices")
    try:
        df = df[['datetime','asset_id','price']]
        with engine.connect() as conn:
            tran = conn.begin()
            df.to_sql('prices', conn, if_exists='append', schema='dashboards', index=False, chunksize=1000)
            if opts.dryrun:
                 tran.rollback()
                 logger.debug("Success on INSERT simulation into prices. Rolling back")
            else:
                tran.commit()
                logger.debug("Success on INSERT into prices")
    except Exception as e:
        logger.error(f"{e}")

# new_record = PortfolioReturn(customer_id=2, portfolio_return=0.1, date='2022-07-01')
# print(new_record.portfolio_return)
# session.add(new_record)
# Commit the changes
# session.commit()
# Close the session
# session.close()
