# Native Python libs:
import logging

# 3rd party Python libs:
import pandas as pd

# own modules and libs:
from utils.init_params import opts
from .connect_pg import get_engine, get_session
from utils.datetime import validate_date_from_date_to

engine = get_engine()
session = get_session()

logger = logging.getLogger(f"__main__.db.{__name__}")

def prepare_sql_where_filter(customer_id=None, date_from=None, date_to=None):
    where = 'where 1=1'
    date_from, date_to = validate_date_from_date_to(date_from, date_to)
    if isinstance(customer_id, int):
        where += f" AND customer_id = {customer_id}"
    if date_from:
        where += " AND date >= '{date}'::date".format(date=date_from.format('YYYY-MM-DD'))
    if date_to:
        where += " AND date <= '{date}'::date".format(date=date_to.format('YYYY-MM-DD'))
    
    return where


def get_portfolio_return_monthly(customer_id=None, date_from=None, date_to=None):
    """SELECT from dashboards.portfolio_return_monthly
    optionally set limits to customer_id and date columns.

    return pandas.DataFrame()

    Example:
    df1 = get_portfolio_return_monthly(customer_id=1, date_from=pendulum.datetime(2021, 1, 1))
    df2 = get_portfolio_return_monthly(customer_id=1, date_from=pendulum.today().subtract(months=3))
    """

    logger.debug(f"Start get_portfolio_return_monthly with customer_id={customer_id}, date_from={str(date_from)}")
    where = prepare_sql_where_filter(customer_id=customer_id, date_from=date_from, date_to=date_to)
            
    sql = f"""
        select 
        to_char(date, 'yyyy-mm') as month, customer_id, portfolio_return as monthly_return 
        from dashboards.portfolio_return_monthly
        {where}
        order by 
            date desc,
            customer_id
        """
    logger.debug(f"composed SQL={sql}")
    df = pd.read_sql(sql, engine)
    logger.debug(df.head(5))

    return df


def get_portfolio_return(customer_id=None, date_from=None, date_to=None):
    """SELECT from dashboards.portfolio_return
    optionally set limits to customer_id and date columns.

    return pandas.DataFrame()

    Example:
    df1 = get_portfolio_return(customer_id=1, date_from=pendulum.datetime(2021, 1, 1))
    df2 = get_portfolio_return(customer_id=1, date_from=pendulum.today().subtract(months=3))
    """

    logger.debug(f"Start get_portfolio_return with customer_id={customer_id}, date_from={str(date_from)}")
    where = prepare_sql_where_filter(customer_id=customer_id, date_from=date_from, date_to=date_to)
        
    sql = f"""
        select * 
        from dashboards.portfolio_return
        {where}
        order by 
            date desc,
            customer_id
        """
    logger.debug(f"composed SQL={sql}")
    df = pd.read_sql(sql, engine)
    logger.debug(df.head(5))
    
    return df


def add_portfolio_return(df = pd.DataFrame()):
    """INSERT padnas.DataFrame content into dashboards.portfolio_return view

    Example:
    df = pd.DataFrame({
        "customer_id": [1, 1, 1],
        "portfolio_return":[0.2,0.3,0.4],
        "date":["2022-06-01","2022-06-02","2022-06-03"]
        })
    add_portfolio_return(df)
    """

    logger.debug(f"Start add_portfolio_return")
    logger.debug(f"Get data for INSERT:")
    logger.debug(df.head(5))

    # try:
    #     df = df[['customer_id','portfolio_return','date']]
    #     with engine.begin() as conn:
    #         df.to_sql('portfolio_return', conn, if_exists='append', schema='dashboards', index=False, chunksize=1000)
    #         if opts.dryrun:
    #              conn.rollback()
    #              logger.debug("Success on INSERT simulation into portfolio_return. Rolling back")

    #         logger.debug("Success on INSERT into portfolio_return")
    try:
        df = df[['customer_id','portfolio_return','date']]
        with engine.connect() as conn:
            tran = conn.begin()
            df.to_sql('portfolio_return', conn, if_exists='append', schema='dashboards', index=False, chunksize=1000)
            if opts.dryrun:
                 tran.rollback()
                 logger.debug("Success on INSERT simulation into portfolio_return. Rolling back")
            else:
                tran.commit()
                logger.debug("Success on INSERT into portfolio_return")
    except Exception as e:
        logger.error(f"{e}")

