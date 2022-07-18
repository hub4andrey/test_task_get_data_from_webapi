# Native Python libs:
from pathlib import Path as path
import logging

# 3rd party Python libs:
import pandas as pd
import pendulum

# own modules and libs:
from utils.init_params import config
from utils.init_logger import set_logger

import db.portfolio as db_portfolio
import db.asset as db_asset

import web_api.portfolio as web_portfolio
import web_api.asset as web_asset

from report import portfolio as report_portfoliod
from report import postoffice




# =============================
# Logger. Configuration
# =============================
logger = logging.getLogger(__name__)
# set default level:
logger = set_logger(logger, console_level = logging.DEBUG, file_level = logging.INFO, dir_log = config['DIR_LOG'])


logger.info(f'START')

def get_portfolio_return_update():
    """
    Call web API to get CSV convent .
    Convert output to Pandas DataFrame.
    Write output inoto DataBase.
    """
    logger.debug("Start get_portfolio_return_update")
    df = web_portfolio.get_portfolio_return_from_api()
    db_portfolio.add_portfolio_return(df)


def get_asset_plublication_update():
    """
    Call web API to get CSV convent .
    Convert output to Pandas DataFrame.
    Write output inoto DataBase.
    """
    logger.debug("Start get_asset_plublication_update")
    df = web_asset.get_asset_publications_from_api()
    print(df)
    db_asset.add_asset_scalar_publication(df)


def send_email_report_portfolio_daily_update():
    """Send report by email

    The output from monthly portfolio return calculation is sent via email. It should have the following structure:

    month customer_id monthly_return
    2022-01 1245 0.04236
    2022-01 5458 0.05647
    2022-02 1245 0.00024
    ...
    """
    report_html = report_portfoliod.daily_update_1( 
        df_portfolio=db_portfolio.get_portfolio_return_monthly(customer_id=1)
        )

    postoffice.send(
        to='4_andreych@mail.ru',
        subject='Portfolio. Daily update',
        html=report_html
    )


logger.info(f'COMPLETE')


if __name__ == '__main__':
    get_portfolio_return_update()
    get_asset_plublication_update()
    send_email_report_portfolio_daily_update()



# print(db_portfolio.get_portfolio_return())
# print(db_portfolio.get_portfolio_return_monthly(customer_id=1, date_from=pendulum.datetime(2022, 2, 1), date_to=pendulum.datetime(2022, 3, 1)))

# df = web_portfolio.get_portfolio_return_from_api()
# print(df.to_html())

# df = pd.DataFrame({
#     "customer_id": [1, 1, 1],
#     "portfolio_return":[0.2,0.3,0.4],
#     "date":["2022-06-01","2022-06-02","2022-06-03"]
#     })
# db_portfolio.add_portfolio_return(df)




# print(db_asset.get_asset_daily_values_by_id(asset_id=1, date_from=pendulum.datetime(2022, 2, 1), date_to=pendulum.datetime(2022, 3, 1)))
# print(db_asset.get_asset_daily_values_by_id(asset_id=2))
# print(db_asset.get_asset_daily_values_by_id())
# print(db_asset.get_asset_daily_values_by_id(asset_id=1, date_from=pendulum.today().subtract(months=6),date_to='2022-10-10') )
# )

# print(db_asset.get_asset_hourly_avg_values_by_name())
# print(db_asset.get_asset_hourly_avg_values_by_name(asset_name='equity_msft'))
# print(db_asset.get_asset_hourly_avg_values_by_name(asset_name='equity_msft', attribute_name='price;day;close'))
# print(db_asset.get_asset_hourly_avg_values_by_name(asset_name='equity_msft', attribute_name='price;day;close', date_from=pendulum.today().subtract(months=6),date_to='2022-10-10'))


# print(db_asset.get_asset_catalog(asset_name='aa'))
# print(db_asset.get_asset_attribute(asset_name='aap'))
# print(db_asset.get_asset_attribute(attribute_name='close'))


# df = web_asset.get_asset_publications_from_api()
# print(df)

# df = pd.DataFrame({
#         "asset_id": [1, 1, 1],
#         "price":[101,102,103],
#         "datetime":["2022-06-01 19:00:00","2022-06-02 19:00:00","2022-06-03 19:00:00"]
#         })
# db_asset.add_asset_scalar_publication(df)













