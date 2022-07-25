# Native Python libs:
from pathlib import Path as path
import logging

# 3rd party Python libs:
import pandas as pd
import pendulum

# own modules and libs:
from utils.init_logger import set_logger

import db.portfolio as db_portfolio
import db.asset as db_asset
from db.connect_pg import test_connection_to_db

# import web_api.portfolio as web_portfolio
# import web_api.asset as web_asset
from web_api.connect_web_api import WebAPI

from report.portfolio import EmailBodyWithOneTable
from report.postoffice import PostOffice

# =============================
# Logger. Configuration
# =============================
from utils.init_params import ALL_TASKS, opts

logger = logging.getLogger(__name__)
# set default level:
logger = set_logger(logger, console_level = logging.INFO, file_level = logging.INFO, dir_log = opts.dir_log)


def get_portfolio_return_update():
    """
    Call web API to get CSV convent .
    Convert output to Pandas DataFrame.
    Write output inoto DataBase.
    """
    logger.debug("Start get_portfolio_return_update")
    req = WebAPI()
    df = req.get_portfolio_return_from_api()
    db_portfolio.add_portfolio_return(df)


def get_asset_plublication_update():
    """
    Call web API to get CSV convent .
    Convert output to Pandas DataFrame.
    Write output inoto DataBase.
    """
    logger.debug("Start get_asset_plublication_update")
    req = WebAPI()
    df = req.get_asset_publications_from_api()
    print(df)
    db_asset.add_asset_scalar_publication(df)


def send_email_report_portfolio_monthly_return_update():
    """Send report by email

    The output from monthly portfolio return calculation is sent via email. It should have the following structure:

    month customer_id monthly_return
    2022-01 1245 0.04236
    2022-01 5458 0.05647
    2022-02 1245 0.00024
    ...
    """

    email_1_table = EmailBodyWithOneTable()
    body_html=email_1_table.get_body_html(
            df=db_portfolio.get_portfolio_return_monthly(customer_id=1), 
            h1_text="Portfolio return")

    office = PostOffice(
        email_to="4_andreych@mail.ru", 
        email_subject="Portfolio. Daily update", 
        body_html=body_html)
    office.send_email()




if __name__ == '__main__':

    
    logger.info(f'START')

    # Test connection to DB. On error, terminate the service:
    test_connection_to_db()


    for task in opts.tasks_list:
        if not task in ALL_TASKS.keys():
            print(f"Skip unknown task {task}")
        else:
            eval(f"{ALL_TASKS[task]}()")
    
    # get_portfolio_return_update()
    # get_asset_plublication_update()
    # send_email_report_portfolio_monthly_return_update()

    logger.info(f'COMPLETE')


