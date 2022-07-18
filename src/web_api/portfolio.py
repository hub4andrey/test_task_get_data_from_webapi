# Native Python libs:
import logging
import io

# 3rd party Python libs:
import pandas as pd

# own modules and libs:
from .connect_web_api import get_scontent_from_url
from utils.init_params import config
from utils.datetime import validate_date_from_date_to



logger = logging.getLogger(f"__main__.web_api.{__name__}")

def get_portfolio_return_from_api():
    """GET portfolio return from WEB API
    **Sidenote**: If web API is responding with 10 000x records or more, then below and other services
    have to be modified to process the data by chunks to prevent server RAM overflow.

    return pandas.DataFrame()

    Example:
    import web_api.asset as portfolio
    df = portfolio.get_portfolio_return_from_api()
    """
    if config['DRYRUN']:
        file_with_csv_test_content = config['DIR_DRYRYN'] / 'web_api_portfolio_return.csv' 
        logger.debug(f'start in DRYRUN mode. Reading data from {file_with_csv_test_content}')
        df = pd.read_csv(file_with_csv_test_content, sep=' ')
        logger.debug(df.head(5))

        return df

    else:
        url = config['WAPI_URL_BASE']
        logger.debug(f'Reading data from {url}')
        df = pd.read_csv(io.StringIO(get_scontent_from_url(url)))
        logger.debug(df.head(5))

        return df