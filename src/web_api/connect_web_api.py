# Native Python libs:
import logging
import sys

# 3rd party Python libs:
import requests

# own modules and libs:
from utils.init_params import opts


logger = logging.getLogger(f"__main__.web_api.{__name__}")

def get_scontent_from_url(url):
    """GET responce from given url

    Example:
    from connect_web_api import get_scontent_from_url
    text = get_scontent_from_url("www.some.url/pointer")
    """
    try:
        logger.debug(f'Going to GET data from {url}')
        with requests.Session() as s:
            s.auth = (opts.api1_user_name, opts.api1_user_pass)
            s.headers.update({'accept': 'text/csv'})

            resp =  s.get(url, verify=False)
            resp.raise_for_status()
            logger.debug("GET request is completed.")
            return resp.content.decode('utf-8')

    except requests.exceptions.HTTPError as err:
        logger.error(err)
        sys.exit(1)

