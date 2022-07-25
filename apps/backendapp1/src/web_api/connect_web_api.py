# Native Python libs:
import logging
import sys, io
from http import HTTPStatus
from dataclasses import dataclass
from pathlib import Path

# 3rd party Python libs:
import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict


# own modules and libs:
from utils.init_params import opts


logger = logging.getLogger(f"__main__.web_api.{__name__}")




@dataclass
class WebAPI():

    def _load_file_content(self, filename):
        file_path = Path.cwd() / "web_api" /  "data_samples" / filename
        with open(file_path, newline=None, mode='rb', encoding=None,) as f:
            return f.read()
    
    def _get_fake_return(self, filename, url):
        resp = requests.Response()
        resp.__setstate__({
            'status_code':200,
            'headers':CaseInsensitiveDict({'Content-Type':"application/json; charset=utf-8"}),
            'url':url,
            'encoding':"utf-8",
            'reason':HTTPStatus.OK
        })
        filename = filename if filename else "web_api_portfolio_return.csv"
        resp._content = self._load_file_content(filename)
        
        return resp
    
    def get_scontent_from_url(self, url: str, filename: str = None):
        """GET responce from given url

        Example:
        from connect_web_api import get_scontent_from_url
        text = get_scontent_from_url("www.some.url/pointer")
        """
        try:
            logger.debug(f'Going to GET data from {url}')
            with requests.Session() as s:
                # s.auth = (opts.api1_user_name, opts.api1_user_pass)
                s.headers.update({'accept': 'text/csv'})
                if opts.dryrun:
                    logger.debug(f'start in DRYRUN mode. Reading data from {filename}')
                    resp = self._get_fake_return(filename, url)
                else:    
                    resp =  s.get(url, verify=False)
                    resp.raise_for_status()
                logger.debug("GET request is completed.")

                return resp

        except requests.exceptions.HTTPError as err:
            logger.error(err)
            sys.exit(1)


    def get_df_from_api(self, url, file_with_test_data):
        resp = requests.Response()
        try:
            logger.debug(f'Reading data from {url}')
            resp = self.get_scontent_from_url(url, file_with_test_data)
            df = pd.read_csv(io.StringIO(resp.content.decode('utf-8')), sep=' ', on_bad_lines='skip') 
            logger.debug(df.head(5))
            return df
        
        except pd.errors.ParserError as e:
            data = resp.content.decode('utf-8'), 10
            data = data if len(data)<300 else data[:300]+" ..."
            logger.error(f"Respond from url: {url} :\n\n {data}")
            logger.error(f"Pandas: this is not CSV file: {e}")
        except Exception as e:
            logger.error(f"Pandas: wrong data set received from web API. Details: {e}")
            sys.exit(1)


    def get_portfolio_return_from_api(self, url=opts.api1_base_url):
        file_with_test_data = "web_api_portfolio_return.csv"
        url = url
        return self.get_df_from_api(url, file_with_test_data)

        
    def get_asset_publications_from_api(self, url=opts.api1_base_url):
        file_with_test_data = "web_api_asset_publication.csv"
        url = url
        return self.get_df_from_api(url, file_with_test_data)

