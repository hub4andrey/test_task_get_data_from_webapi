# Native Python libs:
import unittest

# 3rd party Python libs:
import pandas as pd
import pendulum

# own modules and libs:
from .connect_web_api import *


class WebAPITest(unittest.TestCase):
    """Write `unit tests` to ensure your code produces the expected results. 
    The `tests should cover` all parts of the code:
    - data acquisition
    """
    
    df_standard_portfolio = pd.DataFrame({
        "portfolio_return": [-0.0365, 0.001256, 0.00024],
        "customer_id":[1245,5458,1245],
        "date":["2022-01-10","2022-01-10","2022-01-11"]
        })
    
    df_standard_asset = pd.DataFrame({
        "price": [1.2, 5.4, 5.3],
        "asset_id":[19846,76156,19846],
        "datetime":["2022-02-23T12:45:15","2022-02-23T12:45:15","2022-02-23T12:45:30"]
        })

    def setUp(self):
        self.req = WebAPI()
    
    def test_reposnd_status_code_200(self):    
        self.assertEqual(200, self.req.get_scontent_from_url(opts.api1_base_url).status_code)

    def test_respond_header_content_type_is_json(self):
        self.assertEqual("application/json; charset=utf-8", self.req.get_scontent_from_url(opts.api1_base_url).headers['Content-Type'])


    def test_df_portfolio_return_has_correct_column_names(self):
        self.assertEqual(
            sorted(self.req.get_portfolio_return_from_api().columns.to_list()) , 
            sorted(self.df_standard_portfolio.columns.to_list()))

    def test_df_asset_price_has_correct_column_names(self):
        self.assertEqual(
            sorted(self.req.get_portfolio_return_from_api().columns.to_list()) , 
            sorted(self.df_standard_portfolio.columns.to_list()))

    def test_respond_df_portfolio_return_data_types_are_correct(self):
        left = self.req.get_portfolio_return_from_api()
        right = self.df_standard_portfolio
        columns = sorted(left.columns.to_list())
        self.assertEqual(
            str(left[columns].dtypes),
            str(right[columns].dtypes)
        )

    def tearDown(self) -> None:
        self.req = None
        # return super().tearDown()    
