# Native Python libs:
import unittest

# 3rd party Python libs:
import pandas as pd
import pendulum

# own modules and libs:
from .portfolio import *


class PortfolioReturnTest(unittest.TestCase):
    """Write `unit tests` to ensure your code produces the expected results. 
    The `tests should cover` all parts of the code:
    - calculations in pandas
    """    

    def test_get_portfolio_return_monthly(self):

        # Get Fixture: calculate monthly portfolio return for customer_id = 1  :
        df2 = get_portfolio_return(
            customer_id=1, 
            date_from=pendulum.datetime(2022, 1, 1, 0, 0, 0),
            date_to=pendulum.datetime(2022, 3, 31, 23, 59 ,59))
        df2['return_plus_1'] = 1 + df2['portfolio_return']
        df2['month'] = df2['date'].astype(str).str[:7]
        
        df3 = df2[['month','return_plus_1']].groupby(pd.Grouper(key='month')).prod().round(6).reset_index() # .return_plus_1.prod() .round(6)
        df3['monthly_return'] = df3['return_plus_1'] - 1
        fixture = df3[["month","monthly_return"]].sort_values(['month'],ascending=[False], ignore_index=True) #.sort_values(['month'], ascending=[False], inplace=True, ignore_index=True)   
        
        # Get Fixture: output from report get_portfolio_return_monthly:
        result = get_portfolio_return_monthly(
            customer_id=1, 
            date_from=pendulum.datetime(2022, 1, 1, 0, 0, 0),
            date_to=pendulum.datetime(2022, 3, 31, 23, 59 ,59))
        result = result[["month","monthly_return"]].sort_values(['month'], ascending=[False], ignore_index=True)        

        # Make assertion:
        pd.testing.assert_frame_equal(result, fixture)
