# Native Python libs:
import unittest

# 3rd party Python libs:
import pandas as pd
import pendulum

# own modules and libs:
from .asset import *


class Asset(unittest.TestCase):
    """Write `unit tests` to ensure your code produces the expected results. 
    The `tests should cover` all parts of the code:
    - calculations in pandas
    """

    def test_get_asset_daily_values_by_id(self):

        # Get Fixture: calculate hourly averaged values for asset id = 1 ('equity_msft') and attribute = 'price;day;close' :
        df1 = get_asset_daily_values_by_id(
            asset_id=1, 
            date_from=pendulum.datetime(2022, 2, 1, 0, 0, 0),
            date_to=pendulum.datetime(2022, 2, 1, 23, 59 ,59))

        fixture = df1.groupby(df1['datetime'].dt.floor('h')).price.mean().to_frame('value').reset_index()
        fixture.sort_values(['datetime'], ascending=[False], inplace=True, ignore_index=True)
        fixture.rename(columns={'datetime':"published_at_cet"}, inplace=True)

        # Get Result: output from get_asset_hourly_avg_values_by_name
        result = get_asset_hourly_avg_values_by_name(
            attribute_name='price;day;close',
            asset_name='equity_msft',
            date_from=pendulum.datetime(2022, 2, 1, 0, 0, 0),
            date_to=pendulum.datetime(2022, 2, 1, 23, 59 ,59))[['published_at_cet','value']]
        
        # Make assertion:
        pd.testing.assert_frame_equal(fixture, result)