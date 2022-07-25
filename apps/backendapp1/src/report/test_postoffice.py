# Native Python libs:
import unittest
from unittest.mock import patch

# 3rd party Python libs:
import pandas as pd

# own modules and libs:
from utils.init_params import opts
from report.postoffice import PostOffice
from report.portfolio import EmailBodyWithOneTable


class ModuleTest(unittest.TestCase):
    # https://jingwen-z.github.io/how-to-send-emails-with-python/
    # https://www.anycodings.com/1questions/360707/patch-smtp-client-in-python-with-unittestmock

    instance = None
    msg = None
    SMTP_USER_NAME = "4_andreych@mail.ru"
    df_standard_portfolio = pd.DataFrame({
        "portfolio_return": [-0.0365, 0.001256, 0.00024],
        "customer_id":[1245,5458,1245],
        "date":["2022-01-10","2022-01-10","2022-01-11"]
        })

    def setUp(self):

        with patch("smtplib.SMTP_SSL") as mock_smtp_ssl:

            email_1_table = EmailBodyWithOneTable()
            body_html = email_1_table.get_body_html(
                    df=self.df_standard_portfolio, 
                    h1_text="Portfolio return")
            
            office = PostOffice(
                email_to=self.SMTP_USER_NAME, 
                email_subject="Portfolio. Daily update", 
                body_html=body_html
                )
            office._build_email()
            self.msg = office.msg
            office.send_email()

            # Get instance of mocked SMTP object
            self.instance = mock_smtp_ssl.return_value
    
    def test_check_address_from(self):
        # Check built e-mail elements
        self.assertEqual(self.msg['From'], self.SMTP_USER_NAME)
        
    def test_check_address_to(self):
        self.assertEqual(self.msg['To'], self.SMTP_USER_NAME)

    def test_calls_to_smtp(self):
        self.assertGreaterEqual(len(self.instance.mock_calls), 1)

    def test_msg_get_data_from_df(self):
        for customer_id in self.df_standard_portfolio['customer_id'].values:
            self.assertIn(str(customer_id), self.msg.as_string())


if __name__ == "__main__":
    unittest.main(verbosity=2)