# Native Python libs:
from pathlib import Path as path
import logging
# import sys, io

# 3rd party Python libs:
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import pendulum

# own modules and libs:
from utils.init_params import config

# Get root (__main__) logger:
logger = logging.getLogger(f"__main__.report.{__name__}")


dir_templates_for_email = path(config['DIR_TEMPLATES']) / 'email'
env = env = Environment(loader=FileSystemLoader(dir_templates_for_email))
template = env.get_template("email_body.html")





def daily_update_1(df_portfolio=None):
    pd.options.display.float_format = '{:.2f}'.format
    template_vars = {
        "report_title" : "Portfolio return",
        "report_date": pendulum.today().format('YYYY-MM-DD'),
        "portfolio_return": df_portfolio.to_html(index=False)
        }
    return template.render(template_vars)