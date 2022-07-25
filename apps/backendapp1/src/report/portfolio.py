# Native Python libs:
from pathlib import Path as path
import logging

# 3rd party Python libs:
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import pendulum

# own modules and libs:
from utils.init_params import opts


# Get root (__main__) logger:
logger = logging.getLogger(f"__main__.report.{__name__}")


class EmailBody():
    def __init__(self) -> None:
        """Build generic email body"""
        dir_templates_for_email = path(opts.dir_templates) / 'email'
        self._jinja_env = Environment(loader=FileSystemLoader(dir_templates_for_email))
        # default_template:
        self._jinja_template = self._jinja_env.get_template("email_body_with_div_as_input_1.html")
        self._jinja_template_vars = {}
        self.body_html = None

    def get_body_html(self):
        self.body_html = self._jinja_template.render(self._jinja_template_vars)
        
        return self.body_html


class EmailBodyWithOneTable(EmailBody):
    def __init__(self):
        super(EmailBodyWithOneTable, self).__init__()
        self._jinja_template = self._jinja_env.get_template("email_body_with_div_as_input_1.html")
    
    def get_body_html(self, df: pd.DataFrame = pd.DataFrame(), h1_text:str ="Portfolio return", h2_text:str ="Daily update"):
        pd.options.display.float_format = '{:.2f}'.format  
        self._jinja_template_vars = {
        "h1_text" : h1_text,
        "h2_text" : h2_text,
        "report_date": pendulum.today().format('YYYY-MM-DD'),
        "div_content": df.to_html(index=False)
        }

        return super().get_body_html()

