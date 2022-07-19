# Native Python libs:
from email.policy import default
from pathlib import Path as path
import argparse
import logging, os, sys

# 3rd party Python libs:
from dotenv import dotenv_values

logger = logging.getLogger(f"__main__.utils.{__name__}")

VERSION="1.0"

# update it after main.py modification:
ALL_TASKS = {
    'get_portfolio_return': 'get_portfolio_return_update',
    'get_asset_plublication': 'get_asset_plublication_update',
    'email_portfolio_monthly_return': 'send_email_report_portfolio_monthly_return_update',
}

# Prevalence:
# takes hardcoded (default) configuration values from "config".
# than takes variables from OS ENV. Update (add new & modify existing) hardcoded values.
# than takes variables form .env files in order: .env.shared, .env.secret. This updates the values from all previuos steps
# than takes variables from CLI arguments. This update the values from all previuos steps


# Step 1. Set default hardcoded config values:
config = {
    'LOG_LEVEL':"info",
    'DRYRUN': False,
    'WAPI_URL_BASE': 'https://api.meine-bank.ch/v1/accounts',
    # 'WAPI_URL_BASE': 'https://www.theice.com/publicdocs/clear_europe/irmParameters/harmonized/ENERGY_MARGIN_DELIVERY_20220718.CSV',
    # 'WAPI_URL_BASE': 'https://api2.sgx.com/sites/default/files/reports/fxproducts/2022/07/wcm%40sgx_en%4015-Jul-2022%40fsvdate%40US.csv',
    # 'DIR_DRYRYN': path.cwd().parent.resolve() / 'test_input',
    # 'DIR_LOG': path.cwd().parent.resolve() / 'log',
    # 'DIR_TEMPLATES': path.cwd().parent.resolve() / 'templates',  
}


def prepare_parent_parser():
    """
    Instantiate ArgumentParser. Update args default values by "config" dict cotent.

    IMPORTANT
    Below you have to describe ALL configuration parameters used by this service.
    If you add any parameter into OS ENV or into ".env" files, but not describe below, these "unknown" parameters
    will be ignored.
    """

    # Instantiate ArgumentParser:
    parent_parser = argparse.ArgumentParser(add_help=False)

    tasks_list = " ".join([k for k in ALL_TASKS])
    parent_parser.add_argument("--run", metavar="task", 
        type=str, nargs="+", dest="tasks_list", 
        help=f"space separated list of tasks to be executed. Currently available tasks: {tasks_list}", 
        default="['get_portfolio_return', 'get_asset_plublication']")


    parent_parser.add_argument('--version', action='store_true', help='Print out service version. Do nothing')
    # CRITICAL=50;ERROR=40;WARNING=30;INFO=20;DEBUG=10;NOTSET=0
    parent_parser.add_argument("--log_level", type=str, help="Configure logging level none|info|debug. Default(info)", default="info")
    parent_parser.add_argument("--dryrun", action=argparse.BooleanOptionalAction, help="Read data from CSV instead of URL. Roll transactions back after making them (no SQL changes effectively)", default=False)
    # parent_parser.add_argument("--dump_to_file", action=argparse.BooleanOptionalAction, help="Activate saving of queried data in data_folder", default=False))
   
    parent_parser.add_argument("--db_host", type=str, help="DataBase hostname", default=None)
    parent_parser.add_argument("--db_port", type=int, help="DataBase port", default=5432)
    parent_parser.add_argument("--db_user", type=str, help="DataBase user name", default=None)
    parent_parser.add_argument("--db_pass", type=str, help="DataBase user password", default=None)
    parent_parser.add_argument("--db_name", type=str, help="DataBase name", default=None)

    parent_parser.add_argument("--smtp_host", type=str, help="SMTP server host name or IP address", default=None)
    parent_parser.add_argument("--smtp_user_name", type=str, help="SMTP server user name", default=None)
    parent_parser.add_argument("--smtp_user_pass", type=str, help="SMTP server user password", default=None)

    parent_parser.add_argument("--api1_base_url", type=str, help="Web API #1 base URL", default="https://api.meine-bank.ch")
    parent_parser.add_argument("--api1_user_name", type=str, help="Web API #1 user name", default=None)
    parent_parser.add_argument("--api1_user_pass", type=str, help="Web API #1 user password", default=None)
    parent_parser.add_argument("--api1_token", type=str, help="Web API #1 token", default=None)

    project_dir = path.cwd().parent.resolve()
    parent_parser.add_argument("--dir_log", type=str, help="Directory for log files", default=project_dir / 'log')
    parent_parser.add_argument("--dir_templates", type=str, help="Directory with report templates", default=project_dir / 'templates')
    parent_parser.add_argument("--dir_test_data", type=str, help="Directory with test data set", default=project_dir / 'test_input')

    # Update args default values by "config" dict cotent.
    parent_args, parent_sub_args = parent_parser.parse_known_args(['--help'])
    for arg in config.keys():
        # print(f"processing {arg}")
        if arg.lower() in parent_args:
            # print(f"found {arg} to be update: from {parent_parser.get_default(arg.lower())} to {config[arg]}")
            parent_parser.set_defaults(**{arg.lower():config[arg]})
    # parent_args, parent_sub_args = parent_parser.parse_known_args(['--help'])
    # print(vars(parent_args))
  
    return parent_parser


# Step 2 and 3. Create single dictionary from hardcoded config values, OS ENV values, values from .env files:
config = {
    **config,
    **{k:v for k,v in os.environ.items() if k in config},  # Takes ENV variables if name in hardcoded "config". Overwrite hardcoded value.
    **dotenv_values(path.cwd().parent.resolve() / ".env.shared"),  # load shared development variables
    **dotenv_values(path.cwd().parent.resolve() / ".env.secret"),  # load sensitive variables
}


def validate_critical_arguments(opts):

    if opts.version:
        print(f'version {VERSION}')
        sys.exit(0)
    
    log_levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'none': logging.CRITICAL,
    }
    logging.basicConfig(level = log_levels.get(opts.log_level, "info"))
    
    
    if opts.db_host is None:
        logging.error('DB host is not set')
        sys.exit(1)
    if opts.db_port is None:
        logging.warn('DB port not set, using default 5432')
        pg_port = 5432
    if opts.db_name is None:
        logging.error('DB name is not set')
        sys.exit(1)
    if opts.db_user is None:
        logging.error('DB user is not set')
        sys.exit(1)
    if opts.db_pass is None:
        logging.error('DB password is not set')
        sys.exit(1)


def set_configuration():

    # Only now instantiate ArgumentParser to be proposed for user:
    parser = argparse.ArgumentParser(
        parents=[prepare_parent_parser()],
        description="This service is collecting up-to-date market data from api.meine-bank.ch and store it in DataWarehouse")

    # Get user's input:
    opts = parser.parse_args()
    # print(opts.tasks_list)
    # print(vars(opts))

    validate_critical_arguments(opts)

    return opts

opts = set_configuration()
is_ture = [1, '1', 'True', 'true', 'TRUE', True]
opts.dryrun = True if opts.dryrun in is_ture else False
# opts.dump_to_file = True if opts.dump_to_file in is_ture else False
