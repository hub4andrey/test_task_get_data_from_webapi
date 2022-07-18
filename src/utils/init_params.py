# Native Python libs:
from pathlib import Path as path
import argparse
import logging, os, sys

# 3rd party Python libs:
from dotenv import dotenv_values


# Prevalence:
# takes hardcoded (default) configuration values.
# than takes variables from OS ENV. Update (add new & modify existing) hardcoded values.
# than takes variables form .env files (.env.shared, .env.secret). This updates the values from all previuos steps
# than takes variables from arguments. This update the values from all previuos steps

# Get root (__main__) logger:
logger = logging.getLogger(f"__main__.utils.{__name__}")


# Set default hardcoded config values:
config = {
    'LOG_LEVEL':logging.INFO,
    'DRYRUN': True,
    # 'WAPI_URL_BASE': 'https://api.meine-bank.ch/v1/accounts',
    # 'WAPI_URL_BASE': 'https://www.theice.com/publicdocs/clear_europe/irmParameters/harmonized/ENERGY_MARGIN_DELIVERY_20220718.CSV',
    # 'WAPI_URL_BASE': 'https://api2.sgx.com/sites/default/files/reports/fxproducts/2022/07/wcm%40sgx_en%4015-Jul-2022%40fsvdate%40US.csv',
    'DIR_DRYRYN': path('.').parent / 'test_input',
    'DIR_LOG': path('.').parent / 'log',
    'DIR_TEMPLATES': path('.').parent / 'templates',  
}


logger.debug('Rading configuration from OS ENV and then from .env files')
config = {
    **config,
    **os.environ,  # override loaded values with environment variables
    **dotenv_values(".env.shared"),  # load shared development variables
    **dotenv_values(".env.secret"),  # load sensitive variables
}


def get_config_from_python_run_main_arguments():
    """Rading configuration as attributes from python -m main --attribute_key=value"""
    logger.debug('Rading configuration as attributes from python -m main --attribute_key=value')

    parser = argparse.ArgumentParser(description='Protfolio returns and asset prices web API reader.')
    parser.add_argument('--db_host', type=str, help='DataBase hostname')
    parser.add_argument('--db_port', type=int, help='DataBase port', default=5432)
    parser.add_argument('--db_user', type=str, help='DataBase user name', default=None)
    parser.add_argument('--db_pass', type=str, help='DataBase user password', default=None)
    parser.add_argument('--db_name', type=str, help='DataBase name', default=None)
    parser.add_argument('--log_level', type=str, help='Configure logging level none/info/debug', default='info')
    parser.add_argument('--dryrun', action='store_false', help='Read data from CSV instead of URL. Roll transactions back after making them (no SQL changes effectively)')
    parser.add_argument('--dump_to_file', action='store_false', help='Activate saving of queried data in data_folder')

    opts = parser.parse_args()
    for arg in opts:
        config[arg.upper()] = opts[arg]


    if config['LOG_LEVEL'] == 'none':
        logger.info('muting log messages')
        logging.basicConfig(level = logging.CRITICAL)
    elif config['LOG_LEVEL'] == 'debug':
        logger.info('setting loglevel to debug')
        logging.basicConfig(level = logging.DEBUG)
    else:
        logger.info('setting loglevel to info')
        logging.basicConfig(level = logging.INFO)

    if config.get('DB_HOST', None) is None:
        logging.error('DB host is not set')
        sys.exit(1)
    if config.get('DB_PORT', None) is None:
        logging.warn('DB port not set, using default 5432')
        pg_port = 5432
    if config.get('DB_NAME', None) is None:
        logging.error('DB name is not set')
        sys.exit(1)
    if config.get('DB_USER', None) is None:
        logging.error('DB user is not set')
        sys.exit(1)
    if config.get('DB_PASS', None) is None:
        logging.error('DB password is not set')
        sys.exit(1)
    if config.get('DB_SCHEMA', None) is None:
        logging.error('DB schema missing, set DB_SCHEMA env variable')
        sys.exit(1)


# TO-DO:
# get_config_from_python_run_main_arguments()