import argparse
import logging, os

from dotenv import load_dotenv, dotenv_values
from sqlalchemy import create_engine

# Prevalence:
# takes hardcoded default values
# than takes variables from OS ENV. Overwrites hardcoded one.
# than takes variables form .env file. This overwrite values from OS ENV
# than takes variables from arguments. This overwrite values from .env

# Get root (__main__) logger:
logger = logging.getLogger()

config = {
    'PG_HOST':{'value':None, 'argument':{'name':'--db_host', 'type': str, 'help':'DataBase hostname', 'default':None}},
    'PG_PORT':{'value':None, 'argument':{'name':'--db_port', 'type': int, 'help':'DataBase port', 'default':5432}},
    'PG_USER':{'value':None, 'argument':{'name':'--db_user', 'type': str, 'help':'DataBase user name', 'default':None}},
    'PG_PASSWORD':{'value':None, 'argument':{'name':'--db_pass', 'type': str, 'help':'DataBase user password', 'default':None}},
    'PG_DB':{'value':None, 'argument':{'name':'--db_name', 'type': str, 'help':'DataBase name', 'default':None}},

    'DRYRUN':{'value':False, 'argument':{'name':'--dryrun', 'action': 'store_true', 'help':'Read data from CSV instead of URL. Roll transactions back after making them (no SQL changes effectively)', 'default':False}},
    'LOG_LEVEL':{'value':'info', 'argument':{'name':'--log_level', 'type': str, 'help':'Configure logging level none/info/debug', 'default':'info'}},
    'DUMP':{'value':False, 'argument':{'name':'--dump', 'action': 'store_true', 'help':'Activate saving of queried data in data_folder', 'default':False}},
}


def get_config_from_os_env_and_dotenv():
    """Rading configuration from OS ENV and .env"""
    logger.debug('Rading configuration from OS ENV and .env. START')
    
    load_dotenv() # take environment variables from .env.
    for arg in config:
        config[arg]['value'] = os.environ.get(arg, config[arg]['arguments'].get('default', None))


def get_config_from_python_run_main_arguments():
    """Rading configuration as attributes from python -m main --attribute_key=value"""
    logger.debug('Rading configuration as attributes from python -m main --attribute_key=value')

    parser = argparse.ArgumentParser(description='Protfolio returns and asset prices web API reader.')
    for arg in config:
        arg_config = config[arg].get('argument', {})
        try:
            parser.add_argument(arg_config.get('name', None), type=arg_config.get('type', str), action=arg_config.get('type', None), help=arg_config.get('help', None))
        except:
            pass

    opts = parser.parse_args()

    # parser.add_argument('--data_folder', type=str, help='Folder where ENTSOG files are located')
    # parser.add_argument('--config', type=str, help='Path to query TOML config file')
    # parser.add_argument('--date_from', type=str, help='Start date for query, format dd.mm.yyyy')
    # parser.add_argument('--date_to', type=str, help='End date for query, format dd.mm.yyyy')
    # parser.add_argument('--num_days', type=int, help='Number of days before date_to to query')
    # parser.add_argument('--level', type=str, default='info', help='Configure logging level none/info/debug')
    # parser.add_argument('--pg_connstr', type=str, help='Connection string in the form postgresql://user:pass@host:port/db')
    # parser.add_argument('--dryrun', action='store_true', help='Roll transactions back after making them (no SQL changes effectively)')
    # parser.add_argument('--apikey', type=str, help='ALSI/AGSI api key')
    # parser.add_argument('--dump', action='store_true', help='Activate saving of queried data in data_folder')
    # parser.add_argument('--pause', type=int, default=200, help='avg number of milliseconds to wait between calls to GIE API')
    # opts = parser.parse_args()

    if opts.level == 'none':
        print('muting log messages')
        logging.basicConfig(level = logging.CRITICAL)
    elif opts.level == 'debug':
        print('setting loglevel to debug')
        logging.basicConfig(level = logging.DEBUG)
    else:
        print('setting loglevel to info')
        logging.basicConfig(level = logging.INFO)

    load_dotenv()

    # set up query time range
    if opts.date_to is None:
        logging.warning('to date is not set, using today')
        opts.date_to = datetime.today().date()
    else:
        opts.date_to = datetime.strptime(opts.date_to, '%d.%m.%Y').date()
    if opts.date_from is None:
        if opts.num_days is not None:
            logging.info('will query last {opts.num_days} before date.to')
            opts.date_from = opts.date_to - timedelta(days=opts.num_days)
        else:
            logging.info('from date is not set, using from=to')
            opts.date_from = opts.date_to
    else:
        opts.date_from = datetime.strptime(opts.date_from, '%d.%m.%Y').date()

    # check queried time range is valid
    if opts.date_from > opts.date_to:
        logging.error(f'date from={opts.date_from} > to={opts.date.to}')
        sys.exit(1)
    logging.info(f'updating value in time range {opts.date_from} - {opts.date_to}')

    # select data folder
    if opts.data_folder is None:
        opts.data_folder = os.getcwd()
        logging.info(f'data folder is not set, using current directory {opts.data_folder}')

    # check that api key is set
    if opts.apikey is None:
        opts.apikey = os.environ.get('GIE_APIKEY', None)
        if opts.apikey is None:
            logging.error('GIE apikey is not set on command line or in .env file')
            sys.exit(1)

    # check that config file exists
    if opts.config is None or not os.path.exists(opts.config):
        logging.error(f'configuration file not set or not found')
        sys.exit(1)


def connect_engine_light():
    #load_dotenv()
    env = dotenv_values()
    print(env)
    #pg_connstr = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    pg_connstr = f'postgresql://{env["PG_USER"]}:{env["PG_PASSWORD"]}@{env["PG_HOST"]}:{env["PG_PORT"]}/{env["PG_DB"]}'
            
    return create_engine(pg_connstr, echo=False, connect_args={'options': f'-c search_path={env["PG_SCHEMA"]}'})    


    
def connect_engine():
    """ Constructs DB connection using settings from .env file """
#     pg_connstr = opts.pg_connstr
    pg_connstr = None

    load_dotenv()

    if pg_connstr is None:
        logging.info(f'connection string is not set. Using ENV/.env to construct it')
        pg_host = os.environ.get('PG_HOST', None)
        pg_port = os.environ.get('PG_PORT', None)
        pg_user = os.environ.get('PG_USER', None)
        pg_pass = os.environ.get('PG_PASSWORD', None)
        pg_db = os.environ.get('PG_DB', None)

        if pg_host is None:
            logging.error('PG host is not set')
            sys.exit(1)
        if pg_port is None:
            logging.warn('PG port not set, using default 5432')
            pg_port = 5432
        if pg_db is None:
            logging.error('PG DB is not set')
            sys.exit(1)
        if pg_user is None or pg_user == '':
            logging.error('PG user is not set')
            sys.exit(1)
        if pg_pass is None or pg_pass == '':
            logging.error('PG password is not set')
            sys.exit(1)
        pg_connstr = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'

    pg_schema = os.environ.get('PG_SCHEMA', None)
    if pg_schema is None:
        logging.error('Postgres schema missing, set PG_SCHEMA env variable')
        sys.exit(1)
    logging.info(f'will use database schema {pg_schema}')

    return create_engine(pg_connstr, echo=False, connect_args={'options': f'-c search_path={pg_schema}'})