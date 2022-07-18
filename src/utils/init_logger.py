import logging
from logging.handlers import RotatingFileHandler

def set_logger(logger, console_level = logging.DEBUG, file_level = logging.INFO, dir_log = None):

    # set permitted lowest level:
    logger.setLevel(logging.DEBUG)

    # add the handler to the root logger
    # logger = logging.getLogger('')

    # create formatter for all handlers:
    formatter_stdout = logging.Formatter('%(name)-12s: %(levelname)-8s: %(message)s')
    formatter_file = logging.Formatter('%(asctime)s %(name)s: %(levelname)s: %(message)s')

    # console handler. Set level to DEBUG:
    handler_stdout = logging.StreamHandler()
    handler_stdout.setLevel(console_level)
    handler_stdout.setFormatter(formatter_stdout)
    logger.addHandler(handler_stdout)

    # file handler. Set level to INFO:
    file_with_log_msg = dir_log / 'messages_inyouva.log'
    handler_rotating_file = RotatingFileHandler(file_with_log_msg, maxBytes=1000000, backupCount=5, delay=True)
    handler_rotating_file.setLevel(file_level)
    handler_rotating_file.setFormatter(formatter_file)
    logger.addHandler(handler_rotating_file)

    return logger

