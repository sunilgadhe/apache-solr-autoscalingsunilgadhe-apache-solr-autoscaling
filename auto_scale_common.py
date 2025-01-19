import os
import time

import logging
from logging.handlers import RotatingFileHandler

try:
    from fluent import sender
    from fluent import event
except Exception as e:
    print('cant import fluent: {}'.format(str(e)))

from autoscale_conf import LOG_PATH
from autoscale_conf import ELK_TEST


def get_logger(logger_name, log_dir_path=''):
    """
    General and common function to create the logger object
    :param logger_name: str, name identifying the logger object, and name the log file
    :param log_dir_path: (optional) str, path of the dir where log file will be created

    :return: logger object.
    """
    log = logging.getLogger(logger_name)
    formatter = logging.Formatter(
        '%(asctime)s - %(process)d - %(filename)s:%(lineno)s - %(funcName)20s()- %(levelname)s - %(message)s')
    if not log_dir_path:
        date = time.strftime("%Y%m%d")
        hour = time.strftime("%H")
        log_dir_path = os.path.join(LOG_PATH, date, hour)
    os.makedirs(log_dir_path, exist_ok=True)
    base_log_filename = "{}".format(logger_name)
    handler = RotatingFileHandler(os.path.join(log_dir_path, base_log_filename), maxBytes=5 * 1024 * 1024,
                                  backupCount=10)
    handler.setFormatter(formatter)
    log.setLevel(logging.INFO)
    log.addHandler(handler)

    return log


def clog(data_dict):
    """
    Function for creating central logs
    :param data_dict: dict, the log dict
    :return: None
    """
    key = data_dict.get('event_name')
    if data_dict.get("start_time") and len(data_dict.get("start_time")) == 12:
        date = data_dict.get("start_time")[:8]
        hour = data_dict.get("start_time")[8:10]
        log_dir_path = os.path.join(LOG_PATH, date, hour)

        logger = get_logger('elk_logs', log_dir_path)
    else:
        logger = get_logger('elk_logs')

    logger.info("key: {}, msg: {}".format(key, data_dict))
    if ELK_TEST:
        pass
    else:
        try:
            sender.setup(key, host='localhost', port=24224)
            event.Event('status', data_dict)
            logger.info('logs sent!')
        except Exception as e:
            logger.exception("Error in sending data to fluent : {}".format(str(e)), exc_info=True)

    return
