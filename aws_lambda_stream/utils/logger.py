

import logging
import os


def configure_logger(logger):
    if not isinstance(logger, logging.Logger):
        return logger

    level_name = os.getenv('LOG_LEVEL', 'INFO').upper()
    logger.setLevel(getattr(logging, level_name, logging.INFO))

    if not logger.handlers and not isinstance(logger, logging.RootLogger):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s %(name)s %(message)s'
        ))
        logger.addHandler(handler)

    return logger


def print_start_pipeline(uow):
    print_start(uow['debug'])(uow)


def print_end_pipeline(uow):
    print_end(uow['debug'])(uow)


def print_start(logger):
    def wrapper(uow):
        logger.info("Start type {}, eid: {}".format(
            uow['event']['type'],
            uow['event']['id']
        ))
    return wrapper

def print_end(logger):
    def wrapper(uow):
        logger.info("End type {}, eid: {}".format(
            uow['event']['type'],
            uow['event']['id']
        ))
    return wrapper
