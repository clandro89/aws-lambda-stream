import json
from pydash import get
from aws_lambda_stream.utils.uow import trim_and_redact


def print_start_pipeline(rule):
    return print_start(rule['logger'])


def print_end_pipeline(rule):
    return print_end(rule['logger'])


def print_start(logger):
    def wrapper(uow):
        logger.debug(
            'start type: %s, eid: %s',
            get(uow, 'event.type'),
            get(uow, 'event.id'),
        )
    return wrapper


def print_end(logger):
    def wrapper(uow):
        logger.debug(
            'end type: %s, eid: %s, uow: %s',
            get(uow, 'event.type'),
            get(uow, 'event.id'),
            json.dumps(trim_and_redact(uow), default=str),
        )
    return wrapper
