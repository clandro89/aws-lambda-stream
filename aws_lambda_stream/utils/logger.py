from aws_lambda_powertools import Logger


def print_start_pipeline(uow):
    print_start(uow['debug'])(uow)


def print_end_pipeline(uow):
    print_end(uow['debug'])(uow)


def print_start(logger: Logger):
    def wrapper(uow):
        logger.info("Start type {}, eid: {}".format(
            uow['event']['type'],
            uow['event']['id']
        ), extra={'uow': uow})
    return wrapper

def print_end(logger: Logger):
    def wrapper(uow):
        logger.info("End type {}, eid: {}".format(
            uow['event']['type'],
            uow['event']['id']
        ), extra={'uow': uow})
    return wrapper
