import logging
from expects import equal, expect
from aws_lambda_stream.utils.logger import (
    configure_logger,
    print_end,
    print_end_pipeline,
    print_start,
    print_start_pipeline,
)


def test_configure_logger_ignores_non_standard_loggers():
    class Logger:
        pass

    logger = Logger()

    expect(configure_logger(logger)).to(equal(logger))


def test_configure_logger_sets_level_and_handler(monkeypatch):
    monkeypatch.setenv('LOG_LEVEL', 'DEBUG')
    logger = logging.getLogger('test-configure-logger')
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)

    result = configure_logger(logger)

    expect(result).to(equal(logger))
    expect(logger.getEffectiveLevel()).to(equal(logging.DEBUG))
    expect(len(logger.handlers)).to(equal(1))


def test_configure_logger_does_not_add_handler_to_root(monkeypatch):
    monkeypatch.setenv('LOG_LEVEL', 'INFO')
    logger = logging.getLogger()
    handlers = list(logger.handlers)

    result = configure_logger(logger)

    expect(result).to(equal(logger))
    expect(logger.handlers).to(equal(handlers))


class LegacyLogger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)


def test_legacy_print_helpers():
    logger = LegacyLogger()
    uow = {
        'event': {
            'type': 'thing-created',
            'id': 'evt-1',
        },
    }

    print_start(logger)(uow)
    print_end(logger)(uow)

    expect(logger.messages).to(equal([
        'Start type thing-created, eid: evt-1',
        'End type thing-created, eid: evt-1',
    ]))


def test_legacy_pipeline_print_helpers():
    logger = LegacyLogger()
    uow = {
        'debug': logger,
        'event': {
            'type': 'thing-created',
            'id': 'evt-1',
        },
    }

    print_start_pipeline(uow)
    print_end_pipeline(uow)

    expect(logger.messages).to(equal([
        'Start type thing-created, eid: evt-1',
        'End type thing-created, eid: evt-1',
    ]))
