from expects import contain, equal, expect
from aws_lambda_stream.utils.print import (
    print_end,
    print_end_pipeline,
    print_start,
    print_start_pipeline,
)


class Logger:
    def __init__(self):
        self.messages = []

    def debug(self, message, *args):
        self.messages.append(message % args)


def test_print_start_with_debug_callable():
    logger = Logger()
    rule = {
        'logger': logger,
    }
    uow = {
        'event': {
            'type': 'thing-created',
            'id': 'evt-1',
        },
    }

    print_start_pipeline(rule)(uow)

    expect(logger.messages).to(equal(['start type: thing-created, eid: evt-1']))


def test_print_end_with_logger_and_redacted_uow():
    logger = Logger()
    rule = {
        'logger': logger,
    }
    uow = {
        'pipeline': 'test',
        'record': {
            'secret': 'do-not-redact-record',
        },
        'event': {
            'type': 'thing-created',
            'id': 'evt-1',
            'eem': {
                'fields': ['secret'],
            },
            'secret': 'hide-me',
        },
        'payload': b'abcd',
    }

    print_end_pipeline(rule)(uow)

    expect(len(logger.messages)).to(equal(1))
    expect(logger.messages[0]).to(contain('end type: thing-created, eid: evt-1'))
    expect(logger.messages[0]).to(contain('"secret": "[REDACTED]"'))
    expect(logger.messages[0]).to(contain('"payload": "[BUFFER: 4]"'))
    expect(logger.messages[0]).to(contain('"secret": "do-not-redact-record"'))


def test_print_helpers_can_be_used_directly():
    logger = Logger()
    uow = {
        'event': {
            'type': 'thing-created',
            'id': 'evt-1',
        },
    }

    print_start(logger)(uow)
    print_end(logger)(uow)

    expect(logger.messages[0]).to(equal('start type: thing-created, eid: evt-1'))
    expect(logger.messages[1]).to(contain('end type: thing-created, eid: evt-1'))
