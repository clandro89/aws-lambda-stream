import time
import random
import logging
from expects import contain, equal, expect
from reactivex import Observable, from_list
from reactivex import operators as ops
from aws_lambda_stream.pipelines import initialize_from, StreamPipeline
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.operators import rx_map, rx_filter
from aws_lambda_stream.utils.print import print_end_pipeline, print_start_pipeline



def adding_delay(uow):
    timeout = random.randint(5, 10) * 0.1
    time.sleep(timeout)
    uow['delay'] = f" delay: {timeout}"
    return uow


def test_stream_pipeline():
    _collected = []
    _errors = []

    def raise_exception(i):
        if i['event']['id'] == 3:
            raise Exception('My exception')
        return i

    def _flavor1(_):
        def wrapper(source: Observable):
            return source.pipe(
                rx_map(lambda uow: {**uow, 'map1': True}),
                rx_map(lambda uow: {**uow, 'map2': True}),
                rx_filter(lambda uow: uow['event']['id'] > 1)
            )
        return wrapper

    def _flavor2(_):
        def wrapper(source: Observable):
            return source.pipe(
                rx_map(faulty(raise_exception)),
                rx_map(lambda uow: {**uow, 'map1': True}),
                rx_map(lambda uow: {**uow, 'map2': True}),
            )
        return wrapper

    def _on_next(_, i):
        _collected.append(i)

    def _on_error(_, e):
        _errors.append(e)

    def _on_completed(pipeline):
        print(f"Pipeline {pipeline}  completed")

    streams = [
        {'event': {'id': 1, 'number': 1}},
        {'event': {'id': 2, 'number': 2}},
        {'event': {'id': 3, 'number': 3}}
    ]
    rules = [
        {
            'id': 'test1',
            'flavor': _flavor1,
        },
        {
            'id': 'test2',
            'flavor': _flavor2,
        },
    ]
    StreamPipeline(
        initialize_from(rules),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        streams,
        on_next=_on_next,
        on_error=_on_error,
        on_completed=_on_completed
    )

    expect(len(_collected)).to(equal(4))
    expect(_collected[0]['pipeline']).to(equal('test1'))
    expect(_collected[0]['event']['id']).to(equal(2))
    expect(_collected[0]['map1']).to(equal(True))
    expect(_collected[1]['event']['id']).to(equal(3))
    
    expect(_collected[2]['pipeline']).to(equal('test2'))
    expect(_collected[2]['event']['id']).to(equal(1))
    expect(_collected[2]['map1']).to(equal(True))
    expect(_collected[3]['event']['id']).to(equal(2))


def test_stream_pipeline_with_concurrency_and_flat_map():
    collected = []
    completed = []

    def _flavor(multiplier):
        def wrapper(source: Observable):
            return source.pipe(
                ops.flat_map(lambda uow: from_list([
                    {**uow, 'copy': multiplier},
                    {**uow, 'copy': multiplier * 2},
                ]))
            )
        return wrapper

    StreamPipeline(
        initialize_from([
            {
                'id': 'test1',
                'flavor': lambda _: _flavor(1),
            },
            {
                'id': 'test2',
                'flavor': lambda _: _flavor(10),
            },
        ]),
        DEFAULT_OPTIONS,
    ).assemble(
        [
            {'event': {'id': 1}},
            {'event': {'id': 2}},
        ],
        on_next=lambda _, uow: collected.append(uow),
        on_completed=lambda pipeline: completed.append(pipeline),
    )

    expect(len(collected)).to(equal(8))
    expect(sorted(completed)).to(equal(['test1', 'test2']))
    expect(sorted([uow['copy'] for uow in collected])).to(equal([
        1, 1, 2, 2, 10, 10, 20, 20
    ]))


def test_stream_pipeline_with_concurrency_flat_map_and_failed_item():
    collected = []
    completed = []
    errors = []

    def fail_second_event(uow):
        if uow['event']['id'] == 2:
            raise Exception('bad item')
        return uow

    def _flavor(multiplier):
        def wrapper(source: Observable):
            return source.pipe(
                ops.flat_map(lambda uow: from_list([
                    {**uow, 'copy': multiplier},
                    {**uow, 'copy': multiplier * 2},
                ])),
                rx_map(faulty(fail_second_event)),
            )
        return wrapper

    StreamPipeline(
        initialize_from([
            {
                'id': 'test1',
                'flavor': lambda _: _flavor(1),
            },
            {
                'id': 'test2',
                'flavor': lambda _: _flavor(10),
            },
        ]),
        DEFAULT_OPTIONS,
    ).assemble(
        [
            {'event': {'id': 1}},
            {'event': {'id': 2}},
            {'event': {'id': 3}},
        ],
        on_next=lambda _, uow: collected.append(uow),
        on_error=lambda _, error: errors.append(error),
        on_completed=lambda pipeline: completed.append(pipeline),
    )

    expect(len(errors)).to(equal(0))
    expect(len(collected)).to(equal(8))
    expect(sorted(completed)).to(equal(['test1', 'test2']))
    expect(sorted([uow['event']['id'] for uow in collected])).to(equal([
        1, 1, 1, 1, 3, 3, 3, 3
    ]))


def test_stream_pipeline_configures_standard_logger(monkeypatch):
    collected = []
    rule_loggers = []
    monkeypatch.setenv('LOG_LEVEL', 'DEBUG')
    logger = logging.getLogger('test-standard-logger')
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)
    logger.propagate = False

    def _flavor(rule):
        rule_loggers.append(rule['logger'])

        def wrapper(source: Observable):
            return source.pipe(
                rx_map(lambda uow: uow)
            )
        return wrapper

    StreamPipeline(
        initialize_from([
            {
                'id': 'test',
                'flavor': _flavor,
            },
        ]),
        {
            **DEFAULT_OPTIONS,
            'logger': logger,
        },
        False
    ).assemble(
        [
            {'event': {'id': 1}},
        ],
        on_next=lambda _, uow: collected.append(uow),
    )

    expect(len(collected)).to(equal(1))
    expect(rule_loggers[0]).to(equal(logger))
    expect(rule_loggers[0].getEffectiveLevel()).to(equal(logging.DEBUG))
    expect(len(rule_loggers[0].handlers)).to(equal(1))


def test_stream_pipeline_default_logger_prints(capsys, monkeypatch):
    monkeypatch.setenv('LOG_LEVEL', 'DEBUG')
    logger = logging.getLogger('test')
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)
    logger.propagate = False

    def _flavor(rule):
        def wrapper(source: Observable):
            return source.pipe(
                ops.do_action(print_start_pipeline(rule)),
                ops.do_action(print_end_pipeline(rule)),
            )
        return wrapper

    StreamPipeline(
        initialize_from([
            {
                'id': 'test',
                'flavor': _flavor,
            },
        ]),
        {
            'publish': DEFAULT_OPTIONS['publish'],
        },
        False
    ).assemble(
        [
            {
                'event': {
                    'id': 'evt-1',
                    'type': 'thing-created',
                },
            },
        ],
    )

    output = capsys.readouterr()

    expect(output.err).to(contain('start type: thing-created, eid: evt-1'))
    expect(output.err).to(contain('end type: thing-created, eid: evt-1'))
