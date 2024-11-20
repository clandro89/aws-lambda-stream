import time
import random
from expects import equal, expect
from reactivex import Observable
from aws_lambda_stream.pipelines import initialize_from, StreamPipeline
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.operators import rx_map, rx_filter



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
