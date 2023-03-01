from expects import equal, expect
from pydash import get
from reactivex import Observable
from aws_lambda_stream.events.kinesis import to_kinesis_records, from_kinesis
from aws_lambda_stream.flavors.task import task
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.operators import rx_map
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS


def _execute_task(uow,rule):
    uow['event']['thing'] = {
        'id': rule['id'],
        **uow['event']['thing']
    }
    return uow


def _execute_ops(_):
    def wrapper(source: Observable):
        return source.pipe(
            rx_map(lambda uow: {
                **uow,
                'ex_ops_result': 'OK'
            })
        )
    return wrapper

RULES = [
    {
        'id': 'task1',
        'flavor': task,
        'event_type': 'task1',
        'execute': _execute_task,
        'emit': lambda uow,_,template: {
            **template,
            'type': 'task1-completed',
            'thing': uow['event']['thing']
        }
    },
    {
        'id': 'task2',
        'flavor': task,
        'event_type': 'task2',
        'execute': _execute_task,
        'emit': lambda uow,_,template: [
            {
                **template,
                'type': 'task2-completed-1',
                'thing': uow['event']['thing'],
                'context': uow['event']['context']
            },
            {
                **template,
                'type': 'task2-completed-2',
                'thing': uow['event']['thing'],
                'context': uow['event']['context']
            },
        ]
    },
    {
        'id': 'task3',
        'flavor': task,
        'event_type': 'task1',
        'execute_operators': _execute_ops,
        'emit': lambda uow,_,template: {
            **template,
            'type': 'task1-completed',
            'thing': uow['event']['thing']
        }
    },
]

def test_execute_task():
    events = to_kinesis_records([
        {
            "type": "task1",
            "timestamp": 1548967022000,
            "partition_key": "task1",
            "thing": {
                "name": "Thing One",
                "description": "This is thing one"
            }
        },
        {
            "type": "task2",
            "timestamp": 1548967022000,
            "partition_key": "task2",
            "thing": {
                "name": "Thing Two",
                "description": "This is thing two"
            },
            "context": {
                "data1": "value",
                "data2": {
                    "key1": "value1"
                }
            }
        },
    ])

    collected  = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from(RULES),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_kinesis(events),
        on_next = _on_next
    )

    expect(len(collected)).to(equal(4))
    expect(get(collected, '[0].event.thing.id')).to(equal('task1'))
    expect(get(collected, '[0].emit.type')).to(equal('task1-completed'))
    expect(get(collected, '[1].event.thing.id')).to(equal('task2'))
    expect(get(collected, '[1].emit.type')).to(equal('task2-completed-1'))
    expect(get(collected, '[1].emit.context')).to(equal({
        "data1": "value",
        "data2": {
            "key1": "value1"
        }
    }))
    expect(get(collected, '[2].emit.type')).to(equal('task2-completed-2'))
    expect(get(collected, '[3].ex_ops_result')).to(equal('OK'))
