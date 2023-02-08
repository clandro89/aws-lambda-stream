import json
from expects import expect, equal
from pydash import get
from aws_lambda_stream.events.kinesis import from_kinesis, to_kinesis_records
from aws_lambda_stream.flavors.s3 import s3
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from aws_lambda_stream.utils.time import ttl



DISCRIMINATOR = 'thing'

def to_put_request(uow):
    return {
        'Key': f"{get(uow,'split.id') or get(uow, 'event.thing.id')}/thing",
        'Body': json.dumps({
            **(get(uow,'split') or get(uow, 'event.thing')),
            'discriminator': 'thing',
            'ttl':  ttl(uow['event']['timestamp'], 1),
            'timestamp': uow['event']['timestamp'],
        })
    }


RULES = [
    {
        'id': 'mv1',
        'flavor': s3,
        'event_type': 'm1',
        'filters': [lambda *_: True],
        'to_s3': to_put_request
    },
    {
        'id': 'other1',
        'flavor': s3,
        'event_type': 'x9',
    },
    {
        'id': 'split',
        'flavor': s3,
        'event_type': 'split',
        'split_on': 'event.root.things',
        'to_s3': to_put_request
    },
    {
        'id': 'split-custom',
        'flavor': s3,
        'event_type': 'split',
        'split_on': lambda uow,_: list(map(
            lambda t: {
                **uow,
                'split': t
            },
            get(uow, 'event.root.things', [])
        )),
        'to_s3': to_put_request
    },
]


def test_materialize_s3():
    events = to_kinesis_records([
        {
            'type': 'm1',
            'timestamp': 1548967022000,
            'thing': {
                'id': '1',
                'name': 'Thing One',
                'description': 'This is thing one',
            },
        },
        {
            'type': 'split',
            'timestamp': 1548967022000,
            'root': {
                'things': [
                    {
                        'id': '2',
                        'name': 'Thing One',
                        'description': 'This is thing one',
                    },
                    {
                        'id': '3',
                        'name': 'Thing One',
                        'description': 'This is thing one',
                    }
                ],
            },
        }
    ])


    collected = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from(RULES),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_kinesis(events),
        on_next = _on_next,
    )
    print(collected)

    expect(len(collected)).to(equal(5))
    expect(collected[0]['pipeline']).to(equal('mv1'))
    expect(collected[0]['event']['type']).to(equal('m1'))
    expect(collected[0]['put_request']).to(equal({
        'Key': '1/thing',
        'Body': json.dumps({
            'id': '1',
            'name': 'Thing One',
            'description': 'This is thing one',
            'discriminator': 'thing',
            'ttl': 1549053422,
            'timestamp': 1548967022000,
        }),
    }))
    expect(collected[1]['put_request']['Key']).to(equal('2/thing'))
    expect(collected[2]['put_request']['Key']).to(equal('3/thing'))
    expect(collected[3]['put_request']['Key']).to(equal('2/thing'))
    expect(collected[4]['put_request']['Key']).to(equal('3/thing'))
