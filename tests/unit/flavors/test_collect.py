from expects import equal, expect, have_key
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.events.kinesis import to_kinesis_records, from_kinesis
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from aws_lambda_stream.flavors.collect import collect
from aws_lambda_stream.connectors.dynamodb import Connector


RULES = [
    {
        'id': 'p1',
        'flavor': collect,
        'event_type': [
            'thing-created',
        ],
    }
]

def test_collect(monkeypatch):
    print("-----")

    events = to_kinesis_records([
        {
            'id': 'e1',
            'type': 'thing-created',
            'timestamp': 1548967023,
            'partition_key': '0',
            'thing': {
                "pk": "1",
                "sk": "thing",
                "discriminator": "thing",
                "timestamp": 1548967022000,
                "id": "1",
                "name": "Thing 1",
                "latched": False
            }
        }
    ])

    collected = []
    _errors = []

    monkeypatch.setattr(Connector, "put", lambda *_: {'result': 'OK'})

    def _on_next(_, ouw):
        collected.append(ouw)

    def _on_error(_, error):
        _errors.append(error)

    StreamPipeline(
        initialize_from(RULES),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_kinesis(events),
        on_next = _on_next,
        on_error = _on_error
    )

    expect(len(collected)).to(equal(1))
    expect(collected[0]).to(have_key('put_request'))
    expect(collected[0]['put_request']).to(equal({
        'Item': {
            'pk': 'e1',
            'sk': 'EVENT',
            'discriminator': 'EVENT',
            'timestamp': 1548967023,
            'awsregion': None,
            'sequence_number': '0',
            'ttl': 4400167,
            'expire': None,
            'data': '0',
            'event': {
                'id': 'e1',
                'type': 'thing-created',
                'timestamp': 1548967023,
                'partition_key': '0',
                'thing': {
                    'pk': '1',
                    'sk': 'thing',
                    'discriminator': 'thing',
                    'timestamp': 1548967022000,
                    'id': '1',
                    'name': 'Thing 1',
                    'latched': False
                }
            }
        }
    }))
