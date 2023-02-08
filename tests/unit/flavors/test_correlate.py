import os
from pydash import get
from expects import equal, expect
from aws_lambda_stream.flavors.correlate import correlate
from aws_lambda_stream.events.dynamodb import to_dynamodb_records
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from aws_lambda_stream.events.dynamodb import from_dynamodb
from aws_lambda_stream.connectors.dynamodb import Connector


RULES = [
    {
        'id': 'crl1',
        'flavor': correlate,
        'event_type': 'c1',
        'correlation_key': 'thing.id',
    },
    {
        'id': 'crl2',
        'flavor': correlate,
        'event_type': 'c1',
        'correlation_key': 'thing.id',
        'correlation_key_suffix': 'x',
        'ttl': 11,
    },
    {
        'id': 'crl3',
        'flavor': correlate,
        'event_type': 'c3',
        'correlation_key': lambda uow: '|'.join([
            get(uow, 'event.thing.group'),
            get(uow, 'event.thing.category')
        ]),
        'expire': True
    }
]

def test_correlate(monkeypatch):
    events = to_dynamodb_records([
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "1",
                "sk": "EVENT"
            },
            "newImage": {
                "pk": "1",
                "sk": "EVENT",
                "discriminator": "EVENT",
                "timestamp": 1548967022000,
                "sequence_number": "0",
                "ttl": 1551818222,
                "data": "1",
                "event": {
                    "id": "1",
                    "type": "c1",
                    "timestamp": 1548967022000,
                    "partition_key": "11",
                    "thing": {
                        "id": "11",
                        "name": "Thing One",
                        "description": "This is thing one"
                    }
                }
            }
        },
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "3",
                "sk": "EVENT"
            },
            "newImage": {
                "pk": "3",
                "sk": "EVENT",
                "discriminator": "EVENT",
                "timestamp": 1548967022000,
                "sequence_number": "0",
                "ttl": 1551818222,
                "data": "3",
                "event": {
                    "id": "3",
                    "type": "c3",
                    "timestamp": 1548967022000,
                    "partition_key": "33",
                    "thing": {
                        "id": "33",
                        "name": "Thing Three",
                        "description": "This is thing three",
                        "group": "A",
                        "category": "B"
                    }
                }
            }
        }
        ])

    monkeypatch.setattr(Connector, "put", lambda *_: {'result': 'OK'})

    collected  = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from(RULES),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_dynamodb(events),
        on_next = _on_next
    )

    expect(len(collected)).to(equal(3))
    expect(get(collected, '[0].pipeline')).to(equal('crl1'))
    expect(get(collected, '[0].event.type')).to(equal('c1'))
    expect(get(collected, '[0].put_request')).to(equal({
        "Item": {
            "pk": "11",
            "sk": "1",
            "discriminator": "CORREL",
            "ttl": 1551818222,
            "expire": None,
            "timestamp": 1548967022000,
            "sequence_number": "0",
            "suffix": None,
            "rule_id": "crl1",
            "awsregion": os.getenv('REGION'),
            "event": {
                "id": "1",
                "type": "c1",
                "timestamp": 1548967022000,
                "partition_key": "11",
                "thing": {
                    "id": "11",
                    "name": "Thing One",
                    "description": "This is thing one"
                }
            }
        }
    }))

    expect(get(collected, '[1].pipeline')).to(equal('crl2'))
    expect(get(collected, '[1].event.type')).to(equal('c1'))
    expect(get(collected, '[1].put_request.Item.pk')).to(equal('11.x'))
    expect(get(collected, '[1].put_request.Item.ttl')).to(equal(1549917422))

    expect(get(collected, '[2].pipeline')).to(equal('crl3'))
    expect(get(collected, '[2].event.type')).to(equal('c3'))
    expect(get(collected, '[2].put_request.Item.pk')).to(equal('A|B'))
    expect(get(collected, '[2].put_request.Item.expire')).to(equal(True))
