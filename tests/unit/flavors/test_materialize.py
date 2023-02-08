from expects import expect, equal
from pydash import get
from aws_lambda_stream.events.kinesis import from_kinesis, to_kinesis_records
from aws_lambda_stream.flavors.materialize import materialize
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from aws_lambda_stream.utils.dynamodb import update_expression, timestamp_condition
from aws_lambda_stream.utils.time import ttl



DISCRIMINATOR = 'thing'

def to_update_request(uow):
    return {
        'Key': {
            'pk': get(uow,'split.id') or get(uow, 'event.thing.id'),
            'sk': DISCRIMINATOR
        },
        **update_expression({
            **{k:v for k,v in (get(uow,'split') or get(uow, 'event.thing')).items()
               if k not in ['pk','sk']
            } ,
            'discriminator': 'thing',
            'ttl':  ttl(uow['event']['timestamp'], 1),
            'timestamp': uow['event']['timestamp'],
        }),
        **timestamp_condition()
    }

RULES = [
    {
        'id': 'mv1',
        'flavor': materialize,
        'event_type': 'm1',
        'filters': [lambda *_: True],
        'to_update_request': to_update_request
    },
    {
        'id': 'other1',
        'flavor': materialize,
        'event_type': 'x9',
    },
    {
        'id': 'split',
        'flavor': materialize,
        'event_type': 'split',
        'split_on': 'event.root.things',
        'to_update_request': to_update_request
    },
    {
        'id': 'split-custom',
        'flavor': materialize,
        'event_type': 'split',
        'split_on': lambda uow,_: list(map(
            lambda t: {
                **uow,
                'split': t
            },
            get(uow, 'event.root.things', [])
        )),
        'to_update_request': to_update_request
    },
]


def test_materialize():
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
    expect(collected[0]['update_request']).to(equal({
        "Key": {
            "pk": "1",
            "sk": "thing"
        },
        "ExpressionAttributeNames": {
            "#id": "id",
            "#name": "name",
            "#description": "description",
            "#discriminator": "discriminator",
            "#ttl": "ttl",
            "#timestamp": "timestamp"
        },
        "ExpressionAttributeValues": {
            ":id": "1",
            ":name": "Thing One",
            ":description": "This is thing one",
            ":discriminator": "thing",
            ":ttl": 1549053422,
            ":timestamp": 1548967022000
        },
        "UpdateExpression": "SET #id = :id, #name = :name, #description = :description, #discriminator = :discriminator, #ttl = :ttl, #timestamp = :timestamp",#pylint: disable=C0301
        "ReturnValues": "ALL_NEW",
        "ConditionExpression": "attribute_not_exists(#timestamp) OR #timestamp < :timestamp"
    }))
    expect(collected[1]['update_request']['Key']['pk']).to(equal('2'))
    expect(collected[2]['update_request']['Key']['pk']).to(equal('3'))
    expect(collected[3]['update_request']['Key']['pk']).to(equal('2'))
    expect(collected[4]['update_request']['Key']['pk']).to(equal('3'))
