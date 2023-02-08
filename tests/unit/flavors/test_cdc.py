from expects import equal, expect
from aws_lambda_stream.flavors.cdc import cdc
from aws_lambda_stream.events.dynamodb import from_dynamodb, to_dynamodb_records
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS


RULES = [
    {
        'id': 'l1',
        'flavor': cdc,
        'event_type': [
            'thing-created',
            'thing-updated',
            'thing-deleted'
        ],
        'to_event': lambda uow: {
            'thing': uow['event']['raw']['new'],
            'raw': None
        },
    }
]

def test_cdc():
    print("---test cdc--")

    events = to_dynamodb_records([
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "1",
                "sk": "thing"
            },
            "newImage": {
                "pk": "1",
                "sk": "thing",
                "discriminator": "thing",
                "timestamp": 1548967022000,
                "id": "1",
                "name": "Thing 1",
                "latched": False
            }
        },
    ])
    print(events)
    collected = []

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

    expect(len(collected)).to(equal(1))
    expect(collected[0]['pipeline']).to(equal('l1'))
    expect(collected[0]['event']['thing']).to(equal({
                "pk": "1",
                "sk": "thing",
                "discriminator": "thing",
                "timestamp": 1548967022000,
                "id": "1",
                "name": "Thing 1",
                "latched": False
            }))
    expect(collected[0]['event']['raw']).to(equal(None))
