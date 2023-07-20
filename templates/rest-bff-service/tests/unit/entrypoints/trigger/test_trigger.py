from expects import equal, expect
from aws_lambda_stream.events.dynamodb import from_dynamodb, to_dynamodb_records
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from src.entrypoints.trigger.rules import rules


def test_cdc():
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

    collected = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from(rules),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_dynamodb(events),
        on_next = _on_next
    )

    expect(len(collected)).to(equal(1))
    expect(collected[0]['pipeline']).to(equal('thing-cdc'))
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
