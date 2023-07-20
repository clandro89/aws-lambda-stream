from expects import expect, equal
from aws_lambda_stream.events.kinesis import from_kinesis, to_kinesis_records
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from src.entrypoints.listener.rules import rules


def test_materialize():
    events = to_kinesis_records([
        {
            'type': 'thing-created',
            'timestamp': 1548967022000,
            'thing': {
                'id': '1',
                'name': 'Thing One',
                'description': 'This is thing one',
            },
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
        from_kinesis(events),
        on_next = _on_next,
    )
    print(collected)
    expect(len(collected)).to(equal(1))
    expect(collected[0]['pipeline']).to(equal('thing-materialize'))
    expect(collected[0]['event']['type']).to(equal('thing-created'))
    expect(collected[0]['update_request']).to(equal(
    {
        'Key': {
            'pk': '1',
            'sk': 'thing'
        },
        'ExpressionAttributeNames': {
            '#id': 'id',
            '#name': 'name',
            '#description': 'description',
            '#discriminator': 'discriminator',
            '#last_modified_by': 'last_modified_by',
            '#timestamp': 'timestamp',
            '#deleted': 'deleted',
            '#latched': 'latched',
            '#ttl': 'ttl'
        },
        'ExpressionAttributeValues': {
            ':id': '1',
            ':name': 'Thing One',
            ':description': 'This is thing one',
            ':discriminator': 'thing',
            ':last_modified_by': 'system',
            ':timestamp': 1548967022000,
            ':latched': True,
            ':ttl': 1551818222
        },
        # pylint: disable=C0301
        'UpdateExpression': 'SET #id = :id, #name = :name, #description = :description, #discriminator = :discriminator, #last_modified_by = :last_modified_by, #timestamp = :timestamp, #latched = :latched, #ttl = :ttl REMOVE #deleted',
        'ReturnValues': 'ALL_NEW',
        'ConditionExpression': 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp'
    }
    ))
