from expects import equal, expect
from aws_lambda_stream.events.kinesis import from_kinesis, to_kinesis_records


def test_from_kinesis_sorts_by_timestamp():
    records = to_kinesis_records([
        {
            'type': 'thing-created',
            'timestamp': 2,
            'partition_key': 'thing-2',
        },
        {
            'type': 'thing-created',
            'timestamp': 1,
            'partition_key': 'thing-1',
        },
    ])

    uows = from_kinesis(records)

    expect([uow['event']['partition_key'] for uow in uows]).to(equal([
        'thing-1',
        'thing-2',
    ]))
    expect(uows[0]['event']['id']).to(equal('shardId-000000000000:1'))
    expect(uows[1]['event']['id']).to(equal('shardId-000000000000:0'))
