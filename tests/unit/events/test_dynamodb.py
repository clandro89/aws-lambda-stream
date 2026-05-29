from expects import equal, expect
from aws_lambda_stream.events.dynamodb import from_dynamodb, to_dynamodb_records


def test_from_dynamodb_sorts_by_event_timestamp_and_calculates_types(monkeypatch):
    monkeypatch.setenv('REGION', 'us-west-2')
    records = to_dynamodb_records([
        {
            'timestamp': 2,
            'keys': {
                'pk': 'thing-2',
                'sk': 'THING',
            },
            'newImage': {
                'pk': 'thing-2',
                'sk': 'THING',
                'timestamp': 2000,
            },
        },
        {
            'timestamp': 1,
            'keys': {
                'pk': 'thing-1',
                'sk': 'THING',
            },
            'newImage': {
                'pk': 'thing-1',
                'sk': 'THING',
                'timestamp': 1000,
            },
        },
    ])

    uows = from_dynamodb(records)

    expect([uow['event']['partition_key'] for uow in uows]).to(equal([
        'thing-1',
        'thing-2',
    ]))
    expect(uows[0]['event']['type']).to(equal('THING-created'))
    expect(uows[1]['event']['type']).to(equal('THING-created'))


def test_from_dynamodb_filters_replicas(monkeypatch):
    monkeypatch.setenv('REGION', 'us-west-2')
    records = to_dynamodb_records([
        {
            'timestamp': 1,
            'keys': {
                'pk': 'replica',
                'sk': 'THING',
            },
            'newImage': {
                'pk': 'replica',
                'sk': 'THING',
                'awsregion': 'us-east-1',
            },
        },
        {
            'timestamp': 2,
            'keys': {
                'pk': 'local',
                'sk': 'THING',
            },
            'newImage': {
                'pk': 'local',
                'sk': 'THING',
                'awsregion': 'us-west-2',
            },
        },
    ])

    uows = from_dynamodb(records)

    expect(len(uows)).to(equal(1))
    expect(uows[0]['event']['partition_key']).to(equal('local'))


def test_from_dynamodb_deleted_and_undeleted_suffixes(monkeypatch):
    monkeypatch.setenv('REGION', 'us-west-2')
    records = to_dynamodb_records([
        {
            'timestamp': 1,
            'keys': {
                'pk': 'deleted',
                'sk': 'THING',
            },
            'oldImage': {
                'pk': 'deleted',
                'sk': 'THING',
                'deleted': True,
            },
            'newImage': {
                'pk': 'deleted',
                'sk': 'THING',
                'deleted': False,
            },
        },
        {
            'timestamp': 2,
            'keys': {
                'pk': 'undeleted',
                'sk': 'THING',
            },
            'oldImage': {
                'pk': 'undeleted',
                'sk': 'THING',
                'deleted': True,
            },
            'newImage': {
                'pk': 'undeleted',
                'sk': 'THING',
                'deleted': False,
            },
        },
    ])
    records['Records'][0]['dynamodb']['NewImage']['deleted']['BOOL'] = True

    uows = from_dynamodb(records)

    expect(uows[0]['event']['type']).to(equal('THING-deleted'))
    expect(uows[1]['event']['type']).to(equal('THING-undeleted'))
