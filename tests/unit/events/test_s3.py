from expects import equal, expect
from aws_lambda_stream.events.s3 import from_s3, to_s3_records


def test_from_s3_created_events():
    records = to_s3_records([
        {
            'bucket': {
                'name': 'bucket',
            },
            'object': {
                'key': 'thing.json',
            },
        },
    ])

    uows = from_s3(records)

    expect(len(uows)).to(equal(1))
    expect(uows[0]['event']).to(equal({
        'id': 'thing.json',
        'type': 'object-created',
        's3': records['Records'][0]['s3'],
    }))


def test_from_s3_deleted_events():
    records = to_s3_records([
        {
            'bucket': {
                'name': 'bucket',
            },
            'object': {
                'key': 'thing.json',
            },
        },
    ])
    records['Records'][0]['eventName'] = 'ObjectCreated:Delete'

    expect(from_s3(records)[0]['event']['type']).to(equal('object-deleted'))


def test_from_s3_unknown_events():
    records = to_s3_records([
        {
            'bucket': {},
            'object': {
                'key': 'thing.json',
            },
        },
    ])
    records['Records'][0]['eventName'] = 'ObjectRestore:Completed'

    expect(from_s3(records)[0]['event']['type']).to(equal(None))
