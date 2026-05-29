import json
from expects import equal, expect
from aws_lambda_stream.events.sns import from_sns, to_sns_records


def test_from_sns():
    records = to_sns_records([
        {
            'msg': json.dumps({
                'type': 'thing-created',
                'thing': {
                    'id': 'thing-1',
                },
            }),
            'subject': 'Thing Created',
        },
    ])

    uows = from_sns(records)

    expect(uows[0]['event']).to(equal({
        'id': '00000000-0000-0000-0000-000000000000',
        'type': 'thing-created',
        'thing': {
            'id': 'thing-1',
        },
    }))
    expect(uows[0]['record']).to(equal(records['Records'][0]))
