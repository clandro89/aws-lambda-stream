import json
from expects import equal, expect
from aws_lambda_stream.events.sqs import from_sqs, to_sqs_records


def test_from_sqs():
    records = to_sqs_records([
        {
            'body': json.dumps({
                'type': 'thing-created',
                'thing': {
                    'id': 'thing-1',
                },
            }),
        },
    ])

    uows = from_sqs(records)

    expect(uows[0]['event']).to(equal({
        'id': '00000000-0000-0000-0000-000000000000',
        'type': 'thing-created',
        'thing': {
            'id': 'thing-1',
        },
    }))
    expect(uows[0]['record']).to(equal(records['Records'][0]))
