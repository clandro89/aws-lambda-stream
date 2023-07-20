from aws_lambda_stream.flavors.collect import collect

rules = [
    {
        'id': 'clt1',
        'flavor': collect,
        'event_type': [
            'thing-submitted'
        ],
    },
]
