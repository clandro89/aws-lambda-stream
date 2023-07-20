from aws_lambda_stream.flavors.correlate import correlate


rules = [
    {
        'id': 'crl1',
        'flavor': correlate,
        'event_type': 'thing-submitted',
        'correlation_key': 'thing.another_relationship',
    },
]
