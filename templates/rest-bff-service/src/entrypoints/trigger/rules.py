from aws_lambda_stream.flavors.cdc import cdc



rules = [
    {
        'id': 'thing-cdc',
        'flavor': cdc,
        'event_type': [
            'thing-created',
            'thing-updated',
            'thing-deleted',
            'thing-undeleted',
        ],
        'to_event': lambda uow: {
            'thing': uow['event']['raw']['new'],
            'raw': None,
        },
    },
]
