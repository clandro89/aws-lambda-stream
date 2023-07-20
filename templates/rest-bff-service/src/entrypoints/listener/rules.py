from aws_lambda_stream import materialize
from src.core.domain.models.thing import to_update_request


rules = [
    {
        'id': 'thing-materialize',
        'flavor': materialize,
        'event_type': [
            'thing-created',
            'thing-updated',
            'thing-deleted',
            'thing-undeleted',
        ],
        'to_update_request': to_update_request,
    },
]
