from aws_lambda_stream import (ttl, update_expression, timestamp_condition)
from . import Model

DISCRIMINATOR = 'thing'

class Thing(Model):
    id: str
    created_at: str
    latched: bool = False
    name: str


def to_update_request(uow):
    return {
        'Key': {
            'pk': uow['event']['thing']['id'],
            'sk': DISCRIMINATOR,
        },
        **update_expression({
            **{k: v for k, v in uow['event']['thing'].items() if k not in ['pk', 'sk']},
            'discriminator': DISCRIMINATOR,
            'last_modified_by': 'system',
            'timestamp': uow['event']['timestamp'],
            'deleted': True if uow['event']['type'] == 'thing-deleted' else None,
            'latched': True,
            'ttl': ttl(uow['event']['timestamp'], 33),
        }),
        **timestamp_condition(),
    }
