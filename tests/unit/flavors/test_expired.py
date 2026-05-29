from expects import equal, expect
from pydash import get
from reactivex import Observable
from aws_lambda_stream.events.dynamodb import from_dynamodb, to_dynamodb_records
from aws_lambda_stream.flavors.expired import expired
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.operators import rx_map
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS


def _publish(_):
    def wrapper(source: Observable):
        return source.pipe(
            rx_map(lambda uow: {
                **uow,
                'publish_response': 'OK'
            })
        )
    return wrapper


def test_expired(monkeypatch):
    monkeypatch.setenv('LOG_LEVEL', 'DEBUG')
    events = to_dynamodb_records([
        {
            'timestamp': 1548967023,
            'keys': {
                'pk': 'not-expired',
                'sk': 'EVENT'
            },
            'newImage': {
                'pk': 'not-expired',
                'sk': 'EVENT',
                'discriminator': 'EVENT',
                'timestamp': 1548967022000,
                'ttl': 1548967022,
                'expire': True,
                'event': {
                    'id': 'not-expired',
                    'type': 'thing-created',
                    'timestamp': 1548967022000,
                }
            }
        },
        {
            'timestamp': 1548967023,
            'keys': {
                'pk': 'no-expire',
                'sk': 'EVENT'
            },
            'oldImage': {
                'pk': 'no-expire',
                'sk': 'EVENT',
                'discriminator': 'EVENT',
                'timestamp': 1548967022000,
                'ttl': 1548967022,
                'event': {
                    'id': 'no-expire',
                    'type': 'thing-created',
                    'timestamp': 1548967022000,
                }
            }
        },
        {
            'timestamp': 1548967021,
            'keys': {
                'pk': 'before-ttl',
                'sk': 'EVENT'
            },
            'oldImage': {
                'pk': 'before-ttl',
                'sk': 'EVENT',
                'discriminator': 'EVENT',
                'timestamp': 1548967022000,
                'ttl': 1548967022,
                'expire': True,
                'event': {
                    'id': 'before-ttl',
                    'type': 'thing-created',
                    'timestamp': 1548967022000,
                }
            }
        },
        {
            'timestamp': 1548967023,
            'keys': {
                'pk': 'expired',
                'sk': 'EVENT'
            },
            'oldImage': {
                'pk': 'expired',
                'sk': 'EVENT',
                'discriminator': 'EVENT',
                'timestamp': 1548967022000,
                'ttl': 1548967022,
                'expire': True,
                'event': {
                    'id': 'expired',
                    'type': 'thing.created',
                    'timestamp': 1548967022345,
                }
            }
        },
        {
            'timestamp': 1548967024,
            'keys': {
                'pk': 'expired-custom',
                'sk': 'EVENT'
            },
            'oldImage': {
                'pk': 'expired-custom',
                'sk': 'EVENT',
                'discriminator': 'EVENT',
                'timestamp': 1548967023000,
                'ttl': 1548967023,
                'expire': 'thing-timeout',
                'event': {
                    'id': 'expired-custom',
                    'type': 'thing-created',
                    'timestamp': 1548967023007,
                }
            }
        },
    ])

    collected = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from([
            {
                'id': 'expired',
                'flavor': expired,
                'publish': _publish,
            }
        ]),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_dynamodb(events),
        on_next=_on_next,
    )

    expect(len(collected)).to(equal(2))
    expect(get(collected, '[0].emit')).to(equal({
        'id': '3',
        'type': 'thing.created.expired',
        'timestamp': 1548967022345,
        'triggers': [
            {
                'id': 'expired',
                'type': 'thing.created',
                'timestamp': 1548967022345
            }
        ]
    }))
    expect(get(collected, '[0].publish_response')).to(equal('OK'))
    expect(get(collected, '[1].emit.type')).to(equal('thing-timeout'))
    expect(get(collected, '[1].emit.timestamp')).to(equal(1548967023007))
