from expects import equal, expect
from pydash import get
from aws_lambda_stream.connectors.dynamodb import Connector
from aws_lambda_stream.events.dynamodb import from_dynamodb, to_dynamodb_records
from aws_lambda_stream.events.kinesis import from_kinesis, to_kinesis_records
from aws_lambda_stream.flavors.update import update
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.dynamodb import update_expression
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS


def to_query_request(uow, _):
    return {
        'IndexName': 'DataIndex',
        'KeyConditionExpression': '#data = :data',
        'ExpressionAttributeNames': {
            '#data': 'data',
        },
        'ExpressionAttributeValues': {
            ':data': get(uow, 'event.thing.id'),
        },
        'ConsistentRead': True
    }


def to_get_request(uow, _):
    return {
        'RequestItems': {
            'Things': {
                'Keys': [
                    {
                        'pk': get(uow, 'query_response.entity_id'),
                        'sk': 'thing'
                    }
                ]
            }
        }
    }


def to_update_request(uow, _):
    entity = get(uow, 'batch_get_response.Responses.Things[0]', {})
    return {
        'Key': {
            'pk': get(uow, 'query_response.entity_id'),
            'sk': 'thing',
        },
        **update_expression({
            'last_event_id': get(uow, 'event.id'),
            'name': entity.get('name'),
        })
    }


def to_fallback_update_request(uow, _):
    return {
        'Key': {
            'pk': f"{get(uow, 'query_response.entity_id')}.fallback",
            'sk': 'thing',
        },
        **update_expression({
            'last_event_id': get(uow, 'event.id'),
        })
    }


def test_update_from_collected_event(monkeypatch):
    events = to_dynamodb_records([
        {
            'timestamp': 1548967023,
            'keys': {
                'pk': 'evt-1',
                'sk': 'EVENT'
            },
            'newImage': {
                'pk': 'evt-1',
                'sk': 'EVENT',
                'discriminator': 'EVENT',
                'timestamp': 1548967022000,
                'sequence_number': '0',
                'ttl': 1551818222,
                'data': 'thing-1',
                'event': {
                    'id': 'evt-1',
                    'type': 'thing-submitted',
                    'timestamp': 1548967022000,
                    'partition_key': 'thing-1',
                    'thing': {
                        'id': 'thing-1',
                    }
                }
            }
        }
    ])
    update_calls = []

    monkeypatch.setattr(Connector, 'query_all', lambda *_: [
        {'entity_id': 'thing-a'},
        {'entity_id': 'needs-fallback'},
    ])
    monkeypatch.setattr(Connector, 'batch_get', lambda _, params: {
        'Responses': {
            'Things': [
                {
                    'pk': get(params, 'RequestItems.Things.Keys[0].pk'),
                    'name': 'Thing One',
                }
            ]
        }
    })

    def _update(_, params):
        update_calls.append(params)
        if get(params, 'Key.pk') == 'needs-fallback':
            return {}
        return {'result': get(params, 'Key.pk')}

    monkeypatch.setattr(Connector, 'update', _update)

    collected = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from([
            {
                'id': 'upd1',
                'flavor': update,
                'event_type': 'thing-submitted',
                'table_name': 'Things',
                'to_query_request': to_query_request,
                'to_get_request': to_get_request,
                'to_update_request': to_update_request,
                'to_fallback_update_request': to_fallback_update_request,
            }
        ]),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_dynamodb(events),
        on_next=_on_next,
    )

    expect(len(collected)).to(equal(2))
    expect(get(collected, '[0].event.type')).to(equal('thing-submitted'))
    expect(get(collected, '[0].query_response')).to(equal({'entity_id': 'thing-a'}))
    expect(get(collected, '[0].batch_get_response.Responses.Things[0].pk')).to(
        equal('thing-a')
    )
    expect(get(collected, '[0].update_response')).to(equal({'result': 'thing-a'}))
    expect(get(collected, '[1].update_response')).to(
        equal({'result': 'needs-fallback.fallback'})
    )
    expect([get(c, 'Key.pk') for c in update_calls]).to(equal([
        'thing-a',
        'needs-fallback',
        'needs-fallback.fallback',
    ]))


def test_update_without_query(monkeypatch):
    events = to_kinesis_records([
        {
            'type': 'thing-direct',
            'timestamp': 1548967022000,
            'thing': {
                'id': 'thing-1',
                'name': 'Thing One',
            },
        }
    ])

    monkeypatch.setattr(Connector, 'update', lambda *_: {'result': 'OK'})

    collected = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from([
            {
                'id': 'upd-direct',
                'flavor': update,
                'event_type': 'thing-direct',
                'table_name': 'Things',
                'to_update_request': lambda uow, _: {
                    'Key': {
                        'pk': get(uow, 'event.thing.id'),
                        'sk': 'thing',
                    },
                    **update_expression({
                        'name': get(uow, 'event.thing.name')
                    })
                },
            }
        ]),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_kinesis(events),
        on_next=_on_next,
    )

    expect(len(collected)).to(equal(1))
    expect(get(collected, '[0].update_request.Key.pk')).to(equal('thing-1'))
    expect(get(collected, '[0].update_response')).to(equal({'result': 'OK'}))
