from decimal import Decimal
from expects import equal, expect, have_keys
from reactivex import from_list
from aws_lambda_stream.utils import dynamodb
from aws_lambda_stream.utils.dynamodb import (
    batch_get_dynamodb,
    marshall,
    pk_condition,
    put_dynamodb,
    query_dynamodb,
    timestamp_condition,
    unmarshall,
    update_dynamodb,
    update_expression,
)


def test_update_expression():
    item = {'pk': 'abc', 'st': 'thing', 'name': 'thing name','as':None}
    result = update_expression(item)
    expect(result).to(
        have_keys(
            'ExpressionAttributeNames',
            'ExpressionAttributeValues',
            'UpdateExpression'
            )
    )


class Connector:
    def __init__(self, table_name=None, retry_config=None):
        self.table_name = table_name
        self.retry_config = retry_config
        self.updates = []
        self.puts = []
        self.batch_gets = []
        self.queries = []
        self.update_responses = []

    def update(self, request):
        self.updates.append(request)
        if self.update_responses:
            return self.update_responses.pop(0)
        return {
            'Attributes': request,
        }

    def put(self, request):
        self.puts.append(request)
        return {
            'ok': True,
        }

    def batch_get(self, request):
        self.batch_gets.append(request)
        return [
            {
                'id': 'thing-1',
            },
        ]

    def query_all(self, request):
        self.queries.append(request)
        return [
            {
                'id': 'thing-1',
            },
        ]


def test_conditions():
    expect(timestamp_condition()).to(equal({
        'ConditionExpression': 'attribute_not_exists(#timestamp) OR #timestamp < :timestamp',
    }))
    expect(pk_condition()).to(equal({
        'ConditionExpression': 'attribute_not_exists(pk)',
    }))


def test_marshall_and_unmarshall():
    item = {
        'pk': 'thing-1',
        'count': 1,
        'price': Decimal('12.5'),
    }

    expect(unmarshall(marshall(item))).to(equal({
        'pk': 'thing-1',
        'count': 1,
        'price': 12.5,
    }))


def test_update_dynamodb(monkeypatch):
    connector = Connector('table')
    monkeypatch.setattr(dynamodb, 'Connector', lambda table_name=None: connector)

    result = update_dynamodb('table')({
        'update_request': {
            'Key': {
                'pk': 'thing-1',
            },
        },
    })

    expect(connector.updates).to(equal([
        {
            'Key': {
                'pk': 'thing-1',
            },
        },
    ]))
    expect(result['update_response']).to(equal({
        'Attributes': {
            'Key': {
                'pk': 'thing-1',
            },
        },
    }))


def test_update_dynamodb_skips_empty_request(monkeypatch):
    connector = Connector('table')
    monkeypatch.setattr(dynamodb, 'Connector', lambda table_name=None: connector)
    uow = {
        'event': {
            'id': 'evt-1',
        },
    }

    expect(update_dynamodb('table')(uow)).to(equal(uow))
    expect(connector.updates).to(equal([]))


def test_update_dynamodb_uses_fallback(monkeypatch):
    connector = Connector('table')
    connector.update_responses = [
        {},
        {
            'Attributes': {
                'fallback': True,
            },
        },
    ]
    monkeypatch.setattr(dynamodb, 'Connector', lambda table_name=None: connector)

    result = update_dynamodb('table')({
        'update_request': {
            'Key': {
                'pk': 'primary',
            },
        },
        'fallback_update_request': {
            'Key': {
                'pk': 'fallback',
            },
        },
    })

    expect(connector.updates).to(equal([
        {
            'Key': {
                'pk': 'primary',
            },
        },
        {
            'Key': {
                'pk': 'fallback',
            },
        },
    ]))
    expect(result['update_response']).to(equal({
        'Attributes': {
            'fallback': True,
        },
    }))


def test_put_dynamodb(monkeypatch):
    connector = Connector('table')
    monkeypatch.setattr(dynamodb, 'Connector', lambda table_name=None: connector)

    result = put_dynamodb('table')({
        'put_request': {
            'Item': {
                'pk': 'thing-1',
            },
        },
    })

    expect(connector.puts).to(equal([
        {
            'Item': {
                'pk': 'thing-1',
            },
        },
    ]))
    expect(result['put_response']).to(equal({'ok': True}))


def test_batch_get_dynamodb(monkeypatch):
    connector = Connector('table')
    monkeypatch.setattr(dynamodb, 'Connector', lambda table_name=None, retry_config=None: connector)
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'batch_get_request': {
                'Keys': [],
            },
        },
        {
            'other': True,
        },
    ]).pipe(
        batch_get_dynamodb('table'),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(connector.batch_gets).to(equal([{'Keys': []}]))
    expect(collected[0]['batch_get_response']).to(equal([{'id': 'thing-1'}]))
    expect(collected[1]).to(equal({'other': True}))


def test_query_dynamodb(monkeypatch):
    connector = Connector('table')
    monkeypatch.setattr(dynamodb, 'Connector', lambda table_name=None: connector)
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'query_request': {
                'KeyConditionExpression': 'pk = :pk',
            },
        },
        {
            'other': True,
        },
    ]).pipe(
        query_dynamodb('table'),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(connector.queries).to(equal([{'KeyConditionExpression': 'pk = :pk'}]))
    expect(collected[0]['query_response']).to(equal([{'id': 'thing-1'}]))
    expect(collected[1]).to(equal({'other': True}))
