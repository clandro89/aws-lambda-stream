from expects import equal, expect, raise_error
from aws_lambda_stream.connectors import dynamodb
from aws_lambda_stream.connectors.dynamodb import (
    Connector,
    accumulate,
    unprocessed,
)


class BatchWriter:
    def __init__(self, table):
        self.table = table

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def put_item(self, Item):  # pylint: disable=invalid-name
        self.table.batch_puts.append(Item)

    def delete_item(self, Key):  # pylint: disable=invalid-name
        self.table.batch_deletes.append(Key)


class Table:
    def __init__(self):
        self.calls = []
        self.query_pages = []
        self.batch_puts = []
        self.batch_deletes = []

    def get_item(self, **kwargs):
        self.calls.append(('get_item', kwargs))
        return {
            'Item': kwargs,
        }

    def update_item(self, **kwargs):
        self.calls.append(('update_item', kwargs))
        return {
            'Attributes': kwargs,
        }

    def put_item(self, **kwargs):
        self.calls.append(('put_item', kwargs))
        return {
            'ok': True,
        }

    def query(self, **kwargs):
        self.calls.append(('query', kwargs))
        return self.query_pages.pop(0) if self.query_pages else {
            'Items': [],
        }

    def batch_writer(self):
        return BatchWriter(self)


class Client:
    def __init__(self):
        self.table = Table()
        self.batch_get_responses = []
        self.batch_get_calls = []

    def Table(self, _):  # pylint: disable=invalid-name
        return self.table

    def batch_get_item(self, **kwargs):
        self.batch_get_calls.append(kwargs)
        return self.batch_get_responses.pop(0)


def test_dynamodb_connector_table_methods():
    client = Client()
    connector = Connector('table', client=client)

    expect(connector.get({'Key': {'pk': '1'}})).to(equal({'Item': {'Key': {'pk': '1'}}}))
    expect(connector.update({'Key': {'pk': '1'}})).to(equal({'Attributes': {'Key': {'pk': '1'}}}))
    expect(connector.put({'Item': {'pk': '1'}})).to(equal({'ok': True}))
    expect(connector.query({'KeyConditionExpression': 'pk = :pk'})).to(equal({'Items': []}))
    expect(client.table.calls).to(equal([
        ('get_item', {'Key': {'pk': '1'}}),
        ('update_item', {'Key': {'pk': '1'}}),
        ('put_item', {'Item': {'pk': '1'}}),
        ('query', {'KeyConditionExpression': 'pk = :pk'}),
    ]))


def test_query_all_pages_results():
    client = Client()
    client.table.query_pages = [
        {
            'Items': [
                {
                    'id': '1',
                },
            ],
            'LastEvaluatedKey': {
                'pk': '1',
            },
        },
        {
            'Items': [
                {
                    'id': '2',
                },
            ],
        },
    ]
    connector = Connector('table', client=client)
    params = {
        'KeyConditionExpression': 'pk = :pk',
    }

    expect(connector.query_all(params)).to(equal([
        {
            'id': '1',
        },
        {
            'id': '2',
        },
    ]))
    expect(client.table.calls[1][1]['ExclusiveStartKey']).to(equal({'pk': '1'}))


def test_batch_get_retries_unprocessed_keys(monkeypatch):
    client = Client()
    client.batch_get_responses = [
        {
            'Responses': {
                'table': [
                    {
                        'id': '1',
                    },
                ],
            },
            'UnprocessedKeys': {
                'table': {
                    'Keys': [
                        {
                            'pk': '2',
                        },
                    ],
                },
            },
        },
        {
            'Responses': {
                'table': [
                    {
                        'id': '2',
                    },
                ],
            },
        },
    ]
    monkeypatch.setattr(dynamodb, 'wait', lambda *_: None)
    monkeypatch.setattr(dynamodb, 'get_delay', lambda *_: 0)
    connector = Connector('table', retry_config={'max_retries': 2, 'retry_wait': 1}, client=client)

    expect(connector.batch_get({
        'RequestItems': {
            'table': {
                'Keys': [
                    {
                        'pk': '1',
                    },
                    {
                        'pk': '2',
                    },
                ],
            },
        },
    })).to(equal({
            'Responses': {
                'table': [
                    {
                        'id': '1',
                    },
                    {
                        'id': '2',
                    },
                ],
            },
        'attempts': [
            {
                'Responses': {
                    'table': [
                        {
                            'id': '1',
                        },
                    ],
                },
                'UnprocessedKeys': {
                    'table': {
                        'Keys': [
                            {
                                'pk': '2',
                            },
                        ],
                    },
                },
            },
            {
                'Responses': {
                    'table': [
                        {
                            'id': '2',
                        },
                    ],
                },
            },
        ],
    }))
    expect(client.batch_get_calls[1]).to(equal({
        'RequestItems': {
            'table': {
                'Keys': [
                    {
                        'pk': '2',
                    },
                ],
            },
        },
    }))


def test_batch_get_raises_after_max_retries(monkeypatch):
    client = Client()
    client.batch_get_responses = [
        {
            'Responses': {},
            'UnprocessedKeys': {
                'table': {
                    'Keys': [
                        {
                            'pk': '1',
                        },
                    ],
                },
            },
        },
        {
            'Responses': {},
            'UnprocessedKeys': {
                'table': {
                    'Keys': [
                        {
                            'pk': '1',
                        },
                    ],
                },
            },
        },
    ]
    monkeypatch.setattr(dynamodb, 'wait', lambda *_: None)
    monkeypatch.setattr(dynamodb, 'get_delay', lambda *_: 0)
    connector = Connector('table', retry_config={'max_retries': 1, 'retry_wait': 1}, client=client)

    expect(lambda: connector.batch_get({'RequestItems': {'table': {'Keys': []}}})).to(
        raise_error(Exception)
    )


def test_bulk_insert_and_delete():
    client = Client()
    connector = Connector('table', client=client)

    connector.bulk_insert([
        {
            'pk': '1',
        },
    ])
    connector.bulk_delete([
        {
            'pk': '1',
        },
    ])

    expect(client.table.batch_puts).to(equal([{'pk': '1'}]))
    expect(client.table.batch_deletes).to(equal([{'pk': '1'}]))


def test_unprocessed_and_accumulate_helpers():
    expect(unprocessed({
        'RequestItems': {
            'table': {
                'Keys': [
                    {
                        'pk': '1',
                    },
                ],
            },
        },
    }, {
        'UnprocessedKeys': {
            'table': {
                'Keys': [
                    {
                        'pk': '2',
                    },
                ],
            },
        },
    })).to(equal({
        'RequestItems': {
            'table': {
                'Keys': [
                    {
                        'pk': '2',
                    },
                ],
            },
        },
    }))
    expect(accumulate([
        {
            'Responses': {
                'table': [
                    {
                        'id': '1',
                    },
                ],
            },
        },
    ], {
        'Responses': {
            'table': [
                {
                    'id': '2',
                },
            ],
        },
    })['Responses']['table']).to(equal([
        {
            'id': '1',
        },
        {
            'id': '2',
        },
    ]))
