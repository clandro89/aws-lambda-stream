from expects import contain, equal, expect
from reactivex import from_list
from aws_lambda_stream.utils import elasticsearch


class Connector:
    def __init__(self):
        self.bulk_requests = []
        self.search_requests = []

    def bulk(self, request):
        self.bulk_requests.append(request)
        return {
            'errors': False,
        }

    def search(self, **kwargs):
        self.search_requests.append(kwargs)
        return {
            'hits': [],
        }


def test_update_elasticsearch():
    connector = Connector()
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'update_request': {
                'index': 'things',
                'id': 'thing-1',
                'data': {
                    'name': 'Thing One',
                },
            },
        },
        {
            'ignored': True,
        },
    ]).pipe(
        elasticsearch.update_elasticsearch(connector, batch_size=2),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(len(connector.bulk_requests)).to(equal(1))
    expect(connector.bulk_requests[0]).to(contain('"index": {"_index": "things", "_id": "thing-1"}'))
    expect(connector.bulk_requests[0]).to(contain('"name": "Thing One"'))
    expect(collected).to(equal([
        {
            'update_request': {
                'index': 'things',
                'id': 'thing-1',
                'data': {
                    'name': 'Thing One',
                },
            },
        },
        {
            'ignored': True,
        },
    ]))


def test_query_elasticsearch(monkeypatch):
    connector = Connector()
    monkeypatch.setattr(elasticsearch, 'Connector', lambda host=None, region=None: connector)
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'query_request': {
                'index': 'things',
                'payload': {
                    'size': 1,
                },
                'query': {
                    'match_all': {},
                },
                'ignored': True,
            },
        },
        {
            'other': True,
        },
    ]).pipe(
        elasticsearch.query_elasticsearch(host='host', region='region'),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(connector.search_requests).to(equal([
        {
            'index': 'things',
            'payload': {
                'size': 1,
            },
            'query': {
                'match_all': {},
            },
        },
    ]))
    expect(collected[0]['query_response']).to(equal({'hits': []}))
    expect(collected[1]).to(equal({'other': True}))
