from expects import equal, expect
from aws_lambda_stream.connectors import opensearch
from aws_lambda_stream.connectors.opensearch import Connector


class Response:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class Requests:
    def __init__(self):
        self.calls = []

    def put(self, url, **kwargs):
        self.calls.append(('put', url, kwargs))
        return Response({
            'method': 'put',
            'url': url,
        })

    def get(self, url, **kwargs):
        self.calls.append(('get', url, kwargs))
        return Response({
            'method': 'get',
            'url': url,
        })

    def delete(self, url, **kwargs):
        self.calls.append(('delete', url, kwargs))
        return Response({
            'method': 'delete',
            'url': url,
        })

    def post(self, url, **kwargs):
        self.calls.append(('post', url, kwargs))
        return Response({
            'method': 'post',
            'url': url,
        })


def connector():
    value = Connector('https://search.example.com', 'us-west-2', None)
    value.awsauth = 'auth'
    return value


def test_opensearch_index_methods(monkeypatch):
    requests = Requests()
    monkeypatch.setattr(opensearch, 'requests', requests)
    conn = connector()

    expect(conn.create_index('things', {'settings': {}})).to(equal({
        'method': 'put',
        'url': 'https://search.example.com/things',
    }))
    expect(conn.get_index('things')).to(equal({
        'method': 'get',
        'url': 'https://search.example.com/things',
    }))
    expect(conn.update_mapping('things', {'properties': {}})).to(equal({
        'method': 'put',
        'url': 'https://search.example.com/things/_mapping',
    }))
    expect(conn.delete_index('things')).to(equal({
        'method': 'delete',
        'url': 'https://search.example.com/things',
    }))
    expect(conn.reindex({'source': {}, 'dest': {}})).to(equal({
        'method': 'post',
        'url': 'https://search.example.com/_reindex',
    }))
    expect(conn.create_or_update_alias('things', 'current', None)).to(equal({
        'method': 'put',
        'url': 'https://search.example.com/things/_alias/current',
    }))
    expect(requests.calls[0][2]['auth']).to(equal('auth'))
    expect(requests.calls[0][2]['headers']).to(equal({
        'Content-Type': 'application/json',
    }))
    expect(requests.calls[5][2]['json']).to(equal({}))


def test_opensearch_document_methods(monkeypatch):
    requests = Requests()
    monkeypatch.setattr(opensearch, 'requests', requests)
    conn = connector()

    expect(conn.search('things', {'size': 10, 'from': None}, {'q': 'thing'})).to(equal({
        'method': 'post',
        'url': 'https://search.example.com/things/_search',
    }))
    expect(conn.put('things', 'thing-1', {'name': 'Thing'})).to(equal({
        'method': 'put',
        'url': 'https://search.example.com/things/_doc/thing-1',
    }))
    expect(conn.delete('things', 'thing-1')).to(equal({
        'method': 'delete',
        'url': 'https://search.example.com/things/_doc/thing-1',
    }))
    expect(conn.bulk('payload')).to(equal({
        'method': 'post',
        'url': 'https://search.example.com/_bulk',
    }))
    expect(requests.calls[0][2]['json']).to(equal({'size': 10}))
    expect(requests.calls[0][2]['params']).to(equal({'q': 'thing'}))
    expect(requests.calls[3][2]['headers']).to(equal({
        'Content-Type': 'application/x-ndjson',
    }))
    expect(requests.calls[3][2]['data']).to(equal('payload'))


class Credentials:
    access_key = 'access'
    secret_key = 'secret'
    token = 'token'


class Session:
    def get_credentials(self):
        return Credentials()


def test_opensearch_auth(monkeypatch):
    created = []

    class AWS4Auth:
        def __init__(self, *args, **kwargs):
            created.append((args, kwargs))

    import requests_aws4auth
    monkeypatch.setattr(requests_aws4auth, 'AWS4Auth', AWS4Auth)
    conn = Connector('https://search.example.com', 'us-west-2', Session())
    conn.awsauth = None

    expect(isinstance(conn.auth, AWS4Auth)).to(equal(True))
    expect(created).to(equal([
        (
            ('access', 'secret', 'us-west-2', 'es'),
            {
                'session_token': 'token',
            },
        ),
    ]))
    expect(conn.auth is conn.awsauth).to(equal(True))
