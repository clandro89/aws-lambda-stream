from expects import equal, expect
from reactivex import from_list
from aws_lambda_stream.utils import s3


class Body:
    def read(self):
        return b'content'


class Connector:
    def __init__(self, bucket_name=None):
        self.bucket_name = bucket_name
        self.put_requests = []
        self.get_requests = []
        self.list_requests = []
        self.pages = [
            {
                'Contents': [
                    {
                        'Key': 'one',
                    },
                ],
                'IsTruncated': True,
                'NextContinuationToken': 'next',
            },
            {
                'Contents': [
                    {
                        'Key': 'two',
                    },
                ],
                'IsTruncated': False,
            },
        ]

    def put_object(self, request):
        self.put_requests.append(request)
        return {
            'ETag': 'etag',
        }

    def get_object(self, request):
        self.get_requests.append(request)
        return {
            'Body': Body(),
        }

    def list_objects(self, request):
        self.list_requests.append(request)
        return self.pages.pop(0)


def test_put_object_to_s3():
    connector = Connector()
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'put_request': {
                'Bucket': 'bucket',
                'Key': 'key',
            },
        },
    ]).pipe(
        s3.put_object_to_s3(connector),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(connector.put_requests).to(equal([{'Bucket': 'bucket', 'Key': 'key'}]))
    expect(collected[0]['put_response']).to(equal({'ETag': 'etag'}))


def test_to_get_object_request():
    expect(s3.to_get_object_request({
        'record': {
            's3': {
                'bucket': {
                    'name': 'bucket',
                },
                'object': {
                    'key': 'key',
                },
            },
        },
    })['get_request']).to(equal({
        'Bucket': 'bucket',
        'Key': 'key',
    }))


def test_get_object_from_s3(monkeypatch):
    connector = Connector('bucket')
    monkeypatch.setattr(s3, 'Connector', lambda bucket_name=None: connector)
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'get_request': {
                'Bucket': 'bucket',
                'Key': 'key',
            },
        },
    ]).pipe(
        s3.get_object_from_s3('bucket'),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(connector.get_requests).to(equal([{'Bucket': 'bucket', 'Key': 'key'}]))
    expect(collected[0]['get_response']).to(equal(b'content'))


def test_page_objects_from_s3(monkeypatch):
    connector = Connector('bucket')
    monkeypatch.setattr(s3, 'Connector', lambda bucket_name=None: connector)
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'list_request': {
                'Bucket': 'bucket',
            },
        },
    ]).pipe(
        s3.page_objects_from_s3('bucket'),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect([uow['list_response']['Content']['Key'] for uow in collected]).to(equal([
        'one',
        'two',
    ]))
    expect(connector.list_requests).to(equal([
        {
            'Bucket': 'bucket',
        },
        {
            'Bucket': 'bucket',
            'ContinuationToken': 'next',
        },
    ]))
