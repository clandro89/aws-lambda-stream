from expects import equal, expect
from aws_lambda_stream.connectors.s3 import Connector


class Client:
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(('put_object', kwargs))
        return {
            'put': True,
        }

    def get_object(self, **kwargs):
        self.calls.append(('get_object', kwargs))
        return {
            'get': True,
        }

    def list_objects_v2(self, **kwargs):
        self.calls.append(('list_objects_v2', kwargs))
        return {
            'list': True,
        }

    def delete_object(self, **kwargs):
        self.calls.append(('delete_object', kwargs))
        return {
            'delete': True,
        }


def test_s3_connector_methods_merge_bucket():
    client = Client()
    connector = Connector('bucket', client)

    expect(connector.put_object({'Key': 'key'})).to(equal({'put': True}))
    expect(connector.get_object({'Key': 'key'})).to(equal({'get': True}))
    expect(connector.list_objects({'Prefix': 'things/'})).to(equal({'list': True}))
    expect(connector.delete_object({'Key': 'key'})).to(equal({'delete': True}))
    expect(client.calls).to(equal([
        ('put_object', {
            'Bucket': 'bucket',
            'Key': 'key',
        }),
        ('get_object', {
            'Bucket': 'bucket',
            'Key': 'key',
        }),
        ('list_objects_v2', {
            'Bucket': 'bucket',
            'Prefix': 'things/',
        }),
        ('delete_object', {
            'Bucket': 'bucket',
            'Key': 'key',
        }),
    ]))
