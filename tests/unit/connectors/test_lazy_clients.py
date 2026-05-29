from expects import equal, expect
from aws_lambda_stream.connectors.cloudwatch import Connector as CloudwatchConnector
from aws_lambda_stream.connectors.dynamodb import Connector as DynamoConnector
from aws_lambda_stream.connectors.eventbridge import Connector as EventbridgeConnector
from aws_lambda_stream.connectors.lambda_ import Connector as LambdaConnector
from aws_lambda_stream.connectors.opensearch import Connector as OpenSearchConnector
from aws_lambda_stream.connectors.s3 import Connector as S3Connector
from aws_lambda_stream.connectors.sns import Connector as SnsConnector
from aws_lambda_stream.connectors.sqs import Connector as SqsConnector


class Boto:
    def __init__(self):
        self.client_calls = []
        self.resource_calls = []
        self.sessions = []

    def client(self, *args, **kwargs):
        self.client_calls.append((args, kwargs))
        return {
            'client': args[0],
            'kwargs': kwargs,
        }

    def resource(self, *args, **kwargs):
        self.resource_calls.append((args, kwargs))
        return {
            'resource': args[0],
            'kwargs': kwargs,
        }

    def Session(self):  # pylint: disable=invalid-name
        session = {
            'session': True,
        }
        self.sessions.append(session)
        return session


def test_lazy_boto_clients(monkeypatch):
    import boto3

    boto = Boto()
    monkeypatch.setattr(boto3, 'client', boto.client)
    monkeypatch.setattr(boto3, 'resource', boto.resource)
    monkeypatch.setattr(boto3, 'Session', boto.Session)

    expect(CloudwatchConnector().client).to(equal({'client': 'cloudwatch', 'kwargs': {}}))
    expect(EventbridgeConnector().client).to(equal({'client': 'events', 'kwargs': {}}))
    expect(LambdaConnector().client).to(equal({'client': 'lambda', 'kwargs': {}}))
    expect(S3Connector('bucket').client).to(equal({'client': 's3', 'kwargs': {}}))
    expect(SqsConnector('queue').client).to(equal({'client': 'sqs', 'kwargs': {}}))
    expect(SnsConnector('arn:aws:sns:us-east-1:123:topic').client).to(equal({
        'client': 'sns',
        'kwargs': {
            'region_name': 'us-east-1',
        },
    }))
    expect(DynamoConnector('table').client).to(equal({'resource': 'dynamodb', 'kwargs': {}}))
    expect(OpenSearchConnector('https://search.example.com').aws_session).to(equal({
        'session': True,
    }))

    expect(boto.client_calls).to(equal([
        (('cloudwatch',), {}),
        (('events',), {}),
        (('lambda',), {}),
        (('s3',), {}),
        (('sqs',), {}),
        (('sns',), {
            'region_name': 'us-east-1',
        }),
    ]))
    expect(boto.resource_calls).to(equal([
        (('dynamodb',), {}),
    ]))
    expect(boto.sessions).to(equal([
        {
            'session': True,
        },
    ]))
