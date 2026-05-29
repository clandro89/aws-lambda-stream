from expects import equal, expect
from aws_lambda_stream.connectors.sns import Connector


class Client:
    def __init__(self):
        self.calls = []

    def publish(self, **kwargs):
        self.calls.append(('publish', kwargs))
        return {
            'MessageId': 'message',
        }

    def publish_batch(self, **kwargs):
        self.calls.append(('publish_batch', kwargs))
        return {
            'Successful': [],
        }


def test_sns_connector_methods_merge_topic_arn():
    client = Client()
    connector = Connector('arn:aws:sns:us-east-1:123:topic', client)

    expect(connector.publish({'Message': 'hello'})).to(equal({'MessageId': 'message'}))
    expect(connector.publish_batch({'PublishBatchRequestEntries': []})).to(equal({
        'Successful': [],
    }))
    expect(client.calls).to(equal([
        ('publish', {
            'TopicArn': 'arn:aws:sns:us-east-1:123:topic',
            'Message': 'hello',
        }),
        ('publish_batch', {
            'TopicArn': 'arn:aws:sns:us-east-1:123:topic',
            'PublishBatchRequestEntries': [],
        }),
    ]))
