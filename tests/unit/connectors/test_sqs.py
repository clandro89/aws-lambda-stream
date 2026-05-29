from expects import equal, expect
from aws_lambda_stream.connectors.sqs import Connector


class Client:
    def __init__(self):
        self.calls = []

    def send_message(self, **kwargs):
        self.calls.append(('send_message', kwargs))
        return {
            'MessageId': 'message',
        }

    def send_message_batch(self, **kwargs):
        self.calls.append(('send_message_batch', kwargs))
        return {
            'Successful': [],
        }


def test_sqs_connector_methods_merge_queue_url():
    client = Client()
    connector = Connector('queue-url', client)

    expect(connector.send_message({'MessageBody': 'hello'})).to(equal({
        'MessageId': 'message',
    }))
    expect(connector.send_message_batch({'Entries': []})).to(equal({
        'Successful': [],
    }))
    expect(client.calls).to(equal([
        ('send_message', {
            'QueueUrl': 'queue-url',
            'MessageBody': 'hello',
        }),
        ('send_message_batch', {
            'QueueUrl': 'queue-url',
            'Entries': [],
        }),
    ]))
