from expects import equal, expect
from aws_lambda_stream.connectors.cloudwatch import Connector as CloudwatchConnector
from aws_lambda_stream.connectors.eventbridge import Connector as EventbridgeConnector
from aws_lambda_stream.connectors.lambda_ import Connector as LambdaConnector


class Client:
    def __init__(self):
        self.calls = []

    def put_metric_data(self, **kwargs):
        self.calls.append(('put_metric_data', kwargs))
        return {
            'metric': True,
        }

    def put_events(self, **kwargs):
        self.calls.append(('put_events', kwargs))
        return {
            'events': True,
        }

    def invoke(self, **kwargs):
        self.calls.append(('invoke', kwargs))
        return {
            'lambda': True,
        }


def test_cloudwatch_put():
    client = Client()

    result = CloudwatchConnector(client).put({
        'Namespace': 'App',
        'MetricData': [],
    })

    expect(client.calls).to(equal([
        ('put_metric_data', {
            'Namespace': 'App',
            'MetricData': [],
        }),
    ]))
    expect(result).to(equal({'metric': True}))


def test_eventbridge_put_events():
    client = Client()

    result = EventbridgeConnector(client).put_events({
        'Entries': [],
    })

    expect(client.calls).to(equal([
        ('put_events', {
            'Entries': [],
        }),
    ]))
    expect(result).to(equal({'events': True}))


def test_lambda_invoke():
    client = Client()

    result = LambdaConnector(client).invoke({
        'FunctionName': 'fn',
    })

    expect(client.calls).to(equal([
        ('invoke', {
            'FunctionName': 'fn',
        }),
    ]))
    expect(result).to(equal({'lambda': True}))
