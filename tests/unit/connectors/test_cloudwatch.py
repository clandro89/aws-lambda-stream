from expects import equal, expect
from aws_lambda_stream.connectors.cloudwatch import Connector


class Client:
    def __init__(self):
        self.calls = []

    def put_metric_data(self, **kwargs):
        self.calls.append(kwargs)
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
            },
        }


def test_cloudwatch_put_metric_data():
    client = Client()

    result = Connector(client).put({
        'Namespace': 'App',
        'MetricData': [
            {
                'MetricName': 'ProcessedEvents',
                'Value': 1,
            },
        ],
    })

    expect(client.calls).to(equal([
        {
            'Namespace': 'App',
            'MetricData': [
                {
                    'MetricName': 'ProcessedEvents',
                    'Value': 1,
                },
            ],
        },
    ]))
    expect(result).to(equal({
        'ResponseMetadata': {
            'HTTPStatusCode': 200,
        },
    }))
