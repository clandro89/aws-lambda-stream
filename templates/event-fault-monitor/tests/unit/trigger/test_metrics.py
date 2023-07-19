import json
from expects import equal, expect
from aws_lambda_stream.events.s3 import to_s3_records, from_s3
from aws_lambda_stream.connectors.cloudwatch import Connector
from aws_lambda_stream.connectors.s3 import Connector as S3Connector
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from src.entrypoints.trigger.rules import rules


def test_metrics(monkeypatch):

    monkeypatch.setattr(Connector, 'put', lambda *args, **kwargs: {})

    class ObjectResponse:
        def read(self): #pylint: disable=R0201
            data = {
                "detail": {
                    "id": "e0",
                    "type": "fault",
                    "timestamp": 1671042150000,
                    "tags": {
                        'account': 'dev',
                        'region': 'us-west-2',
                        'stage': 'test',
                        'source': 'undefined',
                        'functionname': 'f1',
                        'pipeline': 'p1',
                    },
                    "err": {
                        'name': 'Error',
                        'message': 'this is an error',
                        'stack': 'the stack trace'
                    },
                    "uow":{}
                },
            }
            return json.dumps(data).encode('ascii')

    monkeypatch.setattr(S3Connector, 'get_object',
                        lambda *args, **kwargs: {'Body': ObjectResponse()})
    events = to_s3_records([
        {
            "bucket": {
                "name": "event-fault-monitor-dev-bucket-1jbfm2f87k1la",
                "ownerIdentity": {
                    "principalId": "A1O2O9L15X2AOZ"
                },
                "arn": "arn:aws:s3:::event-fault-monitor-dev-bucket-1jbfm2f87k1la"
            },
            "object": {
                "key": "us-west-2/2023/03/15/18/event-fault",
                "size": 219,
                "eTag": "e14721f86bbda2cc2b0062319bc37d46",
                "versionId": "liUF5LNMXaxTITIsvAtBdll_6RFgxTL2",
                "sequencer": "00634B2A5C1A443184"
            }
        }
    ])
    events['Records'][0]['eventName'] = 'ObjectCreated:Put'

    collected  = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from(rules),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_s3(events),
        on_next = _on_next,
    )

    expect(collected[0]['pipeline']).to(equal('metrics'))
    expect(collected[0]['put_request']).to(equal({
        'Namespace': 'test-namespace',
        'MetricData': [
            {
                'MetricName': 'domain.event',
                'Timestamp': '1671042150',
                'Unit': 'Count',
                'Value': 1,
                'Dimensions': [
                    {
                        'Name': 'account',
                        'Value': 'dev'
                    },
                    {
                        'Name': 'region',
                        'Value': 'us-west-2'
                    },
                    {
                        'Name': 'stage',
                        'Value': 'test'
                    },
                    {
                        'Name': 'source',
                        'Value': 'undefined'
                    },
                    {
                        'Name': 'functionname',
                        'Value': 'f1'
                    },
                    {
                        'Name': 'pipeline',
                        'Value': 'p1'
                    },
                    {
                        'Name': 'type',
                        'Value': 'fault'
                    }
                ]
            },
            {
                'MetricName': 'domain.event.size',
                'Timestamp': '1671042150',
                'Unit': 'Bytes',
                'Value': 284,
                'Dimensions': [
                    {
                        'Name': 'account',
                        'Value': 'dev'
                    },
                    {
                        'Name': 'region',
                        'Value': 'us-west-2'
                    },
                    {
                        'Name': 'stage',
                        'Value': 'test'
                    },
                    {
                        'Name': 'source',
                        'Value': 'undefined'
                    },
                    {
                        'Name': 'functionname',
                        'Value': 'f1'
                    },
                    {
                        'Name': 'pipeline',
                        'Value': 'p1'
                    },
                    {
                        'Name': 'type',
                        'Value': 'fault'
                    }
                ]
            }
        ]
    }))
    