import base64
import json
import pydash
from aws_lambda_stream.utils.json_encoder import JSONEncoder


def from_kinesis(event):
    return pydash._(
        event['Records']
    ).map(
        lambda record: {
            'record': record,
            'event': base64.b64decode(record["kinesis"]["data"])
        }
    ).map(
        lambda uow: {
            **uow,
            'event': {
                'id': uow['record']['eventID'],
                **json.loads(uow['event'])
            },
        }
    ).value()

# test helper
def to_kinesis_records(events):
    return {
        'Records': [
            {
                'eventSource': 'aws:kinesis',
                'eventID': f"shardId-000000000000:{i}",
                'awsRegion': 'us-west-2',
                'kinesis': {
                    'sequenceNumber': f"{i}",
                    'data': base64.b64encode(json.dumps(e, cls=JSONEncoder).encode('utf-8'))
                }
            }
            for i,e in enumerate(events)
        ]
    }
