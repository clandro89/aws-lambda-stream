# pylint: disable=C0301
import json
import pydash
from pydash import get


def from_sqs(event):
    return pydash.map_(
        event['Records'],
        lambda record: {
            'record':record,
            'event':{
                'id': record['messageId'],
                **json.loads(record['body'])
            }
        }
    )

# test helper
def to_sqs_records(messages):
    return {
        'Records': [
            {
                'eventSource': 'aws:sqs',
                'messageId': f"00000000-0000-0000-0000-00000000000{i}",
                # 'receiptHandle': 'AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...',
                'body': get(m, 'body'),
                'attributes': {
                    # 'ApproximateReceiveCount': '1',
                    'SentTimestamp': get(m, 'timestamp') or '1545082649183',
                    # 'SenderId': 'AIDAIENQZJOLO23YVJ4VO',
                    # 'ApproximateFirstReceiveTimestamp': '1545082649185'

                    # 'SequenceNumber': '18849496460467696128',
                    # 'MessageGroupId': '1',
                    # 'MessageDeduplicationId': '1',

                },
                # 'messageAttributes': {},
                # 'md5OfBody': 'e4e68fb7bd0e697a0ae8f1bb342846b3',
                'awsRegion': get(m, 'region') or 'us-west-2',
                # 'eventSourceARN': 'arn:aws:sqs:us-west-2:123456789012:my-queue',
            }
            for i,m in enumerate(messages)
        ]
    }
