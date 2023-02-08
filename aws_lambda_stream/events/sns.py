#pylint: disable=C0301
import json
import pydash
from pydash import get


def from_sns(event):
    return pydash.map_(
        event['Records'],
        lambda record: {
            'record': record,
            'event': {
                'id': record['Sns']['MessageId'],
                **json.loads(record['Sns']['Message'])
            }
        }
    )

# test helper
def to_sns_records(messages):
    return {
        'Records': [
            {
                # 'EventVersion': '1.0',
                # 'EventSubscriptionArn': 'arn:aws:sns:us-east-2:123456789012:sns-lambda:21be56ed-a058-49f5-8c98-aedd2564c486',
                'EventSource': 'aws:sns',
                'Sns': {
                    # 'SignatureVersion': '1',
                    # 'Timestamp': '2019-01-02T12:45:07.000Z',
                    # 'Signature': 'tcc6faL2yUC6dgZdmrwh1Y4cGa/ebXEkAi6RibDsvpi+tE/1+82j...65r==',
                    # 'SigningCertUrl': 'https://sns.us-east-2.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem',

                    'MessageId': f"00000000-0000-0000-0000-00000000000{i}",
                    'Message':  get(m, 'msg'),

                    'MessageAttributes': get(m, 'attributes') or {
                        # 'Test': {
                        #   'Type': 'String',
                        #   'Value': 'TestString',
                        # },
                        # 'TestBinary': {
                        #   'Type': 'Binary',
                        #   'Value': 'TestBinary',
                        # },
                    },
                    # 'Type': 'Notification',
                    # 'UnsubscribeUrl': 'https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&amp;SubscriptionArn=arn:aws:sns:us-east-2:123456789012:test-lambda:21be56ed-a058-49f5-8c98-aedd2564c486',
                    # 'TopicArn': 'arn:aws:sns:us-east-2:123456789012:sns-lambda',
                    'Subject': get(m, 'subject') or 'TestSubject',
                }
            }
            for i,m in enumerate(messages)
        ]
    }
