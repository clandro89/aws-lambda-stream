#pylint: disable=C0301
import pydash
from pydash import get

def from_s3(event):
    return pydash.map_(
        event['Records'],
        lambda record: {
            'record': record,
            'event':{
                'id': record['s3']['object']['key'],
                'type': _calculate_event_type_prefix(record),
                's3': record['s3'],
            },
        }
    )

def _calculate_event_type_prefix(record):
    if record['eventName'] in ['ObjectCreated:Post','ObjectCreated:Put']:
        return 'object-created'
    if record['eventName'] == 'ObjectCreated:Delete':
        return 'object-deleted'
    return None


# test helper
def to_s3_records(notifications):
    return {
        'Records': [
            {
                # 'eventVersion': '2.1',
                'eventSource': 'aws:s3',
                'awsRegion': 'us-west-2',
                # 'eventTime': '2019-09-03T19:37:27.192Z',
                # 'eventName': 'ObjectCreated:Put',
                # 'userIdentity': {
                #   'principalId': 'AWS:AIDAINPONIXQXHT3IKHL2',
                # },
                # 'requestParameters': {
                #   'sourceIPAddress': '205.255.255.255',
                # },
                'responseElements': {
                    'x-amz-request-id': f"000000000000000{i}",
                #   'x-amz-id-2': 'vlR7PnpV2Ce81l0PRw6jlUpck7Jo5ZsQjryTjKlc5aLWGVHPZLj5NeC6qMa0emYBDXOo6QBU0Wo=',
                },
                's3': {
                # s3SchemaVersion: '1.0',
                # configurationId: '828aa6fc-f7b5-4305-8584-487c791949c1',
                    'bucket': get(n, 'bucket'), # {
                    # name: 'lambda-artifacts-deafc19498e3f2df',
                    # ownerIdentity: {
                    #   principalId: 'A3I5XTEXAMAI3E',
                    # },
                    # arn: 'arn:aws:s3:::lambda-artifacts-deafc19498e3f2df',
                    # },
                    'object': get(n, 'object'), # {
                # key: 'b21b84d653bb07b05b1e6b33684dc11b',
                # size: 1305107,
                # eTag: 'b21b84d653bb07b05b1e6b33684dc11b',
                # sequencer: '0C0F6F405D6ED209E1',
                # },
                },
            }
            for i,n in enumerate(notifications)
        ]
    }
