import os
import json
from decimal import Decimal
import pydash
from aws_lambda_stream.utils.dynamodb import unmarshall,marshall


def from_dynamodb(
    event,
    pk_fn = 'pk',
    sk_fn = 'sk',
    discriminator_fn = 'discriminator',
    event_type_prefix = None):
    return  pydash._(
                event['Records']
            ).filter(
                _out_replicas
            ).filter(
                _out_global_table_extra_modify
            ).map(
                lambda record: {
                            'record': record,
                            'event': {
                                'id': record['eventID'],
                                'type': '%s-%s' % (
                                    _calculate_event_type_prefix(record, {
                                            'sk_fn': sk_fn,
                                            'discriminator_fn': discriminator_fn,
                                            'event_type_prefix': event_type_prefix
                                        }),
                                    _calculate_event_type_suffix(record)
                                ),
                                'partition_key': record['dynamodb']['Keys'][pk_fn]['S'],
                                'timestamp': record['dynamodb']['ApproximateCreationDateTime']*1000,
                                'tags':{
                                    'region': record['awsRegion']
                                },
                                'raw': {
                                    'new': (unmarshall(record.get('dynamodb').get('NewImage'))
                                            if record.get('dynamodb').get('NewImage') else None),
                                    'old': (unmarshall(record.get('dynamodb').get('OldImage'))
                                            if record.get('dynamodb').get('OldImage') else None),
                                }
                            }
                }
            ).map(
                lambda uow: {
                    **uow,
                    'event': {
                        **uow['event'],
                        'timestamp': _get_timestamp(uow)
                    },
                }
            ).value()

def _calculate_event_type_prefix(record, opt):
    if opt.get('event_type_prefix'):
        return opt['event_type_prefix']
    image = record.get('dynamodb').get('NewImage') or record.get('dynamodb').get('OldImage')
    discriminator = image.get(opt['discriminator_fn']) or image.get(opt['sk_fn'])
    return discriminator['S']

def _calculate_event_type_suffix(record):
    suffix = ({
        'INSERT': 'created',
        'MODIFY': 'updated',
        'REMOVE': 'deleted'
    })[record['eventName']]
    if suffix != 'deleted':
        new_image = record.get('dynamodb').get('NewImage')
        old_image = record.get('dynamodb').get('OldImage')
        if ((new_image and new_image.get('deleted')) or
            (old_image and old_image.get('deleted'))):
            if new_image and new_image.get('deleted'):
                return 'deleted'
            if old_image and old_image.get('deleted'):
                return 'undeleted'
    return suffix


def _get_timestamp(uow):
    _new = uow['event']['raw']['new']
    if 'timestamp' in _new:
        return _new['timestamp']
    return int(uow['event']['timestamp'])

def _out_replicas(record):
    image = pydash.get(record, 'dynamodb.NewImage') or \
        pydash.get(record, 'dynamodb.OldImage')
    if 'awsregion' in image:
        return image['awsregion']['S'] == os.getenv('REGION')
    return True


def _out_global_table_extra_modify(record):
    new_image = pydash.get(record, 'dynamodb.NewImage')
    old_image = pydash.get(record, 'dynamodb.OldImage')

    if new_image and pydash.get(new_image, 'awsregion') and \
        old_image and not pydash.get(old_image, 'awsregion'):
        return False

    return True


# test helper
def to_dynamodb_records(events):
    return {
        'Records': [
            {
                'eventID': f"{i}",
                'eventName': ('INSERT' if not e.get('oldImage') else (
                    'REMOVE' if not e.get('newImage') else 'MODIFY'
                )),
                'eventSource': 'aws:dynamodb',
                'awsRegion': 'us-west-2',
                'dynamodb': {
                    'ApproximateCreationDateTime': e.get('timestamp'),
                    'Keys': marshall(e['keys']) if e.get('keys') else None,
                    'NewImage': marshall(json.loads(json.dumps(e['newImage']),
                                           parse_float=Decimal)) if e.get('newImage') else None,
                    'OldImage': marshall(json.loads(json.dumps(e['oldImage']),
                                           parse_float=Decimal)) if e.get('oldImage') else None,
                    'SequenceNumber': f"{i}",
                    'StreamViewType': 'NEW_AND_OLD_IMAGES',
                },
            }
            for i,e in enumerate(events)
        ]
    }
