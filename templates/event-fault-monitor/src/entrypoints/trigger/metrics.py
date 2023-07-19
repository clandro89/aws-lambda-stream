import os
import json
from reactivex import Observable
from pydash import get
from aws_lambda_stream import rx_map, split_buffer
from aws_lambda_stream.utils.s3 import (
    to_get_object_request,
    get_object_from_s3
)
from aws_lambda_stream.utils.cloudwatch import put_metrics


def metrics(_):
    def wrapper(source: Observable):
        return source.pipe(
            rx_map(to_get_object_request),
            get_object_from_s3(),
            rx_map(
                lambda uow: list(map(
                    lambda i: {
                        **uow,
                        'get_response': {
                            'line': i.decode('utf-8')
                        }
                    },
                    uow['get_response'].splitlines()
                ))
            ),
            split_buffer(),
            rx_map(_to_event),
            rx_map(_to_put_request),
            put_metrics(),
        )
    return wrapper


def _to_event(uow):
    detail = json.loads(uow['get_response']['line'])['detail']
    return {
        **uow,
        'event': detail
    }

def _to_put_request(uow):
    timestamp = str(uow['event']['timestamp'])[:10]
    dimensions = [
        {
        'Name': 'account',
        'Value': (get(uow, 'event.tags') and get(uow, 'event.tags.account'))
                or os.getenv('ACCOUNT_NAME') or 'not-specified',
        },
        {
        'Name': 'region',
        'Value': get(uow, 'record.awsRegion')
                or (get(uow, 'event.tags') and get(uow, 'event.tags.region'))
                or os.getenv('AWS_REGION'),
        },
        {
        'Name': 'stage',
        'Value': (get(uow, 'event.tags') and get(uow, 'event.tags.stage') ) or 'not-specified',
        },
        {
        'Name': 'source',
        'Value': (get(uow, 'event.tags') and get(uow, 'event.tags.source') ) or 'not-specified',
        },
        {
        'Name': 'functionname',
        'Value': (get(uow, 'event.tags') and get(uow, 'event.tags.functionname') )
                or 'not-specified',
        },
        {
        'Name': 'pipeline',
        'Value': (get(uow, 'event.tags') and get(uow, 'event.tags.pipeline') ) or 'not-specified',
        },
        {
        'Name': 'type',
        'Value': get(uow, 'event.type'),
        },
    ]

    return {
        **uow,
        'put_request': {
            'Namespace': os.getenv('NAMESPACE'),
            'MetricData': [
                {
                    'MetricName': 'domain.event',
                    'Timestamp': timestamp,
                    'Unit': 'Count',
                    'Value': 1,
                    'Dimensions': dimensions
                },
                {
                    'MetricName': 'domain.event.size',
                    'Timestamp': timestamp,
                    'Unit': 'Bytes',
                    'Value': len(json.dumps(uow['event'])),
                    'Dimensions': dimensions
                }
            ]
        }
    }
