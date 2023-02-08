import os
from reactivex import Observable
from pydash import get, omit
from aws_lambda_stream.utils.dynamodb import put_dynamodb
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.time import ttl_rule
from aws_lambda_stream.utils.operators import rx_filter, rx_map


def collect(rule):
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(on_event_type(rule)),
            rx_filter(on_content(rule)),
            rx_map(_correlation_key(rule)),
            rx_map(_to_put_request(rule)),
            rx_map(
                put_dynamodb(
                    table_name=rule.get('table_name',
                        os.getenv('ENTITY_TABLE_NAME') or
                                os.getenv('EVENT_TABLE_NAME')
                    )
                )
            )
        )
    return wrapper


def _correlation_key(rule):
    def wrapper(uow):
        if not 'correlation_key' in rule:
            key = uow['event']['partition_key']
        elif callable(rule['correlation_key']):
            key = rule['correlation_key'](uow)
        else:
            key = get(uow['event'], rule['correlation_key'])
        return {
            **uow,
            'key': key
        }
    return faulty(wrapper)

def _get_sequence_number(uow):
    seq = get(uow, 'record.kinesis.sequenceNumber')
    if seq:
        return seq
    seq = get(uow, 'record.attributes.SequenceNumber')
    return seq


def _to_put_request(rule):
    def wrapper(uow):
        return {
            **uow,
            "put_request": rule['to_put_request'](uow, rule)
                if 'to_put_request' in rule
                else {
                    'Item': {
                        'pk': uow['event']['id'],
                        'sk': 'EVENT',
                        'discriminator': 'EVENT',
                        'timestamp': uow['event']['timestamp'],
                        'awsregion': os.getenv('REGION'),
                        'sequence_number': _get_sequence_number(uow),
                        'ttl': ttl_rule(rule, uow),
                        'expire': rule.get('expire'),
                        'data': uow['key'],
                        'event': uow['event'] if
                                rule.get('include_raw') else
                                omit(uow['event'], ['raw'])
                    }
                }
        }
    return wrapper
