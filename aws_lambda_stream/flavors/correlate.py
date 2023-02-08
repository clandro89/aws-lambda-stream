import os
from reactivex import Observable
from pydash import get
from aws_lambda_stream.utils.dynamodb import put_dynamodb
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.time import ttl_rule
from aws_lambda_stream.utils.operators import rx_filter, rx_map


def correlate(rule):
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(_for_collected_events),
            rx_map(_normalize),
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

def _for_collected_events(uow):
    return get(uow, 'record.eventName') == 'INSERT' and \
        get(uow, 'record.dynamodb.Keys.sk.S') == 'EVENT'

def _normalize(uow):
    return {
        **uow,
        'meta': {
            'sequence_number': uow['event']['raw']['new']['sequence_number'],
            'ttl': uow['event']['raw']['new']['ttl'],
            'data': uow['event']['raw']['new']['data']
        },
        'event': get(uow, 'event.raw.new.event')
    }


def _correlation_key(rule):
    def wrapper(uow):
        if callable(rule['correlation_key']):
            key = rule['correlation_key'](uow)
        else:
            key = get(uow['event'], rule['correlation_key'])
        # use a suffix when you need the same key for different sets of rules
        key = f"{key}.{rule['correlation_key_suffix']}" \
                if 'correlation_key_suffix' in  rule \
                else key

        return {
            **uow,
            'key': key
        }
    return faulty(wrapper)


def _to_put_request(rule):
    def wrapper(uow):
        return {
            **uow,
            "put_request": {
                'Item': {
                    'pk': uow['key'],
                    'sk': uow['event']['id'],
                    'discriminator': 'CORREL',
                    'timestamp': uow['event']['timestamp'],
                    'awsregion': os.getenv('REGION'),
                    'sequence_number': uow['meta']['sequence_number'],
                    'ttl': ttl_rule(rule, uow) if rule.get('ttl') else uow['meta']['ttl'],
                    'expire': rule.get('expire'),
                    'suffix': rule.get('correlation_key_suffix'),
                    'rule_id': rule.get('id'),
                    'event': uow['event']
                }
            }
        }
    return wrapper
