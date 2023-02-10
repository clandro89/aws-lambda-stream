from functools import reduce
from reactivex import Observable
from pydash import get, pick, omit, merge
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.dynamodb import query_dynamodb
from aws_lambda_stream.utils.operators import rx_filter, rx_map, split_buffer


def evaluate(rule):
    #pylint: disable=line-too-long
    """
    used to evaluate conditions and produce higher-order events
    used in trigger functions
    *
    {
        'id': str
        'flavor': evaluate,
        'eventType': str | List[str] | Callable, // match rules to events based on event type
        'correlation_key_suffix': Optional[str], // match rules to events based on correlation key suffix
        'filters': Optional[List[Callable]],     // evaluate event content
        'expression': Optional[Callable];       // evaluate correlated events, triggers query for correlated events
        'emit': str | Callable;                 // create higher-order event(s) to publish
        'index': Optional[str];
    }
    """
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(_for_events),
            rx_map(_normalize),
            rx_filter(on_event_type(rule)),
            rx_filter(on_content(rule)),
            _complex(rule),
            rx_map(_to_higher_order_events(rule)),
            split_buffer(),
            rule['publish']({
                    **pick(rule,[
                        'logger',
                        'max_batch_size',
                        'bus_name'
                    ]),
                    'max_batch_size': 1,
                    'event_field': 'emit'
                }),
        )
    return wrapper


def _for_events(uow):
    return get(uow, 'record.eventName') == 'INSERT' and \
        (get(uow, 'record.dynamodb.Keys.sk.S') == 'EVENT'
            or get(uow,'record.dynamodb.NewImage.discriminator.S') == 'CORREL'
            )


def _normalize(uow):
    return {
        **uow,
        'meta': {
            'id': get(uow, 'event.id'),
            'sequence_number': get(uow, 'event.raw.new.sequence_number'),
            'ttl': get(uow, 'event.raw.new.ttl'),
            'expire': get(uow, 'event.raw.new.expire'),
            'pk': get(uow, 'event.raw.new.pk'),
            'data': get(uow, 'event.raw.new.data'),
            'correlation_key': get(uow, 'event.raw.new.pk')
                if get(uow, 'event.raw.new.discriminator') == 'CORREL'
                else get(uow, 'event.raw.new.data'),
            'suffix': get(uow, 'event.raw.new.suffix'),
            'correlation': get(uow, 'event.raw.new.discriminator') == 'CORREL',
        },
        'event': get(uow, 'event.raw.new.event')
    }


def _complex(rule):
    def wrapper(source: Observable):
        if not rule.get('expression'):
            return source.pipe(
                rx_map(lambda uow: {
                    **uow,
                    'triggers': [uow['event']]
                }),
            )
        return source.pipe(
            rx_filter(_on_correlation_key_suffix(rule)),
            rx_map(_to_query_request(rule)),
            query_dynamodb(
                **pick({
                    **rule,
                    'query_response_field': 'correlated'
                }, [
                    'table_name',
                    'query_request_field',
                    'query_response_field'
                ])
            ),
            rx_map(lambda uow: {
                **uow,
                'correlated': [i['event'] for i in uow['correlated']]
            }),
            rx_map(_expression(rule)),
            rx_filter(lambda uow: uow['expression'])
        )
    return wrapper


def _on_correlation_key_suffix(rule):
    def wrapper(uow):
        # evaluate rules with no suffix against correlations with no suffix
        if not rule.get('correlation_key_suffix') and \
            not get(uow, 'meta.suffix'):
            return True

        # do not evaluate rules with a suffix against correlations with no suffix
        if rule.get('correlation_key_suffix') and \
            not get(uow, 'meta.suffix'):
            return False

        # evaluate rules with a suffix against correlations with the same suffix
        if rule.get('correlation_key_suffix') and \
            get(uow, 'meta.suffix') == rule.get('correlation_key_suffix'):
            return True
        # do not evaluate rules with a suffix against correlations with a different suffix
        return False
    return faulty(wrapper)


def _to_query_request(rule):
    def wrapper(uow):
        return {
            **uow,
            'query_request': {
                'KeyConditionExpression': '#pk = :pk',
                'ExpressionAttributeNames': {
                    '#pk': 'pk',
                },
                'ExpressionAttributeValues': {
                    ':pk': get(uow, 'meta.pk'),
                },
                'ConsistentRead': True
            } if get(uow, 'meta.correlation')
            else {
                'IndexName': rule.get('index', 'DataIndex'),
                'KeyConditionExpression': '#data = :data',
                'ExpressionAttributeNames': {
                    '#data': 'data',
                },
                'ExpressionAttributeValues': {
                    ':data': get(uow, 'meta.data'),
                },
            },
        }
    return wrapper


def _expression(rule):
    def wrapper(uow):
        result = rule['expression'](uow)
        return {
            **uow,
            'expression': len(result) > 0 if isinstance(result, list) else result,
            'triggers': [uow['event']] if isinstance(result, bool) else _cast_array(result)
        }
    return faulty(wrapper)


def _cast_array(value):
    if isinstance(value, list):
        return value
    return [value]


def _to_higher_order_events(rule):
    def wrapper(uow):
        basic = isinstance(rule['emit'], str)
        trigger = uow['triggers'][-1]
        template = {
            **(uow['event'] if basic else {} ),
            'id': f"{get(uow,'meta.id')}.{rule['id']}",
            'type': rule['emit'] if basic else None,
            'timestamp': trigger['timestamp'],
            'partition_key': get(uow, 'meta.correlation_key').replace(
                f".{rule.get('correlation_key_suffix')}",
                ""
            ),
            'tags': omit(
                reduce(
                    lambda previous, current: merge(previous, current.get('tags')),
                    uow['triggers'],
                    {}
                ),
                ['region', 'source']
            ),
            'triggers': [
                {
                    'id': i['id'],
                    'type': i['type'],
                    'timestamp': i['timestamp']
                }
                for i in uow['triggers']
            ]
        }
        result =  template if basic else rule['emit'](uow, rule, template)
        return [
            {
                **uow,
                'emit': emit
            }
            for emit in _cast_array(result)
        ]
    return faulty(wrapper)
