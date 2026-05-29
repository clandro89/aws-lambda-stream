from reactivex import Observable, operators as ops
from pydash import get, pick
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.operators import rx_filter, rx_map
from aws_lambda_stream.utils.print import print_end_pipeline, print_start_pipeline


def expired(rule):
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(for_expiration),
            ops.do_action(print_start_pipeline(rule)),
            rx_map(faulty(to_expired_event)),
            rule['publish']({
                **pick(rule,[
                    'logger',
                    'bus_name',
                    'source'
                ]),
                'event_field': 'emit'
            }),
            ops.do_action(print_end_pipeline(rule)),
        )
    return wrapper

def for_expiration(uow):
    if get(uow, 'record.eventName') != 'REMOVE':
        return False
    
    ttl = get(uow, 'event.raw.old.ttl')
    expire = get(uow, 'event.raw.old.expire')
    
    if not ttl or not expire:
        return False
    
    removed = get(uow, 'record.dynamodb.ApproximateCreationDateTime')
    
    if removed < ttl:
        return False
    
    return True


def to_expired_event(uow):
    ttl = get(uow, 'event.raw.old.ttl')
    expire = get(uow, 'event.raw.old.expire')
    event = get(uow, 'event.raw.old.event')
    
    id_ = event['id']
    type_ = event['type']
    timestamp = event['timestamp']
    
    return {
        **uow,
        'emit': {
            **event,
            'id': uow['event']['id'],
            'type': calc_type(type_, expire),
            'timestamp': (ttl * 1000) + (timestamp % 1000),
            'triggers': [
                {
                    'id': id_,
                    'type': type_,
                    'timestamp': timestamp
                }
            ]
        }
    }


def calc_type(type_, expire):
    if isinstance(expire, str):
        return expire
    elif '.' in type_:
        return f"{type_}.expired"
    else:
        return f"{type_}-expired"
