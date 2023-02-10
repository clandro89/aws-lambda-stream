from uuid import uuid1
from reactivex import Observable
from pydash import pick, get, set_
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.operators import rx_filter, rx_map, split_buffer
from aws_lambda_stream.utils.time import now



def task(rule):
    #pylint: disable=line-too-long
    """
    used to execute task and optionally emit result
    {
        'id': str
        'flavor': task,
        'event_type': str | List[str] | Callable,
        'execute': Callable # execute task
        'emit': Optional[str | Callable]
    }
    """
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(on_event_type(rule)),
            rx_filter(on_content(rule)),
            rx_map(_execute(rule)),
            _to_event(rule),
        )
    return wrapper

def _execute(rule):
    def wrapper(uow):
        result_key = rule.get('result_key', 'result')
        return set_(
            uow,
            result_key,
            rule['execute'](uow, rule)
        )
    return faulty(wrapper)


def _to_event(rule):
    def wrapper(source: Observable):
        if get(rule, 'emit'):
            return source.pipe(
                rx_map(_to_emit(rule)),
                split_buffer(),
                rule['publish']({
                        **pick(rule,[
                            'logger',
                            'bus_name'
                        ]),
                        'max_batch_size': 1,
                        'event_field': 'emit'
                }),
            )
        return source.pipe()
    return wrapper


def _cast_array(value):
    if isinstance(value, list):
        return value
    return [value]


def _to_emit(rule):
    def wrapper(uow):
        basic = isinstance(rule['emit'], str)
        template = {
            'id': str(uuid1()),
            'type': rule['emit'] if basic else None,
            'timestamp': now(),
            'partition_key': uow['event']['partition_key'],
        }
        result = template if basic else rule['emit'](uow, rule, template)
        return [
            {
                **uow,
                'emit': emit
            }
            for emit in _cast_array(result)
        ]
    return faulty(wrapper)
