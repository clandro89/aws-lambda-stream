from reactivex import Observable
from pydash import pick
from aws_lambda_stream.filters import out_latched
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.operators import rx_filter, rx_map


def cdc(rule):
    "cdc: change data capture"
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(out_latched),
            rx_filter(on_event_type(rule)),
            rx_filter(on_content(rule)),
            rx_map(_to_event(rule)),
            rule['publish'](pick(rule,[
                'logger',
                'max_batch_size',
                'bus_name'
            ])),
        )
    return wrapper

def _to_event(rule):
    def wrapper(uow):
        # pylint: disable=unnecessary-lambda
        return uow if not 'to_event' in rule else {
            **uow,
            'event': {
                **uow['event'],
                **rule['to_event'](uow)
            }
        }
    return faulty(wrapper)
