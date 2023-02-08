import os
from reactivex import Observable
from aws_lambda_stream.filters import out_source_is_self
from aws_lambda_stream.utils.dynamodb import update_dynamodb
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.operators import rx_filter, rx_map
from aws_lambda_stream.utils.split import split_object


def materialize(rule):
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(out_source_is_self),
            rx_filter(on_event_type(rule)),
            rx_filter(on_content(rule)),
            split_object(rule),
            rx_map(_to_update_request(rule)),
            rx_map(
                update_dynamodb(
                    table_name=os.getenv('ENTITY_TABLE_NAME') or
                                os.getenv('EVENT_TABLE_NAME')
                )
            )
        )
    return wrapper

def _to_update_request(rule):
    def wrapper(uow):
        return {
            **uow,
            "update_request": faulty(rule['to_update_request'])(uow)
        }
    return wrapper
