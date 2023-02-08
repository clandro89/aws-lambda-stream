import os
from reactivex import Observable
from aws_lambda_stream.utils.s3 import put_object_to_s3
from aws_lambda_stream.connectors.s3 import Connector
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.operators import rx_filter, rx_map
from aws_lambda_stream.utils.split import split_object


def s3(rule):
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(on_event_type(rule)),
            rx_filter(on_content(rule)),
            split_object(rule),
            rx_map(_to_s3(rule)),
            put_object_to_s3(
                connector=Connector(
                    rule.get('bucket_name') or os.getenv('BUCKET_NAME')
                )
            )
        )
    return wrapper

def _to_s3(rule):
    def wrapper(uow):
        return {
            **uow,
            'put_request': faulty(rule['to_s3'])(uow)
        }
    return faulty(wrapper)
