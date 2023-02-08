import os
from reactivex import Observable
from aws_lambda_stream.utils.sns import publish_to_sns
from aws_lambda_stream.connectors.sns import Connector
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.operators import rx_filter, rx_map


def sns(rule):
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(on_event_type(rule)),
            rx_filter(on_content(rule)),
            rx_map(_to_sns(rule)),
            publish_to_sns(
                connector=Connector(
                    rule.get('topic_arn') or os.getenv('TOPIC_ARN')
                )
            )
        )
    return wrapper

def _to_sns(rule):
    def wrapper(uow):
        return {
            **uow,
            'sns_payload':  rule['to_sns'](uow)
        }
    return faulty(wrapper)
