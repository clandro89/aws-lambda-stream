import os
from reactivex import Observable, operators as ops
from aws_lambda_stream.utils.sns import publish_to_sns
from aws_lambda_stream.connectors.sns import Connector
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.operators import rx_filter, rx_map
from aws_lambda_stream.utils.print import print_end_pipeline, print_start_pipeline


def sns(rule):
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(on_event_type(rule)),
            ops.do_action(print_start_pipeline(rule)),
            rx_filter(on_content(rule)),
            rx_map(_to_sns(rule)),
            publish_to_sns(
                connector=Connector(
                    rule.get('topic_arn') or os.getenv('TOPIC_ARN')
                )
            ),
            ops.do_action(print_end_pipeline(rule)),
        )
    return wrapper

def _to_sns(rule):
    def wrapper(uow):
        return {
            **uow,
            'sns_payload':  rule['to_sns'](uow)
        }
    return faulty(wrapper)
