import os
from reactivex import Observable, operators as ops
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.elasticsearch import update_elasticsearch
from aws_lambda_stream.connectors.opensearch import Connector
from aws_lambda_stream.utils.operators import rx_filter, rx_map
from aws_lambda_stream.utils.print import print_end_pipeline, print_start_pipeline


def elasticsearch(rule):
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(on_event_type(rule)),
            ops.do_action(print_start_pipeline(rule)),
            rx_filter(on_content(rule)),
            rx_map(_to_elasticsearch(rule)),
            update_elasticsearch(
                connector=Connector(
                    rule.get('domain_host') or os.getenv('ES_DOMAIN_HOST')
                )
            ),
            ops.do_action(print_end_pipeline(rule)),
        )
    return wrapper

def _to_elasticsearch(rule):
    def wrapper(uow):
        return {
            **uow,
            "update_request":  {
                **(rule['to_elasticsearch'](uow)),
            }
        }
    return faulty(wrapper)
