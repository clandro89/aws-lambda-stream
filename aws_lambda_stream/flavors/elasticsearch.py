import os
from reactivex import Observable
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.elasticsearch import update_elasticsearch
from aws_lambda_stream.connectors.elasticsearch import Connector
from aws_lambda_stream.utils.operators import rx_filter, rx_map


def elasticsearch(rule):
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(on_event_type(rule)),
            rx_filter(on_content(rule)),
            rx_map(_to_elasticsearch(rule)),
            update_elasticsearch(
                connector=Connector(
                    rule.get('domain_host') or os.getenv('ES_DOMAIN_HOST')
                ),
                max_batch_size=rule['max_batch_size'],
            )
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
