import os
import json
from pydash import get, pick
from reactivex import Observable, operators as ops
from aws_lambda_stream.connectors.elasticsearch import Connector
from aws_lambda_stream.utils.json_encoder import JSONEncoder
from aws_lambda_stream.utils.operators import split_buffer, rx_map
from aws_lambda_stream.utils.faults import faulty
from .batch import to_batch_uow, unbatch_uow


def update_elasticsearch(
    connector: Connector,
    update_field='update_request',
    batch_size=os.getenv('PUBLISH_BATCH_SIZE') or os.getenv('BATCH_SIZE') or 10,
    max_batch_size=os.getenv('PUBLISH_BATCH_SIZE') or os.getenv('BATCH_SIZE') or 10,
):
    def to_input_params(batch_uow):
        return {
            **batch_uow,
            'input_params': "\n".join(list(map(
                    lambda uow: "{}\n{}".format(
                        json.dumps({
                            'index': {
                                '_index': uow[update_field]['index'],
                                '_id': uow[update_field]['id']
                            }
                        }, cls=JSONEncoder),
                        json.dumps(uow[update_field]['data'], cls=JSONEncoder)
                    ),
                    filter(
                        lambda uow: update_field in uow,
                        batch_uow['batch']
                    )
                )))+'\n'
        }
    def put_items(batch_uow):
        return {
            **batch_uow,
            'elasticsearch_response': connector.bulk(batch_uow['input_params'])
        }
    def wrapper(source: Observable):
        return source.pipe(
            ops.buffer_with_count(max_batch_size if batch_size > max_batch_size else batch_size),
            rx_map(to_batch_uow),
            rx_map(to_input_params),
            rx_map(faulty(put_items)),
            rx_map(unbatch_uow),
            split_buffer()
        )
    return wrapper


def query_elasticsearch(
    host = os.getenv('ES_DOMAIN_HOST'),
    region = os.getenv('REGION'),
    query_request_field = 'query_request',
    query_response_field = 'query_response'
    ):

    connector = Connector(host, region)

    def search(uow):
        if not uow.get(query_request_field):
            return uow

        return {
            **uow,
            query_response_field: connector.search(
                **pick(
                    get(uow, query_request_field),
                    'index', 'payload' ,'query'
                )
            )
        }

    def wrapper(source: Observable):
        return source.pipe(
            rx_map(faulty(search)),
        )
    return wrapper
