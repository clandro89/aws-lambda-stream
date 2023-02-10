import os
import json
from reactivex import Observable, operators as ops
from aws_lambda_stream.connectors.elasticsearch import Connector
from aws_lambda_stream.utils.json_encoder import JSONEncoder
from aws_lambda_stream.utils.operators import split_buffer
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
            ops.map(to_batch_uow),
            ops.map(to_input_params),
            ops.map(put_items),
            ops.map(unbatch_uow),
            split_buffer()
        )
    return wrapper
