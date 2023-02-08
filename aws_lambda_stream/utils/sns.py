from uuid import uuid1
from reactivex import Observable, operators as ops
from pydash import map_
from aws_lambda_stream.connectors.sns import Connector

def publish_to_sns(
    connector: Connector,
    publish_message_field='sns_payload'
):
    def to_input_params(uow):
        return {
            **uow,
            'input_params': {
                'PublishBatchRequestEntries': map_(
                    uow[publish_message_field],
                    lambda item: {
                        'Id': str(uuid1()),
                        **item
                    }
                )
            }
        }
    def publish_batch(uow):
        uow['publish_response'] = connector.publish_batch(uow['input_params'])
        return uow
    def _wrapper(source: Observable):
        return source.pipe(
            ops.map(to_input_params),
            ops.map(publish_batch)
        )
    return _wrapper
