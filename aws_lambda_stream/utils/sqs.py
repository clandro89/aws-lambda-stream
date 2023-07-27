import os
from reactivex import Observable, operators as ops
from aws_lambda_powertools import Logger
from aws_lambda_stream.connectors.sqs import Connector
from aws_lambda_stream.utils.operators import split_buffer
from .batch import to_batch_uow, unbatch_uow


def send_to_sqs(
    logger=Logger(),
    queue_url = os.getenv('QUEUE_URL'),
    message_field = 'message',
    batch_size=os.getenv('SQS_BATCH_SIZE') or os.getenv('BATCH_SIZE') or 10
    ):
    connector = Connector(queue_url)

    def to_input_params(batch_uow):
        return {
            **batch_uow,
            'input_params': {
                'Entries': list(map(
                    lambda uow: uow[message_field],
                    filter(
                        lambda uow: message_field in uow,
                        batch_uow['batch']
                    )
                ))
            }
        }

    def send_message_batch(batch_uow):
        if len(batch_uow['input_params']['Entries']) == 0:
            return batch_uow
        logger.info(batch_uow['input_params'])
        return {
            **batch_uow,
            'send_message_batch_response': connector.send_message_batch(
                batch_uow['input_params']
            )
        }

    def wrapper(source: Observable):
        return source.pipe(
            ops.buffer_with_count(batch_size, batch_size),
            ops.map(to_batch_uow),
            ops.map(to_input_params),
            ops.map(send_message_batch),
            ops.map(unbatch_uow),
            split_buffer()
        )
    return wrapper
