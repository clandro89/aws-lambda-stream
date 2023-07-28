import os
import json
from reactivex import Observable, operators as ops
from aws_lambda_powertools import Logger
from aws_lambda_stream.connectors.eventbridge import Connector
from aws_lambda_stream.utils.json_encoder import JSONEncoder
from aws_lambda_stream.utils.operators import split_buffer
from .operators import rx_map
from .tags import adorn_standard_tags
from .batch import to_batch_uow, unbatch_uow

# pylint: disable=unused-argument,too-many-arguments
def publish_to_event_bridge(
    logger=Logger(),
    bus_name=os.getenv('BUS_NAME') or 'undefined',
    source=os.getenv('BUS_SRC') or 'custom',
    event_field='event',
    publish_request_entry_field='publish_request_entry',
    publish_request_field='publish_request',
    batch_size=os.getenv('PUBLISH_BATCH_SIZE') or os.getenv('BATCH_SIZE') or 10,
    ):
    connector = Connector()

    def to_publish_request_entry(uow):
        return {
            **uow,
            publish_request_entry_field: {
                'EventBusName': bus_name(uow) if callable(bus_name) else bus_name,
                'Source': source,
                'DetailType': uow[event_field]['type'],
                'Detail': json.dumps(uow[event_field], cls=JSONEncoder),
            } if uow.get(event_field) else None
        }

    def to_publish_request(batch_uow):
        return {
            **batch_uow,
            publish_request_field: {
                'Entries': list(map(
                    lambda uow: uow[publish_request_entry_field],
                    filter(
                        lambda uow: uow[publish_request_entry_field],
                        batch_uow['batch']
                    )
                ))
            }
        }

    def put_events(batch_uow):
        if len(batch_uow[publish_request_field]['Entries']) == 0:
            return batch_uow
        logger.info(batch_uow[publish_request_field])
        return {
            **batch_uow,
            'publish_response': connector.put_events(batch_uow[publish_request_field])
        }

    def wrapper(source: Observable):
        return source.pipe(
            rx_map(adorn_standard_tags(event_field)),
            rx_map(to_publish_request_entry),
            ops.buffer_with_count(batch_size, batch_size),
            rx_map(to_batch_uow),
            rx_map(to_publish_request),
            rx_map(put_events),
            rx_map(unbatch_uow),
            split_buffer()
        )
    return wrapper
