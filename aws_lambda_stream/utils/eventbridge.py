import os
import json
from reactivex import Observable, operators as ops, from_list
from aws_lambda_stream.connectors.eventbridge import Connector
from aws_lambda_stream.utils.concurrency import OPTIMAL_THREAD_COUNT
from aws_lambda_stream.utils.json_encoder import JSONEncoder
from .tags import adorn_standard_tags
from .batch import to_batch_uow, unbatch_uow


# pylint: disable=unused-argument,too-many-arguments
def publish_to_event_bridge(
    logger=None,
    bus_name=os.getenv('BUS_NAME') or 'undefined',
    source='custom',
    event_field='event',
    batch_size=os.getenv('PUBLISH_BATCH_SIZE') or os.getenv('BATCH_SIZE') or 10,
    max_batch_size=os.getenv('PUBLISH_BATCH_SIZE') or os.getenv('BATCH_SIZE') or 10,
    parallel=os.getenv('PUBLISH_PARALLEL') or os.getenv('PARALLEL') or OPTIMAL_THREAD_COUNT,
    handle_errors=True
):
    connector = Connector()
    def to_input_params(batch_uow):
        return {
            **batch_uow,
            'input_params': {
                'Entries': list(map(
                    lambda uow: {
                        'EventBusName': bus_name(uow) if callable(bus_name) else bus_name,
                        'Source': source,
                        'DetailType': uow[event_field]['type'],
                        'Detail': json.dumps(uow[event_field], cls=JSONEncoder),
                    },
                    filter(
                        lambda uow: event_field in uow,
                        batch_uow['batch']
                    )
                ))
            }
        }
    def put_events(batch_uow):
        if len(batch_uow['input_params']['Entries']) == 0:
            return batch_uow
        return {
            **batch_uow,
            'publish_response': connector.put_events(batch_uow['input_params'])
        }
    def wrapper(source: Observable):
        return source.pipe(
            ops.map(adorn_standard_tags(event_field)),
            ops.buffer_with_count(max_batch_size if batch_size > max_batch_size else batch_size),
            ops.map(to_batch_uow),
            ops.map(to_input_params),
            ops.map(put_events),
            ops.map(unbatch_uow),
            ops.flat_map(from_list)
        )
    return wrapper
