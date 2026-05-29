import os
from reactivex import Observable, operators as ops
from pydash import pick
from aws_lambda_stream.utils.dynamodb import (
    batch_get_dynamodb,
    query_dynamodb,
    update_dynamodb,
)
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.operators import rx_filter, rx_map
from aws_lambda_stream.utils.print import print_end_pipeline, print_start_pipeline
from aws_lambda_stream.utils.split import split_object
from aws_lambda_stream.flavors.correlate import _for_collected_events, normalize


def update(rule):
    def wrapper(source: Observable):
        return source.pipe(
            rx_map(lambda uow: normalize(uow) if _for_collected_events(uow) else uow),
            rx_filter(on_event_type(rule)),
            ops.do_action(print_start_pipeline(rule)),
            rx_filter(on_content(rule)),
            rx_map(_to_query_request(rule)),
            query_dynamodb(
                **pick({
                    **rule,
                    'table_name': rule.get(
                        'table_name',
                        os.getenv('ENTITY_TABLE_NAME') or
                        os.getenv('EVENT_TABLE_NAME')
                    )
                }, [
                    'table_name',
                    'query_request_field',
                    'query_response_field'
                ])
            ),
            split_object({
                **rule,
                **({
                    'split_on': rule.get(
                        'split_on',
                        rule.get('query_response_field', 'query_response')
                    ),
                    'split_target_field': rule.get(
                        'split_target_field',
                        rule.get('query_response_field', 'query_response')
                    )
                } if rule.get('split_on') or rule.get('to_query_request') else {})
            }),
            rx_map(_to_get_request(rule)),
            batch_get_dynamodb(
                **pick({
                    **rule,
                    'table_name': rule.get(
                        'table_name',
                        os.getenv('ENTITY_TABLE_NAME') or
                        os.getenv('EVENT_TABLE_NAME')
                    )
                }, [
                    'table_name',
                    'batch_get_request_field',
                    'batch_get_response_field'
                ])
            ),
            rx_map(_to_update_request(rule)),
            rx_map(_to_fallback_update_request(rule)),
            rx_map(
                update_dynamodb(
                    **pick({
                        **rule,
                        'table_name': rule.get(
                            'table_name',
                            os.getenv('ENTITY_TABLE_NAME') or
                            os.getenv('EVENT_TABLE_NAME')
                        )
                    }, [
                        'table_name',
                        'update_request_field',
                        'update_response_field',
                        'fallback_update_request_field'
                    ])
                )
            ),
            ops.do_action(print_end_pipeline(rule)),
        )
    return wrapper


def _to_query_request(rule):
    def wrapper(uow):
        return {
            **uow,
            'query_request': rule['to_query_request'](uow, rule)
                if 'to_query_request' in rule
                else None
        }
    return faulty(wrapper)


def _to_get_request(rule):
    def wrapper(uow):
        return {
            **uow,
            'batch_get_request': rule['to_get_request'](uow, rule)
                if 'to_get_request' in rule
                else None
        }
    return faulty(wrapper)


def _to_update_request(rule):
    def wrapper(uow):
        return {
            **uow,
            'update_request': rule['to_update_request'](uow, rule)
        }
    return faulty(wrapper)


def _to_fallback_update_request(rule):
    def wrapper(uow):
        return {
            **uow,
            'fallback_update_request': rule['to_fallback_update_request'](uow, rule)
                if 'to_fallback_update_request' in rule
                else None
        }
    return faulty(wrapper)
