import os
import json
from decimal import Decimal
from typing import Union
from functools import reduce
import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from reactivex import Observable
from aws_lambda_stream.connectors.dynamodb import Connector
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.operators import rx_map
from aws_lambda_stream.utils.json_encoder import JSONEncoder


def serialize_number(number: str) -> Union[float, int]:
    if '.' in number:
        return float(number)
    return int(number)


setattr(TypeDeserializer, '_deserialize_n', lambda _, number: serialize_number(number))

client = None

def get_dynamodb_client():
    global client #pylint: disable=global-statement
    if not client:
        client = boto3.resource('dynamodb')
    return client


def update_expression(item: dict):
    keys = item.keys()
    result = {}
    result['ExpressionAttributeNames'] = reduce(
        lambda accumulator, el: {**accumulator, **el},
        map(
            lambda attrName: {f"#{attrName}": attrName },
            keys
        ),
        {}
    )
    result['ExpressionAttributeValues'] = reduce(
        lambda accumulator, el: {**accumulator, **el},
        map(
            lambda attrName: {f":{attrName}": item[attrName] },
            filter(
                lambda attrName: item[attrName] is not None,
                keys
            )
        ),
        {}
    )
    result['UpdateExpression'] = "SET "+", ".join(map(
            lambda attrName: f"#{attrName} = :{attrName}",
            filter(
                lambda attrName: item[attrName] is not None,
                keys
            )
        ))
    update_expression_remove = ", ".join(map(
            lambda attrName: f"#{attrName}",
            filter(
                lambda attrName: item[attrName] is None,
                keys
            )
        ))
    if update_expression_remove:
        result['UpdateExpression'] = "{} REMOVE {}".format(
            result['UpdateExpression'],
            update_expression_remove
        )
    result['ReturnValues'] = 'ALL_NEW'
    return result

def timestamp_condition(field_name = 'timestamp'):
    return {
        'ConditionExpression': "attribute_not_exists(#{fn}) OR #{fn} < :{fn}".format(
            fn=field_name
        )
    }

def pk_condition(field_name = 'pk'):
    return {
        'ConditionExpression': "attribute_not_exists({})".format(
            field_name
        )
    }

def update_dynamodb(
    table_name=os.getenv('ENTITY_TABLE_NAME') or os.getenv('EVENT_TABLE_NAME'),
    update_request_field='update_request',
):
    connector = Connector(table_name)
    def wrapper(uow):
        return {
            **uow,
            'update_response': connector.update(
                json.loads(
                    json.dumps(
                        uow[update_request_field],
                        cls=JSONEncoder
                    ),
                    parse_float=Decimal
                )
            )
        }
    return faulty(wrapper)


def put_dynamodb(
    table_name= os.getenv('EVENT_TABLE_NAME') or os.getenv('ENTITY_TABLE_NAME'),
    put_request_field= 'put_request'
    ):
    connector = Connector(table_name)
    def wrapper(uow):
        return {
            **uow,
            'put_response': connector.put(
                json.loads(
                    json.dumps(
                        uow[put_request_field],
                        cls=JSONEncoder
                    ),
                    parse_float=Decimal
                )
            )
        }
    return faulty(wrapper)


def query_dynamodb(
        table_name= os.getenv('EVENT_TABLE_NAME') or os.getenv('ENTITY_TABLE_NAME'),
        query_request_field = 'query_request',
        query_response_field = 'query_response'
    ):
    connector = Connector(table_name)

    def invoke(uow):
        if not uow.get(query_request_field):
            return uow
        # pylint: disable=fixme
        # TODO:? use cache of query
        # req = json.dumps(uow.get(query_request_field))
        # query_response_field = cache.get(req)
        # try:
        #     res = connector.query(uow.get(query_request_field)

        return {
            **uow,
            query_response_field: connector.query_all(uow.get(query_request_field))
        }

    def wrapper(source: Observable):
        return source.pipe(
            rx_map(faulty(invoke)),
        )
    return wrapper


def unmarshall(image):
    # To go from low-level format to python
    deserializer = TypeDeserializer()
    return {k: deserializer.deserialize(v) for k,v in image.items()}

def marshall(obj: dict):
    serializer = TypeSerializer()
    return { k: serializer.serialize(v) for k, v in obj.items()}
