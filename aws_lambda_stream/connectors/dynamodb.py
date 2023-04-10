from functools import reduce
from typing import Iterable
import boto3
from pydash import reduce_right
from aws_lambda_stream.utils.retry import (
    assert_max_retries,
    DEFAULT_RETRY_CONFIG,
    wait,
    get_delay
)


class Connector():
    def __init__(self,# pylint: disable=W0102
                 table_name = 'undefined',
                 retry_config = DEFAULT_RETRY_CONFIG) -> None:
        self.table_name = table_name
        self.client = boto3.resource('dynamodb')
        self.retry_config = retry_config

    def get(self, input_params):
        return  self.client.Table(self.table_name).get_item(
            **input_params
        )

    def update(self, input_params):
        return  self.client.Table(self.table_name).update_item(
            **input_params
        )

    def put(self, input_params):
        return  self.client.Table(self.table_name).put_item(
            **input_params
        )

    def query(self, input_params):
        return  self.client.Table(self.table_name).query(
            **input_params
        )

    def query_all(self, input_params):
        items = []
        while True:

            result = self.client.Table(self.table_name).query(
                **input_params
            )
            for item in result['Items']:
                items.append(item)

            if 'LastEvaluatedKey' in result and result['LastEvaluatedKey']:
                input_params['ExclusiveStartKey'] = result['LastEvaluatedKey']
            else:
                break
        return items

    def batch_get(self, input_params):
        return self._batch_get(input_params, [])

    def _batch_get(self, params, attempts):
        assert_max_retries(attempts, self.retry_config['max_retries'])
        wait(get_delay(self.retry_config['retry_wait'], len(attempts)))
        resp = self.client.batch_get_item(**params)
        if 'UnprocessedKeys' in resp and resp['UnprocessedKeys']:
            return self._batch_get(
                unprocessed(params, resp),
                [*attempts, resp]
            )
        return accumulate(attempts, resp)

    def bulk_insert(self, items: Iterable):
        with self.client.Table(self.table_name).batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

    def bulk_delete(self, items: Iterable):
        with self.client.Table(self.table_name).batch_writer() as batch:
            for key in items:
                batch.delete_item(Key=key)


def unprocessed(params, resp):
    return {
        **params,
        'RequestItems': resp['UnprocessedKeys']
    }

def accumulate(attempts, resp):
    return reduce_right(
        attempts,
        lambda a,c: {
            **a,
            'Responses': reduce(
                lambda a2,c2: {
                    **a2,
                    c2: [
                        *a2[c2],
                        *a['Responses'][c2]
                    ]
                },
                list(a['Responses']),
                {
                    **c['Responses']
                }
            ),
            'attempts': [
                *attempts,
                resp
            ]
        },
        resp
    )
