from typing import Iterable
import boto3


class Connector():
    def __init__(self, table_name = 'undefined') -> None:
        self.table_name = table_name
        self.client = boto3.resource('dynamodb')

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

    def bulk_insert(self, items: Iterable):
        with self.client.Table(self.table_name).batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

    def bulk_delete(self, items: Iterable):
        with self.client.Table(self.table_name).batch_writer() as batch:
            for key in items:
                batch.delete_item(Key=key)
