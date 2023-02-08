from uuid import uuid4
from datetime import datetime
from aws_lambda_stream import (now, ttl, update_expression, timestamp_condition)
from aws_lambda_stream.connectors.dynamodb import Connector


class BaseEntityRepository():
    discriminator = None
    def __init__(
        self,
        connector: Connector,
        username='system',
    ) -> None:
        self.connector = connector
        self.username = username

    def save(self, input_params: dict):
        timestamp = now()
        params = {
            'Key': {
                'pk': input_params.get('id', str(uuid4())),
                'sk': input_params.get('sk', self.discriminator),
            },
            **update_expression(
                {
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'discriminator': self.discriminator,
                    'last_modified_by': self.username,
                    'deleted': None,
                    'latched': None,
                    'ttl': ttl(timestamp, 33),
                    **{k: v for k, v in input_params.items() if k not in ['pk', 'sk']},
                    'timestamp': timestamp,
                },
            ),
        }
        self.connector.update(params)

    def get(self, input_params):
        return self.connector.get(input_params)
    def delete(self, pk, sk = None, data = None):
        timestamp = now()
        params = {
            'Key': {
                'pk': pk,
                'sk': sk if sk else self.discriminator,
            },
            **update_expression(
                {
                    **(data if data else {}),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'discriminator': self.discriminator,
                    'last_modified_by': self.username,
                    'deleted': True,
                    'latched': False,
                    'ttl': ttl(timestamp, 33),
                    'timestamp': timestamp,
                },
            ),
        }
        self.connector.update(params)
    def query(self, input_params):
        return self.connector.query(input_params)
    def get_item(self, params):
        result = self.query(params)
        if result['Count'] == 0:
            return None
        return result['Items'][0]
    @classmethod
    def to_update_request(cls):
        discriminator = cls.discriminator
        def wrapper(uow: dict):
            entity_name = uow['event']['type'].split('-')[0]
            entity = uow['event'][entity_name]
            return {
                'Key': {
                    'pk': uow['event'][entity_name]['id'],
                    'sk': entity.get('sk', discriminator),
                },
                **update_expression({
                    **{k: v for k, v in entity.items() if k not in ['pk', 'sk']},
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'discriminator': discriminator,
                    'last_modified_by': 'system',
                    'timestamp': uow['event']['timestamp'],
                    'deleted': True if uow['event']['type'] == f'{entity_name}-deleted' else None,
                    'latched': True,
                    'ttl': ttl(uow['event']['timestamp'], 33),
                }),
                **timestamp_condition(),
            }
        return wrapper
