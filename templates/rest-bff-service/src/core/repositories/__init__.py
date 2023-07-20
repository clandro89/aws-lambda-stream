import os
from aws_lambda_stream.connectors.dynamodb import Connector
from .thing_repository import ThingRepository

def get_thing_repository():
    return ThingRepository(
        Connector(os.getenv('ENTITY_TABLE_NAME'))
    )
