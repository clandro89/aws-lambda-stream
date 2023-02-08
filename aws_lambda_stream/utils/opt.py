import os
from aws_lambda_powertools import Logger
from .eventbridge import publish_to_event_bridge

logger = Logger()


DEFAULT_OPTIONS = {
    'logger': logger,
    'bus_name': os.getenv('BUS_NAME'),
    'publish': lambda params: publish_to_event_bridge(**params)
}
