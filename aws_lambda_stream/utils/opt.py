import os
import logging
from .eventbridge import publish_to_event_bridge

logger = logging.getLogger()


DEFAULT_OPTIONS = {
    'logger': logger,
    'bus_name': os.getenv('BUS_NAME'),
    'publish': lambda params: publish_to_event_bridge(**params)
}
