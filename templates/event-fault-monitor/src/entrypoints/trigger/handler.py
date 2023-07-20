from aws_lambda_powertools import Logger
from aws_lambda_stream import (StreamPipeline,
                               initialize_from)
from aws_lambda_stream.events.s3 import from_s3
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from .rules import rules
# pylint: disable=duplicate-code
logger = Logger()

@logger.inject_lambda_context(log_event=True)
def handler(event, _):
    StreamPipeline(
        initialize_from(rules),
        DEFAULT_OPTIONS
    ).assemble(
        from_s3(event)
    )
