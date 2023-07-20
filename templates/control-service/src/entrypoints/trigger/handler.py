from aws_lambda_powertools import Logger
from aws_lambda_stream import (
    StreamPipeline,
    initialize_from,
    from_dynamodb,
)
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from .evaluate_rules import rules as evaluate_rules
from .correlate_rules import rules as correlated_rules


# pylint: disable=duplicate-code
logger = Logger()

def handler(event, _):
    logger.info(event)
    StreamPipeline(
        initialize_from(
            correlated_rules+evaluate_rules
        ),
        DEFAULT_OPTIONS,
    ).assemble(
        from_dynamodb(event),
    )
