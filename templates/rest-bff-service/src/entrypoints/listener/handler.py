from aws_lambda_powertools import Logger
from aws_lambda_stream import (StreamPipeline, initialize_from, from_kinesis)
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from .rules import rules


logger = Logger()

@logger.inject_lambda_context(log_event=True)
def handler(event, _):
    # pylint: disable=duplicate-code
    StreamPipeline(
        initialize_from(rules),
        DEFAULT_OPTIONS,
    ).assemble(
        from_kinesis(event),
    )
