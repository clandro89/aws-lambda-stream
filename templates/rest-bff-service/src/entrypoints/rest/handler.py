from aws_lambda_powertools.logging.correlation_paths import API_GATEWAY_HTTP
from aws_lambda_powertools.utilities.typing import LambdaContext
from src.entrypoints.rest.app import logger, app



@logger.inject_lambda_context(
    log_event=True,
    correlation_id_path=API_GATEWAY_HTTP,
)
def handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)
