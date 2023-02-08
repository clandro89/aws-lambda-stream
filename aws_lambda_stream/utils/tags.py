import os
from aws_lambda_stream.filters.skip import skip_tag


def adorn_standard_tags(event_field):
    def wrapper(uow: dict):
        return uow if not event_field in uow else {
            **uow,
            event_field: {
                **uow[event_field],
                'tags': {
                    **env_tags(uow.get('pipeline', None)),
                    **skip_tag(),
                    **(uow.get('event_field', {}).get('tags', {}))
                }
            }
        }
    return wrapper


def env_tags(pipeline):
    return {
        'account': os.getenv('ACCOUNT_NAME', 'undefined'),
        'region': os.getenv('AWS_REGION', 'undefined'),
        'stage': os.getenv('STAGE', 'undefined'),
        'source': (
            os.getenv('SERVICE') or
            os.getenv('PROJECT') or
            os.getenv('SERVERLESS_PROJECT') or 'undefined'),
        'functionname': os.getenv('AWS_LAMBDA_FUNCTION_NAME', 'undefined'),
        'pipeline': pipeline or 'undefined'
    }
