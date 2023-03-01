from reactivex import Observable
from aws_lambda_stream.connectors.lambda_ import Connector
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.operators import rx_map


def invoke_lambda(
        invoke_field = 'invoke_request',
    ):
    connector = Connector()

    def invoke(uow):
        return {
            **uow,
            'invoke_response': connector.invoke(uow[invoke_field])
        }

    def wrapper(source: Observable):
        return source.pipe(
            rx_map(faulty(invoke)),
        )
    return wrapper
