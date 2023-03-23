from reactivex import Observable
from aws_lambda_stream.connectors.cloudwatch import Connector
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.operators import rx_map


def put_metrics(
        put_field = 'put_request',
    ):
    connector = Connector()

    def put(uow):
        return {
            **uow,
            'put_response': connector.put(uow[put_field])
        }

    def wrapper(source: Observable):
        return source.pipe(
            rx_map(faulty(put)),
        )
    return wrapper
