from reactivex import Observable, operators as ops
from aws_lambda_stream.connectors.s3 import Connector

def put_object_to_s3(
    connector: Connector,
    put_request_field='put_request'
):
    def put_object(uow):
        uow['put_response'] = connector.put_object(uow[put_request_field])
        return uow
    def _wrapper(source: Observable):
        return source.pipe(
            ops.map(put_object)
        )
    return _wrapper
