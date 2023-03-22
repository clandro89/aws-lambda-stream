import os
from pydash import omit_by, get, set_
from reactivex import Observable, operators as ops, create, just, empty
from aws_lambda_stream import faulty, rx_map
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


def to_get_object_request(uow):
    return {
        **uow,
        'get_request': {
            'Bucket': uow['record']['s3']['bucket']['name'],
            'Key': uow['record']['s3']['object']['key']
        }
    }


def get_object_from_s3(
    bucket_name = os.getenv('BUCKET_NAME'),
    get_request_field='get_request',
    get_response_field='get_response'
    ):
    connector = Connector(bucket_name)
    def get_object(uow):
        key = connector.get_object(uow[get_request_field])
        uow[get_response_field] = key['Body'].read()
        return uow
    def _wrapper(source: Observable):
        return source.pipe(
            rx_map(faulty(get_object))
        )
    return _wrapper


def page_objects_from_s3(
    bucket_name = os.getenv('BUCKET_NAME'),
    list_request_field = 'list_request',
):
    connector = Connector(bucket_name)
    def wrapper(source: Observable):

        def subscribe(observer, _):
            def list_objects(uow):
                params = omit_by(
                    {
                        **uow[list_request_field],
                        'ContinuationToken': get(
                            uow,
                            f"{list_request_field}.ContinuationToken"
                        )
                    },
                    lambda value: value is None
                )

                res = connector.list_objects(params)

                for obj in res.get('Contents', []):
                    observer.on_next({
                        **uow,
                        list_request_field: params,
                        'list_response': {
                            'Content': obj
                        }
                    })

                if res['IsTruncated']:
                    return just(set_( # pylint: disable=E1102
                        uow,
                        f"{list_request_field}.ContinuationToken",
                        res['NextContinuationToken']
                    ))
                return empty()

            source.pipe(
                ops.expand(list_objects),
            ).subscribe(
                on_error=observer.on_error
            )

        return create(
            subscribe
        )

    return wrapper
