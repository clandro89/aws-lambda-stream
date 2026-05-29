from expects import equal, expect
from aws_lambda_stream.utils.apigateway import to_api_gateway_request_v2


def test_to_api_gateway_request_v2():
    request = to_api_gateway_request_v2(
        method='post',
        path='/things/{id}',
        body='{"name": "Thing"}',
        path_parameters={'id': 'thing-1'},
        query_string_parameters={'include': 'details'},
        headers={'authorization': 'Bearer token'},
        authorizer={'jwt': {'claims': {'sub': 'user-1'}}},
    )

    expect(request['version']).to(equal('2.0'))
    expect(request['routeKey']).to(equal('POST /things/{id}'))
    expect(request['requestContext']['http']['method']).to(equal('POST'))
    expect(request['body']).to(equal('{"name": "Thing"}'))
    expect(request['pathParameters']).to(equal({'id': 'thing-1'}))
    expect(request['queryStringParameters']).to(equal({'include': 'details'}))
    expect(request['headers']).to(equal({'authorization': 'Bearer token'}))
    expect(request['requestContext']['authorizer']).to(equal({
        'jwt': {
            'claims': {
                'sub': 'user-1',
            },
        },
    }))
