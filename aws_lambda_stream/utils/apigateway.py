

def to_api_gateway_request_v2(**kwargs):
    method = kwargs.get('method').upper()
    path = kwargs.get('path')
    body = kwargs.get('body', None)
    path_parameters = kwargs.get('path_parameters', {})
    query_string_parameters = kwargs.get('query_string_parameters', {})
    headers = kwargs.get('headers', {})
    authorizer = kwargs.get('authorizer', {})

    return {
        "version": "2.0",
        "routeKey": f"{method} {path}",
        "rawPath": path,
        "rawQueryString": "",
        "headers": headers,
        "queryStringParameters": query_string_parameters,
        "requestContext": {
            "accountId": "AXXXXXXX",
            "apiId": "apiid",
            "authorizer": authorizer,
            "domainName": "api.example.com",
            "domainPrefix": "api",
            "http": {
                "method": method,
                "path": path,
                "protocol": "HTTP/1.1",
                "sourceIp": "187.189.102.22",
                "userAgent": "PostmanRuntime/7.29.2"
            },
            "requestId": "ddp92gJUvHcEJVw=",
            "routeKey": f"{method} {path}",
            "stage": "$default",
            "time": "20/Dec/2022:21:05:15 +0000",
            "timeEpoch": 1671570315611
        },
        "body": body,
        "pathParameters": path_parameters,
        "isBase64Encoded": False
}
