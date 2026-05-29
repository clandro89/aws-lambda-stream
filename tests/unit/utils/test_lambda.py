from expects import equal, expect
from reactivex import from_list
from aws_lambda_stream.utils import lambda_ as lambda_utils


class Connector:
    def __init__(self):
        self.requests = []

    def invoke(self, request):
        self.requests.append(request)
        return {
            'StatusCode': 200,
        }


def test_invoke_lambda(monkeypatch):
    connector = Connector()
    monkeypatch.setattr(lambda_utils, 'Connector', lambda: connector)
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'invoke_request': {
                'FunctionName': 'fn',
            },
        },
    ]).pipe(
        lambda_utils.invoke_lambda(),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(connector.requests).to(equal([{'FunctionName': 'fn'}]))
    expect(collected[0]['invoke_response']).to(equal({'StatusCode': 200}))
