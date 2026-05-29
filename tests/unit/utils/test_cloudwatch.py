from expects import equal, expect
from reactivex import from_list
from aws_lambda_stream.utils import cloudwatch


class Connector:
    def __init__(self):
        self.requests = []

    def put(self, request):
        self.requests.append(request)
        return {
            'ok': True,
            'request': request,
        }


def test_put_metrics(monkeypatch):
    connector = Connector()
    monkeypatch.setattr(cloudwatch, 'Connector', lambda: connector)
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'put_request': {
                'MetricData': [],
            },
        },
    ]).pipe(
        cloudwatch.put_metrics(),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(connector.requests).to(equal([{'MetricData': []}]))
    expect(collected[0]['put_response']).to(equal({
        'ok': True,
        'request': {
            'MetricData': [],
        },
    }))
