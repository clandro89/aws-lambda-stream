from expects import equal, expect
from reactivex import from_list
from aws_lambda_stream.utils.sns import publish_to_sns


class Connector:
    def __init__(self):
        self.requests = []

    def publish_batch(self, request):
        self.requests.append(request)
        return {
            'Successful': [],
        }


def test_publish_to_sns():
    connector = Connector()
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'sns_payload': [
                {
                    'Message': 'hello',
                },
            ],
        },
    ]).pipe(
        publish_to_sns(connector),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(len(connector.requests)).to(equal(1))
    expect(connector.requests[0]['PublishBatchRequestEntries'][0]['Message']).to(equal('hello'))
    expect('Id' in connector.requests[0]['PublishBatchRequestEntries'][0]).to(equal(True))
    expect(collected[0]['publish_response']).to(equal({'Successful': []}))
