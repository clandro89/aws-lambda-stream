from expects import equal, expect
from reactivex import from_list
from aws_lambda_stream.utils import sqs


class Connector:
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.requests = []

    def send_message_batch(self, request):
        self.requests.append(request)
        return {
            'Successful': [],
        }


class Logger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)


def test_send_to_sqs(monkeypatch):
    connector = Connector('queue')
    logger = Logger()
    monkeypatch.setattr(sqs, 'Connector', lambda queue_url: connector)
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'message': {
                'Id': '1',
                'MessageBody': 'one',
            },
        },
        {
            'other': True,
        },
    ]).pipe(
        sqs.send_to_sqs(logger=logger, queue_url='queue', batch_size=2),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(connector.requests).to(equal([
        {
            'Entries': [
                {
                    'Id': '1',
                    'MessageBody': 'one',
                },
            ],
        },
    ]))
    expect(logger.messages).to(equal(connector.requests))
    expect('send_message_batch_response' in collected[1]).to(equal(False))


def test_send_to_sqs_skips_empty_batches(monkeypatch):
    connector = Connector('queue')
    logger = Logger()
    monkeypatch.setattr(sqs, 'Connector', lambda queue_url: connector)
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'other': True,
        },
    ]).pipe(
        sqs.send_to_sqs(logger=logger, queue_url='queue', batch_size=1),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(connector.requests).to(equal([]))
    expect(logger.messages).to(equal([]))
    expect(collected).to(equal([{'other': True}]))
