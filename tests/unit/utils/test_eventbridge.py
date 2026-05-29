from expects import expect, equal
from reactivex import from_list
from aws_lambda_stream.utils import eventbridge
from aws_lambda_stream.utils.eventbridge import publish_to_event_bridge


class Connector:
    def __init__(self):
        self.requests = []

    def put_events(self, request):
        self.requests.append(request)
        return {
            'FailedEntryCount': 0,
        }


class Logger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)

def test_publish():
    
    collected = []
    from_list([
        {
            'event': {
                'id': 'uuid1',
                'type': 'event_type_1',
            }
        }
    ]).pipe(
        publish_to_event_bridge()
    ).subscribe(
        on_next=lambda i: collected.append(i)
    )

    expect(len(collected)).to(equal(1))
    expect(collected[0]['event']).to(equal({
        'id': 'uuid1',
        'type': 'event_type_1',
        'tags': {
            'account': 'undefined',
            'region': 'undefined',
            'stage': 'test',
            'app': 'undefined',
            'source': 'undefined',
            'functionname':'undefined',
            'pipeline': 'undefined',
            'skip': True
        }
    }))
    expect(collected[0]['publish_request_entry']).to(equal(
        {
            'EventBusName': 'undefined',
            'Source': 'custom',
            'DetailType': 'event_type_1',
            'Detail': '{"id": "uuid1", "type": "event_type_1", "tags": {"account": "undefined", "region": "undefined", "stage": "test", "app": "undefined", "source": "undefined", "functionname": "undefined", "pipeline": "undefined", "skip": true}}'
        }    
    ))
    print(collected)


def test_publish_to_event_bridge_puts_events(monkeypatch):
    connector = Connector()
    logger = Logger()
    monkeypatch.setattr(eventbridge, 'Connector', lambda: connector)
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'event': {
                'id': 'uuid1',
                'type': 'event_type_1',
            },
        },
        {
            'other': True,
        },
    ]).pipe(
        publish_to_event_bridge(
            logger=logger,
            bus_name=lambda uow: 'bus-{}'.format(uow['event']['id']),
            source='source',
            batch_size=2,
        )
    ).subscribe(
        on_next=lambda i: collected.append(i)
    )

    expect(connector.requests).to(equal([
        {
            'Entries': [
                collected[0]['publish_request_entry'],
            ],
        },
    ]))
    expect(logger.messages).to(equal(connector.requests))
    expect('publish_response' in collected[1]).to(equal(False))
    expect(collected[0]['publish_request_entry']['EventBusName']).to(equal('bus-uuid1'))


def test_publish_to_event_bridge_skips_empty_batches(monkeypatch):
    connector = Connector()
    logger = Logger()
    monkeypatch.setattr(eventbridge, 'Connector', lambda: connector)
    collected = []

    from_list([ # pylint: disable=E1102
        {
            'other': True,
        },
    ]).pipe(
        publish_to_event_bridge(logger=logger, batch_size=1)
    ).subscribe(
        on_next=lambda i: collected.append(i)
    )

    expect(connector.requests).to(equal([]))
    expect(logger.messages).to(equal([]))
    expect(collected).to(equal([{
        'other': True,
        'publish_request_entry': None,
    }]))
