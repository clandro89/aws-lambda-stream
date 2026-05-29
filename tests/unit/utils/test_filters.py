from expects import equal, expect
from aws_lambda_stream.utils.filters import on_content, on_event_type


def test_on_event_type():
    predicate = on_event_type({
        'id': 'rule',
        'event_type': 'thing-created',
    })

    expect(predicate({'event': {'type': 'thing-created'}})).to(equal(True))
    expect(predicate({'event': {'type': 'thing-updated'}})).to(equal(False))


def test_on_content():
    predicate = on_content({
        'filters': [
            lambda uow, _: uow['event']['id'] == 'evt-1',
        ],
    })

    expect(predicate({'event': {'id': 'evt-1'}})).to(equal(True))
    expect(predicate({'event': {'id': 'evt-2'}})).to(equal(False))
