from expects import equal, expect
from aws_lambda_stream.filters.content import filter_on_content


def test_filter_on_content_defaults_to_true():
    expect(filter_on_content({}, {'event': {'type': 'thing-created'}})).to(equal(True))


def test_filter_on_content_runs_all_filters():
    calls = []
    rule = {
        'id': 'test',
        'filters': [
            lambda uow, rule: calls.append((uow['event']['id'], rule['id'])) or True,
            lambda uow, _: uow['event']['type'] == 'thing-created',
        ],
    }

    expect(filter_on_content(rule, {
        'event': {
            'id': 'evt-1',
            'type': 'thing-created',
        },
    })).to(equal(True))
    expect(calls).to(equal([('evt-1', 'test')]))


def test_filter_on_content_short_circuits_false_filters():
    calls = []
    rule = {
        'filters': [
            lambda *_: False,
            lambda *_: calls.append(True) or True,
        ],
    }

    expect(filter_on_content(rule, {'event': {}})).to(equal(False))
    expect(calls).to(equal([]))
