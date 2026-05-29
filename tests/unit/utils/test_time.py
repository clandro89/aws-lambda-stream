from expects import equal, expect
from aws_lambda_stream.utils.time import ttl, ttl_rule


def test_ttl():
    expect(ttl(1000, 2)).to(equal(172801))


def test_ttl_rule_with_callable():
    rule = {
        'ttl': lambda rule, uow: uow['event']['timestamp'] + rule['offset'],
        'offset': 10,
    }

    expect(ttl_rule(rule, {
        'event': {
            'timestamp': 1000,
        },
    })).to(equal(1010))


def test_ttl_rule_with_rule_default_and_env(monkeypatch):
    uow = {
        'event': {
            'timestamp': 1000,
        },
    }

    expect(ttl_rule({'ttl': 2}, uow)).to(equal(172801))
    expect(ttl_rule({'default_ttl': 3}, uow)).to(equal(259201))

    monkeypatch.setenv('TTL', '4')
    expect(ttl_rule({}, uow)).to(equal(345601))

    monkeypatch.delenv('TTL')
    expect(ttl_rule({}, uow)).to(equal(2851201))
