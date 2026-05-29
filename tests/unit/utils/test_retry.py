from expects import equal, expect, raise_error
from aws_lambda_stream.utils import retry


def test_assert_max_retries_allows_attempts_within_limit():
    retry.assert_max_retries([{'id': 1}], 1)


def test_assert_max_retries_raises_when_limit_exceeded():
    expect(lambda: retry.assert_max_retries([{'id': 1}, {'id': 2}], 1)).to(
        raise_error(Exception, 'Failed batch requests: {"id": 2}')
    )


def test_wait(monkeypatch):
    sleeps = []
    monkeypatch.setattr(retry, 'sleep', lambda seconds: sleeps.append(seconds))

    retry.wait(250)

    expect(sleeps).to(equal([0.25]))


def test_get_delay(monkeypatch):
    expect(retry.get_delay(100, 0)).to(equal(0))

    monkeypatch.setattr(retry, 'random', lambda: 0.5)

    expect(retry.get_delay(100, 2)).to(equal(154))
