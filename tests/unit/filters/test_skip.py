from expects import equal, expect
from aws_lambda_stream.filters.skip import out_skip, skip_tag


def test_skip_tag_marks_pytest_events(monkeypatch):
    monkeypatch.setenv('PYTEST_CURRENT_TEST', 'test')
    expect(skip_tag()).to(equal({'skip': True}))

    monkeypatch.delenv('PYTEST_CURRENT_TEST')
    expect(skip_tag()).to(equal({'skip': False}))


def test_out_skip():
    expect(out_skip({'event': {}})).to(equal(True))
    expect(out_skip({'event': {'tags': {'skip': False}}})).to(equal(True))
    expect(out_skip({'event': {'tags': {'skip': True}}})).to(equal(False))
