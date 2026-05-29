from expects import equal, expect
from aws_lambda_stream.utils.aws import get_region


def test_get_region_from_arn():
    expect(get_region('arn:aws:lambda:us-east-1:123:function:name')).to(equal('us-east-1'))


def test_get_region_from_env(monkeypatch):
    monkeypatch.setenv('REGION', 'us-west-2')

    expect(get_region('not-an-arn')).to(equal('us-west-2'))
