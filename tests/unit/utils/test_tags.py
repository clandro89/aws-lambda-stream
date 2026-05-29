from expects import equal, expect
from aws_lambda_stream.utils.tags import adorn_standard_tags, env_tags


def test_env_tags(monkeypatch):
    monkeypatch.setenv('ACCOUNT_NAME', 'account')
    monkeypatch.setenv('AWS_REGION', 'us-west-2')
    monkeypatch.setenv('STAGE', 'dev')
    monkeypatch.setenv('APP_NAME', 'app')
    monkeypatch.setenv('SERVICE', 'service')
    monkeypatch.setenv('AWS_LAMBDA_FUNCTION_NAME', 'function')

    expect(env_tags('pipeline')).to(equal({
        'account': 'account',
        'region': 'us-west-2',
        'stage': 'dev',
        'app': 'app',
        'source': 'service',
        'functionname': 'function',
        'pipeline': 'pipeline',
    }))


def test_adorn_standard_tags_merges_existing_tags(monkeypatch):
    monkeypatch.setenv('SERVICE', 'service')
    monkeypatch.setenv('PYTEST_CURRENT_TEST', 'test')

    result = adorn_standard_tags('emit')({
        'pipeline': 'pipeline',
        'emit': {
            'type': 'thing-created',
            'tags': {
                'source': 'custom',
                'extra': 'value',
            },
        },
    })

    expect(result['emit']['tags']['pipeline']).to(equal('pipeline'))
    expect(result['emit']['tags']['skip']).to(equal(True))
    expect(result['emit']['tags']['source']).to(equal('custom'))
    expect(result['emit']['tags']['extra']).to(equal('value'))


def test_adorn_standard_tags_ignores_missing_event_field():
    uow = {
        'pipeline': 'pipeline',
    }

    expect(adorn_standard_tags('emit')(uow)).to(equal(uow))
