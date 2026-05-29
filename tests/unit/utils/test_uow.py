from array import array
from expects import equal, expect
from aws_lambda_stream.utils.uow import trim_and_redact


def test_trim_and_redact_uow():
    uow = {
        'pipeline': 'test',
        'record': {
            'secret': 'do-not-redact-record',
        },
        'event': {
            'eem': {
                'fields': ['secret'],
            },
            'secret': 'redact-me',
            'visible': 'keep-me',
        },
        'secret': 'redact-me-too',
        'decryptResponse': {
            'ignore': True,
        },
        'payload': b'abcdef',
        'typed': memoryview(b'abcd'),
    }

    result = trim_and_redact(uow)

    expect(result).to(equal({
        'pipeline': 'test',
        'record': {
            'secret': 'do-not-redact-record',
        },
        'event': {
            'eem': {
                'fields': ['secret'],
            },
            'secret': '[REDACTED]',
            'visible': 'keep-me',
        },
        'secret': '[REDACTED]',
        'payload': '[BUFFER: 6]',
        'typed': '[TYPED_ARRAY: 4]',
    }))


def test_trim_and_redact_prefers_undecrypted_event():
    uow = {
        'pipeline': 'test',
        'record': {},
        'event': {
            'secret': 'encrypted',
        },
        'undecryptedEvent': {
            'eem': {
                'fields': ['secret'],
            },
            'secret': 'plain',
        },
    }

    result = trim_and_redact(uow)

    expect(result['event']).to(equal({
        'eem': {
            'fields': ['secret'],
        },
        'secret': '[REDACTED]',
    }))


def test_trim_and_redact_batch_and_circular_references():
    circular = {
        'value': 'root',
    }
    circular['self'] = circular

    result = trim_and_redact({
        'batch': [
            {
                'pipeline': 'test1',
                'record': {},
                'event': {
                    'eem': {
                        'fields': ['token'],
                    },
                    'token': 'redact-me',
                    'typed': array('I', [1, 2]),
                },
            },
            {
                'pipeline': 'test2',
                'record': {},
                'event': {
                    'token': 'redact-me-too',
                    'nested': circular,
                },
            },
        ],
        'metadata': {
            'token': 'redact-me-three',
        },
    })

    expect(result['batch'][0]['event']['token']).to(equal('[REDACTED]'))
    expect(result['batch'][0]['event']['typed']).to(equal('[TYPED_ARRAY: 8]'))
    expect(result['batch'][1]['event']['token']).to(equal('[REDACTED]'))
    expect(result['batch'][1]['event']['nested']['self']).to(equal('[CIRCULAR]'))
    expect(result['metadata']['token']).to(equal('[REDACTED]'))
