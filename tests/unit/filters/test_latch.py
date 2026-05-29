from expects import equal, expect
from aws_lambda_stream.filters.latch import out_latched, out_source_is_self


def test_out_source_is_self(monkeypatch):
    monkeypatch.setenv('SERVICE', 'inventory')

    expect(out_source_is_self({'event': {}})).to(equal(True))
    expect(out_source_is_self({
        'event': {
            'tags': {
                'source': 'billing',
            },
        },
    })).to(equal(True))
    expect(out_source_is_self({
        'event': {
            'tags': {
                'source': 'inventory',
            },
        },
    })).to(equal(False))


def test_out_latched_create_or_update_events():
    expect(out_latched({
        'event': {
            'raw': {
                'new': {
                    'latched': False,
                },
            },
        },
    })).to(equal(True))
    expect(out_latched({
        'event': {
            'raw': {
                'new': {
                    'latched': True,
                },
            },
        },
    })).to(equal(False))


def test_out_latched_delete_events():
    expect(out_latched({
        'event': {
            'raw': {
                'new': None,
                'old': {
                    'latched': False,
                },
            },
        },
    })).to(equal(True))
    expect(out_latched({
        'event': {
            'raw': {
                'new': None,
                'old': {
                    'latched': True,
                },
            },
        },
    })).to(equal(False))
