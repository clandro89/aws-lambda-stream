from expects import equal, expect
from aws_lambda_stream.utils.batch import to_batch_uow, unbatch_uow


def test_batch_helpers():
    batch = [
        {
            'event': {
                'id': 'evt-1',
            },
        },
        {
            'event': {
                'id': 'evt-2',
            },
        },
    ]

    expect(to_batch_uow(batch)).to(equal({'batch': batch}))
    expect(unbatch_uow({'batch': batch})).to(equal(batch))
