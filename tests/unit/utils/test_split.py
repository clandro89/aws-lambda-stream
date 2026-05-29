from reactivex import from_list
from expects import expect, equal
from aws_lambda_stream.utils.split import split_object

def test_split_path():
    collected = []
    def _on_next(uow):
        collected.append(uow)

    source = from_list([ # pylint: disable=E1102
        {'things': [{'id':1},{'id':2}]},
        {'things': [{'id':3},{'id':4}]}
    ])
    source.pipe(
        split_object(
            {
                'split_on': 'things'
            },
        ),
    ).subscribe(
        on_next=_on_next
    )

    expect(len(collected)).to(equal(4))
    expect(collected[0]['split']).to(equal({'id':1}))
    expect(collected[1]['split']).to(equal({'id':2}))
    expect(collected[2]['split']).to(equal({'id':3}))
    expect(collected[3]['split']).to(equal({'id':4}))


def test_split_callable():
    collected = []

    source = from_list([ # pylint: disable=E1102
        {
            'thing': {
                'id': 1,
            },
        },
    ])
    source.pipe(
        split_object(
            {
                'split_on': lambda uow, _: [
                    {
                        **uow,
                        'copy': 1,
                    },
                    {
                        **uow,
                        'copy': 2,
                    },
                ],
            },
        ),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect([uow['copy'] for uow in collected]).to(equal([1, 2]))


def test_split_without_split_on_returns_source():
    collected = []

    source = from_list([ # pylint: disable=E1102
        {
            'thing': {
                'id': 1,
            },
        },
    ])
    source.pipe(
        split_object({}),
    ).subscribe(
        on_next=lambda uow: collected.append(uow)
    )

    expect(collected).to(equal([
        {
            'thing': {
                'id': 1,
            },
        },
    ]))
