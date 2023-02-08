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
