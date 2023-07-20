from expects import equal, expect, have_keys
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from aws_lambda_stream.events.kinesis import from_kinesis, to_kinesis_records
from src.entrypoints.listener.rules import rules



def test_collect_events():
    event = to_kinesis_records([
        {
            'id': 'e0',
            'partition_key': 'thing1',
            'type': 'thing-submitted',
            'timestamp': 1671042150000,
            'thing': {
                'id': 'thing1',
                'name': 'Thing',
                'another_relationship': 'rel1'
            }
        }
    ])
    collected = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from(rules),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_kinesis(event),
        on_next=_on_next
    )

    expect(collected[0]['pipeline']).to(equal('clt1'))
    expect(collected[0]['event']['type']).to(equal('thing-submitted'))
    expect(collected[0]['put_request']).to(have_keys({
        'Item': {
            'pk': 'e0',
            'sk': 'EVENT',
            'discriminator': 'EVENT',
            'timestamp': 1671042150000,
            'awsregion': 'us-west-2',
            'sequence_number': '0',
            'ttl': 1673893350,
            'expire': None,
            'data': 'thing1',
            'event': {
                'id': 'e0',
                'partition_key': 'thing1',
                'type': 'thing-submitted',
                'timestamp': 1671042150000,
                'thing': {
                    'id': 'thing1',
                    'name': 'Thing',
                    'another_relationship': 'rel1'
                }
            }
        }
    }))
