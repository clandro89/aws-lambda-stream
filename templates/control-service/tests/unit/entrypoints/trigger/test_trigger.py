from expects import equal, expect
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from aws_lambda_stream.events.dynamodb import from_dynamodb, to_dynamodb_records
from src.entrypoints.trigger.evaluate_rules import rules as evaluate_rules
from src.entrypoints.trigger.correlate_rules import rules as correlate_rules

THING_SUBMITTED_EVENT = {
            "timestamp": 1548967023,
            "keys": {
                "pk": "e0",
                "sk": "EVENT"
            },
            "newImage": {
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
        }


def test_correlate_events():
    event = to_dynamodb_records([
        THING_SUBMITTED_EVENT,
    ])

    collected = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from(correlate_rules),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_dynamodb(event),
        on_next=_on_next
    )
    expect(collected[0]['pipeline']).to(equal('crl1'))
    expect(collected[0]['event']['type']).to(equal('thing-submitted'))
    expect(collected[0]['put_request']).to(equal({
        'Item': {
            'pk': 'rel1',
            'sk': 'e0',
            'discriminator': 'CORREL',
            'timestamp': 1671042150000,
            'awsregion': 'us-west-2',
            'sequence_number': '0',
            'ttl': 1673893350,
            'expire': None,
            'suffix': None,
            'rule_id': 'crl1',
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


def test_evaluate_events():
    event = to_dynamodb_records([
        THING_SUBMITTED_EVENT,
    ])

    collected = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from(evaluate_rules),
        DEFAULT_OPTIONS,
        False
    ).assemble(
        from_dynamodb(event),
        on_next=_on_next
    )

    expect(collected[0]['pipeline']).to(equal('eval1'))
    expect(collected[0]['event']['type']).to(equal('thing-submitted'))
    expect(collected[0]['meta']).to(equal({
        'id': '0',
        'pk': 'e0',
        'data': 'thing1',
        'sequence_number': '0',
        'ttl': 1673893350,
        'expire': None,
        'correlation_key': 'thing1',
        'correlation': False,
        'suffix': None
    }))
    expect(collected[0]['emit']).to(equal({
        'id': '0.eval1',
        'partition_key': 'thing1',
        'type': 'thing-xyz',
        'timestamp': 1671042150000,
        'thing': {
            'id': 'thing1',
            'name': 'Thing',
            'another_relationship': 'rel1'
        },
        'tags': {
            'account': 'undefined',
            'region': 'undefined',
            'stage': 'test',
            'app': 'undefined',
            'source': 'undefined',
            'functionname': 'undefined',
            'pipeline': 'eval1',
            'skip': True
        },
        'triggers': [
            {
                'id': 'e0',
                'type': 'thing-submitted',
                'timestamp': 1671042150000
            }
        ]
    }))
