from expects import expect, equal
from reactivex import from_list
from aws_lambda_stream.utils.eventbridge import publish_to_event_bridge

def test_publish():
    
    collected = []
    from_list([
        {
            'event': {
                'id': 'uuid1',
                'type': 'event_type_1',
            }
        }
    ]).pipe(
        publish_to_event_bridge()
    ).subscribe(
        on_next=lambda i: collected.append(i)
    )

    expect(len(collected)).to(equal(1))
    expect(collected[0]['event']).to(equal({
        'id': 'uuid1',
        'type': 'event_type_1',
        'tags': {
            'account': 'undefined',
            'region': 'undefined',
            'stage': 'test',
            'app': 'undefined',
            'source': 'undefined',
            'functionname':'undefined',
            'pipeline': 'undefined',
            'skip': True
        }
    }))
    expect(collected[0]['publish_request_entry']).to(equal(
        {
            'EventBusName': 'undefined',
            'Source': 'custom',
            'DetailType': 'event_type_1',
            'Detail': '{"id": "uuid1", "type": "event_type_1", "tags": {"account": "undefined", "region": "undefined", "stage": "test", "app": "undefined", "source": "undefined", "functionname": "undefined", "pipeline": "undefined", "skip": true}}'
        }    
    ))
    print(collected)
