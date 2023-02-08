from pydash import get, find
from expects import equal, expect
from aws_lambda_stream.events.dynamodb import from_dynamodb
from aws_lambda_stream.connectors.dynamodb import Connector
from aws_lambda_stream.flavors.evaluate import evaluate
from aws_lambda_stream.events.dynamodb import to_dynamodb_records
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS



RULES = [
  {
    'id': 'eval1-basic-emit',
    'flavor': evaluate,
    'event_type': 'e1',
    'emit': 'e111',
  },
  {
    'id': 'eval2-single-emit',
    'flavor': evaluate,
    'event_type': 'e2',
    'emit': lambda uow,_,template: {
        **template,
        'type':  'e222',
        'thing': get(uow, 'event.thing')
    }
  },
  {
    'id': 'eval3-multi-emit',
    'flavor': evaluate,
    'event_type': 'e3',
    'emit': lambda uow, _, template : [
      {
        **template,
        'id': f"{template['id']}.1",
        'type': 'e333.1',
        'thing': get(uow, 'event.thing'),
      },
      {
        **template,
        'id': f"{template['id']}.2",
        'type': 'e333.2',
        'thing': get(uow, 'event.thing'),
      }
    ]
  },
  {
    'id': 'eval4',
    'flavor': evaluate,
    'event_type': 'e4',
    'expression': lambda _: True,
    'emit': 'e444',
  },
  {
    'id': 'eval5',
    'flavor': evaluate,
    'event_type': 'e5',
    'expression': lambda _ : [{
      'id': '51',
      'type': 'e51',
      'timestamp': 1548967022000,
    },
    {
      'id': '52',
      'type': 'e52',
      'timestamp': 1548967022000,
    }],
    'emit': 'e555',
  },
  {
    'id': 'eval6',
    'flavor': evaluate,
    'event_type': 'e6',
    'expression': lambda uow: find(uow['correlated'], lambda e: e['type'] == 'e66'),
    'emit': 'e666',
  },
  {
    'id': 'eval7',
    'flavor': evaluate,
    'event_type': 'e7',
    'correlation_key_suffix': 'seven',
    'expression': lambda _ : True,
    'emit': 'e777',
  }
]

def test_should_execute_simple_rules(monkeypatch):
    events = to_dynamodb_records([
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "1",
                "sk": "EVENT"
            },
            "newImage": {
                "pk": "1",
                "sk": "EVENT",
                "discriminator": "EVENT",
                "timestamp": 1548967022000,
                "sequence_number": "0",
                "ttl": 1551818222,
                "data": "11",
                "event": {
                    "id": "1",
                    "type": "e1",
                    "timestamp": 1548967022000,
                    "partition_key": "11",
                    "thing": {
                        "id": "11",
                        "name": "Thing One",
                        "description": "This is thing one"
                    }
                }
            }
        },
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "2",
                "sk": "EVENT"
            },
            "newImage": {
                "pk": "2",
                "sk": "EVENT",
                "discriminator": "EVENT",
                "timestamp": 1548967022000,
                "sequence_number": "0",
                "ttl": 1551818222,
                "data": "22",
                "event": {
                    "id": "2",
                    "type": "e2",
                    "timestamp": 1548967022000,
                    "partition_key": "22",
                    "thing": {
                        "id": "22",
                        "name": "Thing Two",
                        "description": "This is thing two"
                    }
                }
            }
        },
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "3",
                "sk": "EVENT"
            },
            "newImage": {
                "pk": "3",
                "sk": "EVENT",
                "discriminator": "EVENT",
                "timestamp": 1548967022000,
                "sequence_number": "0",
                "ttl": 1551818222,
                "data": "33",
                "event": {
                    "id": "3",
                    "type": "e3",
                    "timestamp": 1548967022000,
                    "partition_key": "33",
                    "thing": {
                        "id": "33",
                        "name": "Thing Three",
                        "description": "This is thing three"
                    }
                }
            }
        }
        ])

    collected = []

    monkeypatch.setattr(Connector, "put", lambda *_: {'result': 'OK'})

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from(RULES), DEFAULT_OPTIONS, False
    ).assemble(
        from_dynamodb(events),
        on_next = _on_next,
    )

    expect(len(collected)).to(equal(4))
    expect(get(collected, '[0].pipeline')).to(equal('eval1-basic-emit'))
    expect(get(collected, '[0].event.type')).to(equal('e1'))
    expect(get(collected, '[0].meta')).to(equal({
      'id': '0',
      'pk': '1',
      'data': '11',
      'sequence_number': '0',
      'ttl': 1551818222,
      'expire': None,
      'correlation_key': '11',
      'correlation': False,
      'suffix': None
    }))
    expect(get(collected, '[0].emit')).to(equal({
        "id": "0.eval1-basic-emit",
        "type": "e111",
        "timestamp": 1548967022000,
        "thing": {
            "id": "11",
            "name": "Thing One",
            "description": "This is thing one"
        },
        "partition_key": "11",
        "tags": {
            "account": "undefined",
            "region": "undefined",
            "stage": "test",
            "source": "undefined",
            "functionname": "undefined",
            "pipeline": "eval1-basic-emit",
            "skip": True
        },
        "triggers": [
            {
                "id": "1",
                "type": "e1",
                "timestamp": 1548967022000
            }
        ]
    }))

    expect(len(collected)).to(equal(4))
    expect(get(collected, '[1].pipeline')).to(equal('eval2-single-emit'))
    expect(get(collected, '[1].event.type')).to(equal('e2'))
    expect(get(collected, '[1].emit')).to(equal({
      "id": "1.eval2-single-emit",
      "type": "e222",
      "timestamp": 1548967022000,
      "partition_key": "22",
      "thing": {
        "id": "22",
        "name": "Thing Two",
        "description": "This is thing two"
      },
      "tags": {
        "account": "undefined",
        "region": "undefined",
        "stage": "test",
        "source": "undefined",
        "functionname": "undefined",
        "pipeline": "eval2-single-emit",
        "skip": True
      },
      "triggers": [
        {
          "id": "2",
          "type": "e2",
          "timestamp": 1548967022000
        }
      ]
    }))

    expect(len(collected)).to(equal(4))
    expect(get(collected, '[2].pipeline')).to(equal('eval3-multi-emit'))
    expect(get(collected, '[2].event.type')).to(equal('e3'))
    expect(get(collected, '[2].emit')).to(equal({
      "id": "2.eval3-multi-emit.1",
      "type": "e333.1",
      "timestamp": 1548967022000,
      "partition_key": "33",
      "thing": {
        "id": "33",
        "name": "Thing Three",
        "description": "This is thing three"
      },
      "tags": {
        "account": "undefined",
        "region": "undefined",
        "stage": "test",
        "source": "undefined",
        "functionname": "undefined",
        "pipeline": "eval3-multi-emit",
        "skip": True
      },
      "triggers": [
        {
          "id": "3",
          "type": "e3",
          "timestamp": 1548967022000
        }
      ]
    }))

    expect(get(collected, '[3].emit.id')).to(equal('2.eval3-multi-emit.2'))
    expect(get(collected, '[3].emit.type')).to(equal('e333.2'))


def test_should_execute_complex_rules(monkeypatch):
    print("test_should_execute_complex_rules")

    def _calls_fake(_, input_params):
        ck = get(input_params, 'ExpressionAttributeValues.:data')
        resp = {
            '44': [{'event': {'id': '4', 'type': 'e4', 'timestamp': 1548967022000}}],
            '55': [{'event': {'id': '5', 'type': 'e5', 'timestamp': 1548967022000}}]
        }
        return resp[ck]
    monkeypatch.setattr(Connector, 'query_all', _calls_fake)
    # pylint: disable=duplicate-code
    events = to_dynamodb_records([
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "4",
                "sk": "EVENT"
            },
            "newImage": {
                "pk": "4",
                "sk": "EVENT",
                "discriminator": "EVENT",
                "timestamp": 1548967022000,
                "sequence_number": "0",
                "ttl": 1551818222,
                "data": "44",
                "event": {
                    "id": "4",
                    "type": "e4",
                    "timestamp": 1548967022000,
                    "partition_key": "44",
                    "thing": {
                        "id": "44",
                        "name": "Thing Four",
                        "description": "This is thing four"
                    }
                }
            }
        },
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "5",
                "sk": "EVENT"
            },
            "newImage": {
                "pk": "5",
                "sk": "EVENT",
                "discriminator": "EVENT",
                "timestamp": 1548967022000,
                "sequence_number": "0",
                "ttl": 1551818222,
                "data": "55",
                "event": {
                    "id": "5",
                    "type": "e5",
                    "timestamp": 1548967022000,
                    "partition_key": "55",
                    "thing": {
                        "id": "55",
                        "name": "Thing Five",
                        "description": "This is thing five"
                    }
                }
            }
        }
    ])

    collected = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from(RULES), DEFAULT_OPTIONS, False
    ).assemble(
        from_dynamodb(events),
        on_next = _on_next,
    )

    expect(len(collected)).to(equal(2))
    expect(get(collected, '[0].pipeline')).to(equal('eval4'))
    expect(get(collected, '[0].event.type')).to(equal('e4'))
    expect(get(collected, '[0].meta')).to(equal({
        'id': '0',
        'pk': '4',
        'data': '44',
        'sequence_number': '0',
        'ttl': 1551818222,
        'expire': None,
        'correlation_key': '44',
        'correlation': False,
        'suffix': None
    }))
    expect(get(collected, '[0].query_request')).to(equal({
        "IndexName": "DataIndex",
        "KeyConditionExpression": "#data = :data",
        "ExpressionAttributeNames": {
            "#data": "data"
        },
        "ExpressionAttributeValues": {
            ":data": "44"
        }
    }))
    expect(get(collected, '[0].correlated')).to(equal([
        { 'id': '4', 'type': 'e4', 'timestamp': 1548967022000 },
    ]))
    expect(get(collected, '[0].emit')).to(equal({
        "id": "0.eval4",
        "type": "e444",
        "timestamp": 1548967022000,
        "partition_key": "44",
        "thing": {
            "id": "44",
            "name": "Thing Four",
            "description": "This is thing four"
        },
        "tags": {
            "account": "undefined",
            "region": "undefined",
            "stage": "test",
            "source": "undefined",
            "functionname": "undefined",
            "pipeline": "eval4",
            "skip": True
        },
        "triggers": [
            {
                "id": "4",
                "type": "e4",
                "timestamp": 1548967022000
            }
        ]
    }))

    expect(get(collected, '[1].pipeline')).to(equal('eval5'))
    expect(get(collected, '[1].event.type')).to(equal('e5'))
    expect(get(collected, '[1].emit.triggers')).to(equal([
        {
            "id": "51",
            "type": "e51",
            "timestamp": 1548967022000
        },
        {
            "id": "52",
            "type": "e52",
            "timestamp": 1548967022000
        }
    ]))


def test_should_execute_correlation_rules(monkeypatch):
    def _calls_fake(_, input_params):
        ck = get(input_params, 'ExpressionAttributeValues.:pk')
        resp = {
            "66": [
                {
                "event": {
                    "id": "66",
                    "type": "e66",
                    "timestamp": 1548967022000
                }
                }
            ],
            "77.seven": [
                {
                "event": {
                    "id": "77",
                    "type": "e77",
                    "timestamp": 1548967022000
                }
                }
            ]
        }
        return resp[ck]

    monkeypatch.setattr(Connector, 'query_all', _calls_fake)

    events = to_dynamodb_records([
        # match - no suffix
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "66",
                "sk": "6"
            },
            "newImage": {
                "pk": "66",
                "sk": "6",
                "discriminator": "CORREL",
                "timestamp": 1548967022000,
                "sequence_number": "0",
                "ttl": 1551818222,
                "event": {
                    "id": "6",
                    "type": "e6",
                    "timestamp": 1548967022000,
                    "partition_key": "66",
                    "thing": {
                        "id": "66",
                        "name": "Thing Six",
                        "description": "This is thing six"
                    }
                }
            }
        },
        # match - equal suffix
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "77.seven",
                "sk": "7"
            },
            "newImage": {
                "pk": "77.seven",
                "sk": "7",
                "discriminator": "CORREL",
                "timestamp": 1548967022000,
                "sequence_number": "0",
                "ttl": 1551818222,
                "suffix": "seven",
                "event": {
                    "id": "7",
                    "type": "e7",
                    "timestamp": 1548967022000,
                    "partition_key": "77",
                    "thing": {
                        "id": "77",
                        "name": "Thing Seven",
                        "description": "This is thing seven"
                    }
                }
            }
        },
        # no match - missing suffix
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "77",
                "sk": "7"
            },
            "newImage": {
                "pk": "77",
                "sk": "7",
                "discriminator": "CORREL",
                "timestamp": 1548967022000,
                "sequence_number": "0",
                "ttl": 1551818222,
                "suffix": "undefined",
                "event": {
                    "type": "e7"
                }
            }
        },
        # no match - wrong suffix
        {
            "timestamp": 1548967023,
            "keys": {
                "pk": "77.seventy",
                "sk": "7"
            },
            "newImage": {
                "pk": "77.seventy",
                "sk": "7",
                "discriminator": "CORREL",
                "timestamp": 1548967022000,
                "sequence_number": "0",
                "ttl": 1551818222,
                "suffix": "seventy",
                "event": {
                    "type": "e7"
                }
            }
        }
    ])

    collected = []

    def _on_next(_, uow):
        collected.append(uow)

    StreamPipeline(
        initialize_from(RULES), DEFAULT_OPTIONS, False
    ).assemble(
        from_dynamodb(events),
        on_next = _on_next,
    )

    expect(len(collected)).to(equal(2))
    expect(get(collected, '[0].pipeline')).to(equal('eval6'))
    expect(get(collected, '[0].event.type')).to(equal('e6'))
    expect(get(collected, '[0].meta')).to(equal({
        "id": "0",
        "pk": "66",
        "data": None,
        "sequence_number": "0",
        "ttl": 1551818222,
        "expire": None,
        "correlation_key": "66",
        "correlation": True,
        "suffix": None
    }))
    expect(get(collected, '[0].query_request')).to(equal({
        "KeyConditionExpression": "#pk = :pk",
        "ExpressionAttributeNames": {
            "#pk": "pk"
        },
        "ExpressionAttributeValues": {
            ":pk": "66"
        },
        "ConsistentRead": True
    }))
    expect(get(collected, '[0].correlated')).to(equal([
        { 'id': '66', 'type': 'e66', 'timestamp': 1548967022000 },
    ]))
    expect(get(collected, '[0].emit')).to(equal({
        "id": "0.eval6",
        "type": "e666",
        "timestamp": 1548967022000,
        "partition_key": "66",
        "thing": {
            "id": "66",
            "name": "Thing Six",
            "description": "This is thing six"
        },
        "tags": {
            "account": "undefined",
            "region": "undefined",
            "stage": "test",
            "source": "undefined",
            "functionname": "undefined",
            "pipeline": "eval6",
            "skip": True
        },
        "triggers": [
            {
            "id": "66",
            "type": "e66",
            "timestamp": 1548967022000
            }
        ]
    }))

    expect(get(collected, '[1].pipeline')).to(equal('eval7'))
    expect(get(collected, '[1].event.type')).to(equal('e7'))
    expect(get(collected, '[1].meta')).to(equal({
        "id": "1",
        "pk": "77.seven",
        "data": None,
        "sequence_number": "0",
        "ttl": 1551818222,
        "expire": None,
        "correlation_key": "77.seven",
        "correlation": True,
        "suffix": "seven"
    }))
    expect(get(collected, '[1].query_request')).to(equal({
        "KeyConditionExpression": "#pk = :pk",
        "ExpressionAttributeNames": {
            "#pk": "pk"
        },
        "ExpressionAttributeValues": {
            ":pk": "77.seven"
        },
        "ConsistentRead": True
    }))
    expect(get(collected, '[1].correlated')).to(equal([
        { 'id': '77', 'type': 'e77', 'timestamp': 1548967022000 },
    ]))
    expect(get(collected, '[1].emit.triggers')).to(equal([
       {
            'id': '7',
            'type': 'e7',
            'timestamp': 1548967022000,
        }
    ]))
