from aws_lambda_stream.flavors.evaluate import evaluate

rules = [
    {
        'id': 'eval1',
        'flavor': evaluate,
        'event_type': 'thing-submitted',
        # pylint: disable: C0301
        # 'expression': lambda uow: len(
        #     [e for e in uow['correlated'] if e['type'] in ['t1', 't2']]
        # ) == 2,
        'emit': 'thing-xyz',
        # 'emit': lambda uow, rule, template: {
        #     **template,
        #     'type': 'thing-xyz',
        #     'thing': uow['event']['thing']
        # }
    }
]
