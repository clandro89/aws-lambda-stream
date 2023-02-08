import json
from enum import Enum
from decimal import Decimal
from expects import expect, equal
from aws_lambda_stream.utils.json_encoder import JSONEncoder


def test_json_encoder():
    class TestEnum(Enum):
        KEY = 'value'

    data= {
        'x': TestEnum.KEY,
        'y': Decimal(12.5)
    }
    expect({
        'x': 'value',
        'y': 12.5
    }).to(equal(json.loads(
        json.dumps(data, cls=JSONEncoder)
    )))
