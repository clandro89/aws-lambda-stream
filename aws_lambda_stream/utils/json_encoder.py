import json
from decimal import Decimal
from enum import Enum


class JSONEncoder(json.JSONEncoder):
    def default(self, obj): #pylint: disable=W0221
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, Enum):
            return obj.value
        return json.JSONEncoder.default(self, obj)
