import json
import base64
from pydash import map_



class CreatePaginationModel:
    @classmethod
    def from_elasticsearch(cls, result, size):
        next_token = None
        _total = len(result['hits']['hits'])
        if _total > 0 and size == _total:
            next_token = base64.b64encode(
                json.dumps(
                    result['hits']['hits'][_total - 1]['sort']
                ).encode('ascii')
            )
        return cls(
            total=result['hits']['total']['value'],
            count=_total,
            next_token=next_token,
            data=map_(
                result['hits']['hits'],
                lambda item: item['_source']
            )
        )
