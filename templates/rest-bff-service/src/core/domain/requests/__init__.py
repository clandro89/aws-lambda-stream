import json
import base64
from typing import Optional
from pydantic import BaseModel, root_validator


class SearchModel(BaseModel):
    query: Optional[dict]
    size: Optional[int] = 20
    next_token: Optional[str]
    search_after: Optional[list]
    sort: Optional[list] = [
            {'created_at': 'desc'}
        ]

    @root_validator
    def validation(cls, values): #pylint: disable=no-self-argument,no-self-use
        next_token = values.get('next_token')
        if next_token:
            values['search_after'] = json.loads(
                base64.b64decode(next_token.encode('ascii'))
            )
        return values
