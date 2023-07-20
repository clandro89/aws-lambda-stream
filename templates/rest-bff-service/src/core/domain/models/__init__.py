import json
from decimal import Decimal
from datetime import datetime, date
from pydantic import BaseModel


def to_datetime_string(dt: datetime) -> str:
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def to_date_string(dt: date) -> str:
    return dt.strftime('%Y-%m-%d')


class Model(BaseModel):

    def to_event(self):
        return json.loads(
            self.json()
        )

    def to_dynamodb(self):
        return json.loads(
            self.json(),
            parse_float=Decimal
        )

    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: to_datetime_string,
            date: to_date_string
        }
