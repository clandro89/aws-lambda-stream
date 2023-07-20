from uuid import uuid4
from datetime import datetime
from src.core.repositories.thing_repository import ThingRepository
from src.core.domain.models.thing import Thing
from src.exceptions import EntityNotFound




def create_thing(
    data: dict,
    thing_repository: ThingRepository
    ):
    _data = {
        **data,
        'id': str(uuid4()),
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
    _item = Thing(**_data)
    thing_repository.save(
        _item.to_dynamodb()
    )
    return _item

def get_thing(
    _id: str,
    thing_repository: ThingRepository
    ):
    item = thing_repository.get_thing(_id)
    if not item or item.get('deleted', False):
        raise EntityNotFound('Thing not found')
    return Thing(**item)


def update_thing(
    _id: str,
    data: dict,
    thing_repository: ThingRepository
    ):
    item = thing_repository.get_thing(_id)
    if not item or item.get('deleted', False):
        raise EntityNotFound('Thing not found')
    _data = {
            **item,
            **data,
            'latched': False
    }
    _item = Thing(**_data)
    thing_repository.save(
        _item.to_dynamodb()
    )
    return _item


def delete_thing(
    _id: str,
    thing_repository: ThingRepository
    ):
    item = thing_repository.get_thing(_id)
    if not item or item.get('deleted', False):
        raise EntityNotFound('Thing not found')
    thing_repository.delete(
        pk=_id,
        data={'code': f"{item['code']}__deleted"}
    )
