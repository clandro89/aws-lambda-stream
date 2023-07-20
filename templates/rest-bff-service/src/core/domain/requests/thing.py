from src.core.domain.models import Model


class StoreThingRequest(Model):
    code: str
    description: str


class UpdateThingRequest(Model):
    description: str
