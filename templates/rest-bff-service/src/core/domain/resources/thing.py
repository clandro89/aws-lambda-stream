from src.core.domain.models import Model


class ThingResource(Model):
    id: str
    code: str
    description: str
