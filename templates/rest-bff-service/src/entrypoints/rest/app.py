import json
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.event_handler import Response, content_types
from aws_lambda_powertools.event_handler import APIGatewayHttpResolver
from src.core.domain.requests.thing import (
    StoreThingRequest,
    UpdateThingRequest
)
from src.core.domain.resources.thing import (
    ThingResource,
)
from src.core.services.thing_services import (
    create_thing,
    get_thing,
    update_thing,
    delete_thing
)
from src.core.repositories import get_thing_repository

tracer = Tracer()
logger = Logger()
app = APIGatewayHttpResolver()


@app.post('/things')
def create_thing_resolver():
    data = StoreThingRequest(
        **(app.current_event.json_body
           if app.current_event.body else {})
    )
    model = create_thing(
                data.dict(),
                get_thing_repository()
            )
    return Response(
        status_code=201,
        content_type=content_types.APPLICATION_JSON,
        body=ThingResource(
            **model.dict()
        ).dict()
    )

@app.get('/things/<thing_id>')
def get_thing_resolver(**_):
    model = get_thing(
                _.get('thing_id'),
                get_thing_repository()
            )
    return Response(
        status_code=200,
        content_type=content_types.APPLICATION_JSON,
        body=ThingResource(
            **model.dict()
        ).json()
    )

@app.put('/things/<thing_id>')
def update_thing_resolver(**_):
    data = UpdateThingRequest(
        **(app.current_event.json_body
           if app.current_event.body else {})
    )
    model = update_thing(
                _.get('thing_id'),
                data.dict(),
                get_thing_repository()
            )
    return Response(
        status_code=200,
        content_type=content_types.APPLICATION_JSON,
        body=ThingResource(
            **model.dict()
        ).json()
    )


@app.delete('/things/<thing_id>')
def delete_thing_resolver(**_):
    delete_thing(
        _.get('thing_id'),
        get_thing_repository()
    )
    return Response(
        status_code=200,
        content_type=content_types.APPLICATION_JSON,
        body=json.dumps({'status': 'success'})
    )
