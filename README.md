# aws-lambda-stream

`aws-lambda-stream` helps you build AWS Lambda functions that react to events and coordinate
work across AWS services.

Use it when a Lambda receives a batch of records from Kinesis, DynamoDB Streams, S3, SNS, SQS,
or EventBridge, and you need to:

- decide which business rules should run for each event;
- execute tasks;
- publish new domain events;
- store events for later correlation;
- update DynamoDB materialized views;
- write objects to S3 or messages to SNS/SQS;
- keep processing the rest of the batch when one record fails.

Instead of writing one large Lambda handler with nested loops, filters, try/except blocks,
AWS calls, and batch handling, you define small rules and run them through a stream pipeline.

The library uses [ReactiveX for Python](https://github.com/ReactiveX/RxPY) under the hood and
is inspired by [jgilbert01/aws-lambda-stream](https://github.com/jgilbert01/aws-lambda-stream).

## What Problem Does It Solve?

In an event-driven system, one incoming event often triggers several independent reactions.

For example, when an `order-created` event arrives, you may want to:

- save the event in DynamoDB;
- correlate it with `payment-authorized`;
- emit `order-ready` only when both events exist;
- materialize an order read model;
- write a JSON snapshot to S3;
- notify another system through SNS.

With `aws-lambda-stream`, each reaction is a rule:

```python
rules = [
    collect_order_events,
    correlate_order,
    evaluate_order_ready,
    materialize_order,
    write_order_snapshot,
    notify_order_ready,
]
```

The Lambda handler stays small:

```python
StreamPipeline(
    initialize_from(rules),
    options,
).assemble(
    from_kinesis(event),
)
```

The framework handles event normalization, rule matching, RxPY execution, batching helpers,
fault collection, and AWS connector glue.

## Installation

With pip:

```bash
pip install aws-lambda-stream
```

With Poetry:

```bash
poetry add aws-lambda-stream
```

## Quick Start

```python
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.events.kinesis import from_kinesis
from aws_lambda_stream.flavors.task import task
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS


def execute(uow, rule):
    return {
        "handled": True,
        "event_id": uow["event"]["id"],
    }


rules = [
    {
        "id": "handle-thing-created",
        "flavor": task,
        "event_type": "thing-created",
        "execute": execute,
    },
]


def handler(event, _context):
    StreamPipeline(
        initialize_from(rules),
        DEFAULT_OPTIONS,
    ).assemble(
        from_kinesis(event),
    )
```

Each incoming record becomes a unit of work (`uow`) shaped roughly like:

```python
{
    "pipeline": "handle-thing-created",
    "record": {...},
    "event": {
        "id": "...",
        "type": "thing-created",
        "timestamp": 1548967022000,
    },
}
```

## Lambda Handler Shape

In most Lambdas you do three things:

1. Convert the AWS trigger event into UOWs.
2. Initialize rules.
3. Assemble and run the stream.

```python
from aws_lambda_powertools import Logger
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.events.dynamodb import from_dynamodb
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS
from .rules import rules


logger = Logger()


def handler(event, context):
    options = {
        **DEFAULT_OPTIONS,
        "logger": logger,
        "bus_name": "my-event-bus",
    }

    StreamPipeline(
        initialize_from(rules),
        options,
    ).assemble(
        from_dynamodb(event),
    )
```

`DEFAULT_OPTIONS` provides:

- `logger`: standard Python root logger.
- `bus_name`: `BUS_NAME` from the environment.
- `publish`: EventBridge publisher.

You can override any of them per Lambda.

## Rules And Flavors

Rules are dictionaries. `initialize_from(rules)` turns them into executable pipelines.

Common rule fields:

- `id`: unique rule/pipeline id.
- `flavor`: a callable that receives the rule and returns an RxPY operator.
- `event_type`: string, list, regex, or callable used to match events.
- `filters`: optional list of content predicates.
- `publish`: publisher function, usually from `DEFAULT_OPTIONS`.
- `logger`: optional logger. If omitted, a standard pipeline logger is configured.

Built-in flavors include:

- `task`: execute business logic and optionally emit an event.
- `collect`: store events for later correlation.
- `correlate`: build correlation records.
- `evaluate`: evaluate collected/correlated events and emit higher-order events.
- `materialize`: update materialized views.
- `cdc`: publish change-data-capture events.
- `s3`, `sns`, `elasticsearch`: write to external destinations.
- `expired`: emit expiration events from DynamoDB TTL removals.
- `update`: query/get/update DynamoDB records.

## Filtering Events

Most flavors start by checking `event_type` and then optional content filters:

```python
{
    "id": "collect-thing-created",
    "flavor": collect,
    "event_type": "thing-created",
    "filters": [
        lambda uow, rule: uow["event"]["thing"]["status"] == "active",
    ],
}
```

`event_type` can be any of these:

```python
"thing-created"
["thing-created", "thing-updated"]
re.compile(r"thing-(created|updated)$")
lambda uow: uow["event"]["type"].startswith("thing-")
```

`filters` receive `(uow, rule)` and all filters must return `True`.

## Task Flavor

Use `task` when you want to run business logic for matching events.

```python
from aws_lambda_stream.flavors.task import task


def execute(uow, rule):
    return {
        "thing_id": uow["event"]["thing"]["id"],
        "processed": True,
    }


rules = [
    {
        "id": "process-thing",
        "flavor": task,
        "event_type": "thing-created",
        "execute": execute,
    },
]
```

The result is stored in `uow["result"]` by default. Use `result_key` to change it:

```python
{
    "id": "process-thing",
    "flavor": task,
    "event_type": "thing-created",
    "execute": execute,
    "result_key": "task_result",
}
```

### Emitting Events From A Task

Add `emit` to publish a follow-up event.

```python
{
    "id": "complete-task",
    "flavor": task,
    "event_type": "task-requested",
    "execute": execute,
    "emit": lambda uow, rule, template: {
        **template,
        "type": "task-completed",
        "thing": uow["event"]["thing"],
    },
}
```

`template` contains a generated `id`, `timestamp`, and `partition_key`.

Return a list from `emit` to publish multiple events:

```python
"emit": lambda uow, rule, template: [
    {
        **template,
        "type": "first-event",
    },
    {
        **template,
        "type": "second-event",
    },
]
```

## Collect, Correlate, Evaluate

These flavors are useful for event-driven workflows where one event is not enough.

### collect

`collect` stores raw events in DynamoDB so they can be queried later.

```python
from aws_lambda_stream.flavors.collect import collect


{
    "id": "collect-order-events",
    "flavor": collect,
    "event_type": ["order-created", "payment-authorized"],
    "correlation_key": "order.id",
}
```

If `correlation_key` is a string, it is read from `event`. If it is callable, it receives the
UOW:

```python
"correlation_key": lambda uow: uow["event"]["order"]["id"]
```

### correlate

`correlate` reads collected event table stream records and writes correlation records.

```python
from aws_lambda_stream.flavors.correlate import correlate


{
    "id": "correlate-order",
    "flavor": correlate,
    "event_type": ["order-created", "payment-authorized"],
    "correlation_key": "order.id",
}
```

Use `correlation_key_suffix` when different workflows share the same entity id:

```python
{
    "id": "correlate-payment",
    "flavor": correlate,
    "event_type": "payment-authorized",
    "correlation_key": "order.id",
    "correlation_key_suffix": "payment",
}
```

### evaluate

`evaluate` checks whether enough correlated events exist and emits higher-order events.

```python
from aws_lambda_stream.flavors.evaluate import evaluate


def has_order_and_payment(uow):
    types = [event["type"] for event in uow["correlated"]]
    return "order-created" in types and "payment-authorized" in types


{
    "id": "order-ready",
    "flavor": evaluate,
    "event_type": ["order-created", "payment-authorized"],
    "correlation_key_suffix": "payment",
    "expression": has_order_and_payment,
    "emit": "order-ready",
}
```

`expression` can return:

- `True` or `False`.
- An event dictionary.
- A list of event dictionaries.

When it returns `True`, the triggering event is used as the trigger.

## CDC Flavor

Use `cdc` to publish a new event from a DynamoDB stream record.

```python
from aws_lambda_stream.flavors.cdc import cdc


{
    "id": "thing-cdc",
    "flavor": cdc,
    "event_type": "THING-created",
    "to_event": lambda uow: {
        "type": "thing-created",
        "thing": uow["event"]["raw"]["new"],
    },
}
```

`to_event` receives the UOW and returns fields that are merged into `uow["event"]` before
publishing.

## Materialize Flavor

Use `materialize` to update a DynamoDB materialized view from an event.

```python
from aws_lambda_stream.flavors.materialize import materialize
from aws_lambda_stream.utils.dynamodb import update_expression


{
    "id": "materialize-thing",
    "flavor": materialize,
    "event_type": "thing-created",
    "to_update_request": lambda uow: {
        "Key": {
            "pk": uow["event"]["thing"]["id"],
            "sk": "THING",
        },
        **update_expression({
            "name": uow["event"]["thing"]["name"],
            "timestamp": uow["event"]["timestamp"],
        }),
    },
}
```

Use `split_on` when one event should update multiple records:

```python
{
    "id": "materialize-order-items",
    "flavor": materialize,
    "event_type": "order-created",
    "split_on": "event.items",
    "split_target_field": "item",
    "to_update_request": lambda uow: {
        "Key": {
            "pk": uow["item"]["id"],
            "sk": "ITEM",
        },
        **update_expression({
            "order_id": uow["event"]["order"]["id"],
        }),
    },
}
```

## S3 And SNS Flavors

Use `s3` to write derived objects.

```python
from aws_lambda_stream.flavors.s3 import s3


{
    "id": "write-order-snapshot",
    "flavor": s3,
    "event_type": "order-ready",
    "bucket_name": "my-snapshot-bucket",
    "to_s3": lambda uow: {
        "Key": "orders/{}.json".format(uow["event"]["order"]["id"]),
        "Body": json.dumps(uow["event"]["order"]).encode("utf-8"),
        "ContentType": "application/json",
    },
}
```

Use `sns` to publish batches to an SNS topic.

```python
from aws_lambda_stream.flavors.sns import sns


{
    "id": "notify-order-ready",
    "flavor": sns,
    "event_type": "order-ready",
    "topic_arn": "arn:aws:sns:us-east-1:123456789012:orders",
    "to_sns": lambda uow: [
        {
            "Message": json.dumps(uow["event"]),
        },
    ],
}
```

## Expired Flavor

Use `expired` with DynamoDB TTL stream removals. If an item has `ttl` and `expire`, the flavor
emits an expiration event.

```python
from aws_lambda_stream.flavors.expired import expired


{
    "id": "expired-events",
    "flavor": expired,
}
```

If `expire` is `True`, the emitted type is derived from the original event:

- `thing.created` becomes `thing.created.expired`
- `thing-created` becomes `thing-created-expired`

If `expire` is a string, that string is used as the emitted type:

```python
{
    "ttl": 1548967022,
    "expire": "thing-timeout",
    "event": {
        "id": "thing-1",
        "type": "thing-created",
        "timestamp": 1548967022000,
    },
}
```

## Update Flavor

Use `update` when an event needs to query records, optionally split results, and update each
target.

```python
from aws_lambda_stream.flavors.update import update
from aws_lambda_stream.utils.dynamodb import update_expression


{
    "id": "update-related-things",
    "flavor": update,
    "event_type": "thing-renamed",
    "to_query_request": lambda uow, rule: {
        "IndexName": "DataIndex",
        "KeyConditionExpression": "#data = :data",
        "ExpressionAttributeNames": {
            "#data": "data",
        },
        "ExpressionAttributeValues": {
            ":data": uow["event"]["thing"]["id"],
        },
    },
    "split_on": "query_response",
    "split_target_field": "target",
    "to_get_request": lambda uow, rule: {
        "Keys": [
            {
                "pk": uow["target"]["pk"],
                "sk": uow["target"]["sk"],
            },
        ],
    },
    "to_update_request": lambda uow, rule: {
        "Key": {
            "pk": uow["target"]["pk"],
            "sk": uow["target"]["sk"],
        },
        **update_expression({
            "thing_name": uow["event"]["thing"]["name"],
        }),
    },
}
```

Use `to_fallback_update_request` when an update can legitimately return `{}` and you want to
try a second request.

## Event Sources

Helpers normalize AWS event payloads:

```python
from aws_lambda_stream.events.kinesis import from_kinesis
from aws_lambda_stream.events.dynamodb import from_dynamodb
from aws_lambda_stream.events.s3 import from_s3
from aws_lambda_stream.events.sns import from_sns
from aws_lambda_stream.events.sqs import from_sqs
```

Test helpers are also available, for example `to_kinesis_records(...)`,
`to_dynamodb_records(...)`, and `to_s3_records(...)`.

Example local test input:

```python
from aws_lambda_stream.events.kinesis import to_kinesis_records, from_kinesis


event = to_kinesis_records([
    {
        "type": "thing-created",
        "timestamp": 1548967022000,
        "partition_key": "thing-1",
        "thing": {
            "id": "thing-1",
        },
    },
])

uows = from_kinesis(event)
```

## Concurrency

`StreamPipeline` runs pipelines concurrently by default:

```python
StreamPipeline(pipelines, options, concurrency=True)
```

You can disable concurrency for deterministic local tests:

```python
StreamPipeline(pipelines, options, concurrency=False)
```

Concurrent pipelines wait for each rule to complete before shutting down their scheduler, so
RxPY operators such as `flat_map` can safely emit nested observables.

## Unit Of Work (UOW)

A Unit Of Work, usually called `uow`, is the dictionary that moves through the pipeline.
Every flavor receives a stream of UOWs and may return the same UOW enriched with new fields.

The base shape is intentionally small:

```python
{
    "record": {...},
    "event": {
        "id": "event-id",
        "type": "thing-created",
        "timestamp": 1548967022000,
        "tags": {...},
    },
}
```

Common fields:

- `record`: the original AWS record. It is kept so failures can be inspected or resubmitted.
- `event`: the normalized domain event used by filters and flavors.
- `event.id`: stable event id.
- `event.type`: event type used by `event_type` filters.
- `event.timestamp`: event timestamp in milliseconds.
- `event.tags`: optional metadata such as region, source, pipeline, or test skip tags.

Everything else depends on the source event or your domain payload.

For example, an event from an event hub may look like:

```python
{
    "record": {...},
    "event": {
        "id": "evt-1",
        "type": "thing-created",
        "timestamp": 1548967022000,
        "tags": {...},
        "thing": {
            "id": "thing-1",
            "name": "Thing One",
        },
    },
}
```

A UOW created from DynamoDB Streams includes source-specific data under `event.raw`:

```python
{
    "record": {...},
    "event": {
        "id": "3",
        "type": "EVENT-created",
        "timestamp": 1548967022000,
        "tags": {
            "region": "us-west-2",
        },
        "raw": {
            "new": {...},
            "old": None,
        },
    },
}
```

When `StreamPipeline` runs a rule, it also adds `pipeline` to the UOW so downstream operators
know which rule is processing it:

```python
{
    "pipeline": "rule-id",
    "record": {...},
    "event": {...},
}
```



## Fault Handling

Use `faulty(...)` around functions that may fail for a single record:

```python
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.utils.operators import rx_map


source.pipe(
    rx_map(faulty(lambda uow: do_work(uow))),
)
```

Expected per-record failures are collected as fault events and processing continues with the
next record. Unexpected RxPY `on_error` errors remain terminal.

## Writing A Custom Flavor

A flavor receives a `rule` and returns an RxPY operator.

```python
from reactivex import Observable, operators as ops
from aws_lambda_stream.utils.filters import on_event_type, on_content
from aws_lambda_stream.utils.operators import rx_filter, rx_map
from aws_lambda_stream.utils.print import print_start_pipeline, print_end_pipeline


def my_flavor(rule):
    def wrapper(source: Observable):
        return source.pipe(
            rx_filter(on_event_type(rule)),
            ops.do_action(print_start_pipeline(rule)),
            rx_filter(on_content(rule)),
            rx_map(lambda uow: {
                **uow,
                "custom": True,
            }),
            ops.do_action(print_end_pipeline(rule)),
        )
    return wrapper
```

Use `ops.do_action(...)` for logging or side effects that should not transform the UOW.
Use `rx_map(...)` and `rx_filter(...)` when you want per-record failures to be collected as
faults and processing to continue.

## Local Testing

You can run a pipeline directly in a unit test:

```python
from aws_lambda_stream.pipelines import StreamPipeline, initialize_from
from aws_lambda_stream.events.kinesis import from_kinesis, to_kinesis_records
from aws_lambda_stream.utils.opt import DEFAULT_OPTIONS


def test_pipeline():
    collected = []

    event = to_kinesis_records([
        {
            "type": "thing-created",
            "timestamp": 1,
            "partition_key": "thing-1",
        },
    ])

    StreamPipeline(
        initialize_from(rules),
        DEFAULT_OPTIONS,
        concurrency=False,
    ).assemble(
        from_kinesis(event),
        on_next=lambda pipeline_id, uow: collected.append((pipeline_id, uow)),
    )

    assert len(collected) == 1
```

Use `concurrency=False` when you want stable ordering in tests.

## Project Templates

The repository includes Serverless Framework templates:

- `event-hub`
- `event-lake-s3`
- `event-fault-monitor`
- `rest-bff-service`
- `control-service`

Create a project from a template:

```bash
sls create \
  --template-url https://github.com/clandro89/aws-lambda-stream/tree/master/templates/event-hub \
  --path myprefix-event-hub
```

## Development

Run tests:

```bash
poetry run pytest
```

Build:

```bash
poetry build
```

Publish:

```bash
poetry publish
```

## Credits

- [jgilbert01/aws-lambda-stream](https://github.com/jgilbert01/aws-lambda-stream)

## License

This library is licensed under the MIT License. See the `LICENSE` file.
