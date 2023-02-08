from aws_lambda_stream.filters import filter_on_event_type
from aws_lambda_stream.utils.faults import faulty
from aws_lambda_stream.filters.content import filter_on_content


def on_event_type(rule):
    def wrapper(uow):
        return filter_on_event_type(rule, uow)
    return faulty(wrapper)


def on_content(rule):
    def wrapper(uow):
        return filter_on_content(rule, uow)
    return faulty(wrapper)
