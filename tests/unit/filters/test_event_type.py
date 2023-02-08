import re
from expects import equal, expect
from aws_lambda_stream.filters.event_type import filter_on_event_type



def test_filter_string():
    rule = {
        'event_type': 'thing-created'
    }
    expect(filter_on_event_type(rule, {'event': {'type': 'thing-created'}})).to(equal(True))
    expect(filter_on_event_type(rule, {'event': {'type': 'thing-updated'}})).to(equal(False))

def test_filter_list():
    rule = {
        'event_type':['thing-created', 'thing-updated']
    }
    expect(filter_on_event_type(rule, {'event': {'type': 'thing-created'}})).to(equal(True))
    expect(filter_on_event_type(rule, {'event': {'type': 'thing-updated'}})).to(equal(True))
    expect(filter_on_event_type(rule, {'event': {'type': 'thing-deleted'}})).to(equal(False))

def test_filter_callable():
    rule = {
        'event_type': lambda uow: uow['event']['type'] == 'thing-created'
    }
    expect(filter_on_event_type(rule, {'event': {'type': 'thing-created'}})).to(equal(True))
    expect(filter_on_event_type(rule, {'event': {'type': 'thing-updated'}})).to(equal(False))


def test_filter_regex():
    rule = {
        'event_type': re.compile('(created|updated|deleted|undeleted)$')
    }
    expect(filter_on_event_type(rule, {'event': {'type': 'thing-created'}})).to(equal(True))
    expect(filter_on_event_type(rule, {'event': {'type': 'thing-updated'}})).to(equal(True))
    expect(filter_on_event_type(rule, {'event': {'type': 'thing-deleted'}})).to(equal(True))
    expect(filter_on_event_type(rule, {'event': {'type': 'thing-undeleted'}})).to(equal(True))
    expect(filter_on_event_type(rule, {'event': {'type': 'thing'}})).to(equal(False))
