import os
import traceback
from uuid import uuid1, uuid4
from reactivex import operators as ops, create, Observer
from  pydash import map_, clone_deep_with, pick
from .time import now


FAULT_EVENT_TYPE = 'fault'

the_faults = []


def throw_fault(uow, ignore=False):
    def wrapper(err):
        if not ignore:
            # adorn the troubled uow
            # for processing in the errors handler
            err.uow = uow
        raise err
    return wrapper


def faulty(funct, ignore=False):
    #pylint: disable=inconsistent-return-statements
    def wrapper(uow, **kwargs):
        try:
            return funct(uow, **kwargs)
        except Exception as e: #pylint: disable=broad-except
            #print("add exception to list")
            throw_fault(uow, ignore)(e)
    return wrapper


def format_fault(err: Exception):
    return {
        'id': str(uuid1()),
        'partition_key': str(uuid4()),
        'type': FAULT_EVENT_TYPE,
        'timestamp': now(),
        'pipeline': err.uow.get('pipeline') or 'undefined',
        'tags':{
            'functionname': os.getenv('AWS_LAMBDA_FUNCTION_NAME'),
            'pipeline': err.uow.get('pipeline') or 'undefined'
        },
        'err': {
            'name': type(err).__name__,
            'message': str(err),
            'stack': traceback.format_exc()
        },
        'uow': _trim_and_redact(err.uow)
    }

def flush_faults(rule):
    rule['logger'].info("flush faults")
    rule['logger'].info(the_faults)
    def push_errors(observer: Observer, _):
        while len(the_faults) > 0:
            observer.on_next(the_faults.pop(0))
        observer.on_completed()
    create(push_errors).pipe(
        ops.map(lambda fault: {'event': fault}),
        rule['publish'](pick(rule,[
            'logger',
            'max_batch_size',
            'bus_name'
        ])),
    ).subscribe()


def _trim_and_redact(_uow):
    # pylint: disable=unused-argument,inconsistent-return-statements
    def clone_customizer(value, key, obj):
        if not callable(value):
            return value
    def tr(uow):
        return {
            'pipeline': uow.get('pipeline'),
            'record': uow.get('record'),
            **clone_deep_with(
                {
                    'event': uow.get('event')
                },
                clone_customizer
            )
        }
    if not 'batch' in _uow:
        return tr(_uow)
    return {
        'batch': map_(
            _uow.get('batch', []),
            lambda u: tr(u) # pylint: disable=unnecessary-lambda
        )
    }


def faults(err):
    if hasattr(err, 'uow'):
        the_faults.append(format_fault(err))
    else:
        # raise unhandled/unexpected exceptions to stop processing
        raise err
